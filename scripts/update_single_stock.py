# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# MODIFIED: New script — update a single stock in qlib data directory

"""
Update a single stock's data in the qlib data directory.

Usage (run from quant_finance/qlib/scripts/data_collector/all_source/):
    python ../../../scripts/update_single_stock.py --symbol AAPL --qlib_data_1d_dir ~/.qlib/qlib_data/us_data --end_date 2026-02-14 --region US

Or from quant_finance/qlib/scripts/:
    cd data_collector/all_source
    python ../../update_single_stock.py --symbol AAPL --qlib_data_1d_dir ~/.qlib/qlib_data/us_data
"""

import os
import sys
import csv
import time
import datetime
import traceback
import multiprocessing
from pathlib import Path
from typing import Optional

import fire
import numpy as np
import pandas as pd
from loguru import logger

# Setup path so that data_collector imports work
SCRIPT_DIR = Path(__file__).resolve().parent
ALL_SOURCE_DIR = SCRIPT_DIR / "data_collector" / "all_source"
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ALL_SOURCE_DIR))

import qlib
from qlib.utils import exists_qlib_data, fname_to_code, code_to_fname


class FailureLogger:
    """Log failed stock downloads to CSV and console."""

    def __init__(self, fail_log_path: str):
        self.fail_log_path = Path(fail_log_path).resolve()
        self.failures = []

    def log(self, symbol: str, start_date: str, end_date: str,
            error_type: str, error_message: str):
        entry = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        self.failures.append(entry)

        if error_type == "empty_data":
            logger.warning(
                f"[{symbol}] {start_date}~{end_date}: "
                f"All sources returned no data — possibly delisted. {error_message}"
            )
        else:
            logger.error(
                f"[{symbol}] {start_date}~{end_date}: "
                f"{error_type} — {error_message}"
            )

    def save(self):
        if not self.failures:
            logger.info("No failures to report.")
            return
        self.fail_log_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.fail_log_path.exists()
        with open(self.fail_log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "symbol", "start_date", "end_date",
                "error_type", "error_message", "timestamp"
            ])
            if not file_exists:
                writer.writeheader()
            writer.writerows(self.failures)
        logger.info(f"Failure log saved to: {self.fail_log_path} ({len(self.failures)} entries)")


def _read_instruments(instruments_path: Path) -> pd.DataFrame:
    """Read instruments/all.txt into DataFrame."""
    if not instruments_path.exists():
        return pd.DataFrame(columns=["symbol", "start_datetime", "end_datetime"])
    df = pd.read_csv(
        instruments_path, sep="\t", header=None,
        names=["symbol", "start_datetime", "end_datetime"],
        dtype={"symbol": str},
    )
    return df


def _update_instruments_file(instruments_path: Path, symbol: str,
                              start_dt: str, end_dt: str):
    """Update instruments/all.txt: add new or update existing symbol's end_datetime."""
    df = _read_instruments(instruments_path)
    symbol_upper = symbol.upper()

    mask = df["symbol"].str.upper() == symbol_upper
    if mask.any():
        # Update existing
        df.loc[mask, "end_datetime"] = end_dt
        logger.info(f"Updated {symbol_upper} end_datetime to {end_dt} in instruments/all.txt")
    else:
        # Add new
        new_row = pd.DataFrame([{
            "symbol": symbol_upper,
            "start_datetime": start_dt,
            "end_datetime": end_dt,
        }])
        df = pd.concat([df, new_row], ignore_index=True)
        logger.info(f"Added {symbol_upper} ({start_dt} ~ {end_dt}) to instruments/all.txt")

    df.to_csv(instruments_path, sep="\t", header=False, index=False)


def _update_calendar(calendar_path: Path, new_dates: list):
    """Update calendars/day.txt with new trading dates."""
    if not calendar_path.exists():
        logger.warning(f"Calendar file not found: {calendar_path}")
        return

    existing = pd.read_csv(calendar_path, header=None, names=["date"])
    existing_dates = set(existing["date"].tolist())

    new_entries = sorted(set(d for d in new_dates if d not in existing_dates))
    if not new_entries:
        logger.info("No new calendar dates to add.")
        return

    all_dates = sorted(existing["date"].tolist() + new_entries)
    np.savetxt(str(calendar_path), all_dates, fmt="%s", encoding="utf-8")
    logger.info(f"Added {len(new_entries)} new dates to calendar (total: {len(all_dates)})")


def update_single_stock(
    symbol: str,
    qlib_data_1d_dir: str = "~/.qlib/qlib_data/us_data",
    end_date: str = None,
    region: str = "US",
    delay: float = 1,
    fail_log: str = "./single_stock_fail_log.csv",
):
    """Update a single stock in the qlib data directory.

    Parameters
    ----------
    symbol : str
        Stock symbol, e.g. AAPL
    qlib_data_1d_dir : str
        qlib data directory, default ~/.qlib/qlib_data/us_data
    end_date : str
        End date (excluded), default today. e.g. 2026-02-14
    region : str
        Market region, default US
    delay : float
        Delay between requests in seconds, default 1
    fail_log : str
        Path for failure log CSV, default ./single_stock_fail_log.csv
    """
    # Resolve paths
    qlib_data_1d_dir = str(Path(qlib_data_1d_dir).expanduser().resolve())
    failure_logger = FailureLogger(fail_log)

    # Validate qlib data dir exists
    if not exists_qlib_data(qlib_data_1d_dir):
        logger.error(
            f"Qlib data directory not found or incomplete: {qlib_data_1d_dir}\n"
            f"Please run init_qlib_data.py first."
        )
        return

    # Default end_date = today + 1 day (to include today, since end is exclusive)
    if end_date is None:
        end_date = (pd.Timestamp(datetime.datetime.now().strftime("%Y-%m-%d"))).strftime("%Y-%m-%d")

    # Clamp end_date to not exceed today
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if pd.Timestamp(end_date) > pd.Timestamp(today_str):
        end_date = today_str
        logger.info(f"Clamped end_date to today: {end_date}")

    logger.info(f"=== Updating single stock: {symbol} ===")
    logger.info(f"qlib_data_1d_dir: {qlib_data_1d_dir}")
    logger.info(f"end_date: {end_date}")
    logger.info(f"region: {region}")

    # Read instruments to check if symbol exists
    instruments_path = Path(qlib_data_1d_dir) / "instruments" / "all.txt"
    instruments_df = _read_instruments(instruments_path)
    calendar_path = Path(qlib_data_1d_dir) / "calendars" / "day.txt"

    symbol_upper = symbol.upper()
    symbol_fname = code_to_fname(symbol_upper)
    feature_dir = Path(qlib_data_1d_dir) / "features" / symbol_fname

    # Determine start_date
    existing_mask = instruments_df["symbol"].str.upper() == symbol_upper
    if existing_mask.any() and feature_dir.exists():
        # Stock exists — find its last date from bin data
        existing_end = instruments_df.loc[existing_mask, "end_datetime"].iloc[0]
        start_date = (pd.Timestamp(existing_end) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"{symbol_upper} exists in database (end: {existing_end}), updating from {start_date}")
    else:
        # New stock — download from earliest available
        start_date = "2000-01-01"
        logger.info(f"{symbol_upper} not found in database, downloading full history from {start_date}")

    if pd.Timestamp(start_date) >= pd.Timestamp(end_date):
        logger.info(f"{symbol_upper} is already up to date (start={start_date} >= end={end_date})")
        return

    # === Step 1: Download raw data (Yahoo → Stooq fallback) ===
    logger.info(f"Step 1: Downloading {symbol} data ({start_date} ~ {end_date})...")

    from update_qlib_data import _fetch_stock_data_multi_source

    try:
        raw_df = _fetch_stock_data_multi_source(
            symbol_yahoo=symbol,
            symbol_fname=symbol_fname,
            start=start_date,
            end=end_date,
        )
    except Exception as e:
        failure_logger.log(symbol, start_date, end_date, "network_error", str(e))
        failure_logger.save()
        return

    if raw_df is None or raw_df.empty:
        failure_logger.log(
            symbol, start_date, end_date, "empty_data",
            "Yahoo + Stooq both returned no data — possibly delisted"
        )
        failure_logger.save()
        return

    # Report source
    data_source = "unknown"
    if "_source" in raw_df.columns:
        data_source = raw_df["_source"].iloc[0]
        raw_df = raw_df.drop(columns=["_source"])
    logger.info(f"Downloaded {len(raw_df)} rows for {symbol} (source: {data_source})")

    # === Step 2: Save raw CSV ===
    source_dir = ALL_SOURCE_DIR / "source_single"
    source_dir.mkdir(parents=True, exist_ok=True)

    raw_df["symbol"] = symbol_fname
    csv_path = source_dir / f"{symbol_fname}.csv"
    raw_df.to_csv(csv_path, index=False)
    logger.info(f"Saved raw CSV to: {csv_path}")

    # === Step 3: Normalize ===
    logger.info(f"Step 2: Normalizing data...")
    normalize_dir = ALL_SOURCE_DIR / "normalize_single"
    normalize_dir.mkdir(parents=True, exist_ok=True)

    is_new_stock = not (existing_mask.any() and feature_dir.exists())

    try:
        if is_new_stock:
            # New stock: use standard YahooNormalize1d
            from collector import YahooNormalizeUS1d
            from data_collector.base import Normalize

            normalizer = Normalize(
                source_dir=source_dir,
                target_dir=normalize_dir,
                normalize_class=YahooNormalizeUS1d,
                max_workers=1,
                date_field_name="date",
                symbol_field_name="symbol",
            )
            normalizer.normalize()
        else:
            # Existing stock: use YahooNormalize1dExtend
            from collector import YahooNormalizeUS1dExtend
            from data_collector.base import Normalize

            normalizer = Normalize(
                source_dir=source_dir,
                target_dir=normalize_dir,
                normalize_class=YahooNormalizeUS1dExtend,
                max_workers=1,
                date_field_name="date",
                symbol_field_name="symbol",
                old_qlib_data_dir=qlib_data_1d_dir,
            )
            normalizer.normalize()
    except Exception as e:
        failure_logger.log(symbol, start_date, end_date, "normalize_error", str(e))
        failure_logger.save()
        logger.error(f"Normalize failed: {traceback.format_exc()}")
        return

    # Check if normalized file exists
    norm_csv = normalize_dir / f"{symbol_fname}.csv"
    if not norm_csv.exists():
        failure_logger.log(
            symbol, start_date, end_date, "normalize_error",
            "Normalized CSV not produced"
        )
        failure_logger.save()
        return

    norm_df = pd.read_csv(norm_csv)
    if norm_df.empty:
        failure_logger.log(
            symbol, start_date, end_date, "empty_data",
            "Normalized data is empty"
        )
        failure_logger.save()
        return

    logger.info(f"Normalized {len(norm_df)} rows for {symbol}")

    # === Step 4: Dump to bin ===
    logger.info(f"Step 3: Dumping to bin format...")

    try:
        from dump_bin import DumpDataUpdate

        _dump = DumpDataUpdate(
            data_path=str(normalize_dir),
            qlib_dir=qlib_data_1d_dir,
            exclude_fields="symbol,date",
            max_workers=max(multiprocessing.cpu_count() - 2, 1),
        )
        _dump.dump()
    except Exception as e:
        failure_logger.log(symbol, start_date, end_date, "dump_error", str(e))
        failure_logger.save()
        logger.error(f"Dump failed: {traceback.format_exc()}")
        return

    # === Step 5: Verify and update instruments/all.txt ===
    logger.info(f"Step 4: Updating instruments and calendar...")

    # Determine actual date range from normalized data
    norm_dates = pd.to_datetime(norm_df["date"]).dt.strftime("%Y-%m-%d").tolist()
    actual_start = min(norm_dates)
    actual_end = max(norm_dates)

    # For existing stocks, keep the original start_datetime
    if existing_mask.any():
        original_start = instruments_df.loc[existing_mask, "start_datetime"].iloc[0]
    else:
        original_start = actual_start

    # DumpDataUpdate.dump() already updates instruments/all.txt and calendars
    # But we verify the feature dir was actually created/updated
    if feature_dir.exists() and list(feature_dir.glob("*.bin")):
        logger.info(f"Successfully dumped bin data for {symbol_upper} to {feature_dir}")
    else:
        logger.warning(f"Feature dir not found after dump: {feature_dir}")

    # === Step 6: Cleanup temp files ===
    logger.info(f"Step 5: Cleaning up temp files...")
    try:
        for f in source_dir.glob("*.csv"):
            f.unlink()
        for f in normalize_dir.glob("*.csv"):
            f.unlink()
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")

    # Save failure log
    failure_logger.save()

    logger.info(f"=== {symbol_upper} update complete ===")
    logger.info(f"  Data range: {original_start} ~ {actual_end}")
    logger.info(f"  Feature dir: {feature_dir}")


if __name__ == "__main__":
    fire.Fire(update_single_stock)
