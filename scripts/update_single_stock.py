# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# MODIFIED: New script — update a single stock in qlib data directory

"""
Update single / batch stocks in the qlib data directory.

Single stock:
    cd data_collector/all_source
    python ../../update_single_stock.py single --symbol AAPL --qlib_data_1d_dir qlib_data/us_data

Batch update from a txt file (one symbol per line):
    cd data_collector/all_source
    python ../../update_single_stock.py batch --symbols_file watchlist.txt --qlib_data_1d_dir qlib_data/us_data
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

from tqdm import tqdm
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

# Qlib instrument name → actual Yahoo Finance ticker
# Used when the Yahoo ticker contains characters invalid in filenames (^, =, etc.)
YAHOO_TICKER_OVERRIDE = {
    "VIX": "^VIX",    # CBOE Volatility Index
    "DXY": "DX=F",    # ICE US Dollar Index Futures
}


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

    symbol_yahoo = YAHOO_TICKER_OVERRIDE.get(symbol_upper, symbol_upper)
    if symbol_yahoo != symbol_upper:
        logger.info(f"Using Yahoo ticker override: {symbol_upper} → {symbol_yahoo}")

    # For YAHOO_TICKER_OVERRIDE symbols (e.g. ^VIX), bypass the multi-source fetcher
    # and use yfinance directly — the multi-source fetcher can't handle ^ in tickers.
    if symbol_upper in YAHOO_TICKER_OVERRIDE:
        try:
            import yfinance as yf
            yf_df = yf.download(symbol_yahoo, start=start_date, end=end_date,
                                progress=False, auto_adjust=True)
            if yf_df.empty:
                failure_logger.log(symbol, start_date, end_date, "empty_data",
                                   f"yfinance returned no data for {symbol_yahoo}")
                failure_logger.save()
                return
            if isinstance(yf_df.columns, pd.MultiIndex):
                yf_df.columns = yf_df.columns.droplevel(1)
            yf_df = yf_df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume",
            })
            yf_df.index.name = "date"
            yf_df = yf_df.reset_index()
            yf_df["date"] = pd.to_datetime(yf_df["date"]).dt.tz_localize(None)
            yf_df["adjclose"] = yf_df["close"]   # index data — no splits/dividends
            # Normalizer drops rows where volume <= 0; use 1 as placeholder for index data
            yf_df["volume"] = yf_df["volume"].replace(0, 1).fillna(1).clip(lower=1)
            yf_df["symbol"] = symbol_fname
            raw_df = yf_df[["date", "open", "high", "low", "close",
                             "volume", "adjclose", "symbol"]]
            logger.info(f"yfinance fetched {len(raw_df)} rows for {symbol_yahoo}")
        except Exception as e:
            failure_logger.log(symbol, start_date, end_date, "network_error", str(e))
            failure_logger.save()
            return
    else:
        try:
            raw_df = _fetch_stock_data_multi_source(
                symbol_yahoo=symbol_yahoo,
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
    source_dir = ALL_SOURCE_DIR / "source"
    source_dir.mkdir(parents=True, exist_ok=True)

    raw_df["symbol"] = symbol_fname
    csv_path = source_dir / f"{symbol_fname}.csv"
    raw_df.to_csv(csv_path, index=False)
    logger.info(f"Saved raw CSV to: {csv_path}")

    # === Step 3: Normalize ===
    logger.info(f"Step 2: Normalizing data...")
    normalize_dir = ALL_SOURCE_DIR / "normalize"
    normalize_dir.mkdir(parents=True, exist_ok=True)

    is_new_stock = not (existing_mask.any() and feature_dir.exists())

    try:
        if is_new_stock:
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
        else:
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
    except Exception as e:
        failure_logger.log(symbol, start_date, end_date, "normalize_error", str(e))
        failure_logger.save()
        logger.error(f"Normalize failed: {traceback.format_exc()}")
        return

    # Retry loop: if normalize fails at a bad early date, truncate source and retry.
    # _executor processes one file and logs a WARNING on failure (never raises).
    # We capture that WARNING to extract the failing date, then trim the CSV.
    # If the failure is at the very start of remaining data (stuck), skip an entire year.
    import re as _re
    norm_csv = normalize_dir / f"{symbol_fname}.csv"
    current_source_df = pd.read_csv(csv_path)
    current_source_df["date"] = pd.to_datetime(current_source_df["date"])

    for _attempt in range(20):
        _failed_dates = []

        def _make_sink(bucket):
            def _sink(msg):
                text = str(msg.record["message"])
                if msg.record["level"].name == "WARNING" and "failed" in text:
                    m = _re.search(r'datetime\.date\((\d+),\s*(\d+),\s*(\d+)\)', text)
                    if m:
                        bucket.append(pd.Timestamp(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            return _sink

        _sink_id = logger.add(_make_sink(_failed_dates), level="WARNING")
        try:
            normalizer._executor(csv_path)
        finally:
            logger.remove(_sink_id)

        if norm_csv.exists():
            break  # success

        if not _failed_dates:
            logger.warning(f"Normalize failed with no recoverable date info, giving up")
            break

        cutoff = max(_failed_dates)
        min_date = current_source_df["date"].min()

        # If failure is at or near the start of remaining data, skip an entire year
        # (consecutive per-day failures mean the entire year's data is unusable)
        if (cutoff - min_date).days <= 5:
            cutoff = pd.Timestamp(cutoff.year + 1, 1, 1)
            logger.warning(f"Normalize stuck at start of data, jumping to {cutoff.date()}")

        trimmed = current_source_df[current_source_df["date"] >= cutoff]
        if trimmed.empty:
            logger.warning(f"No data remains after truncating to {cutoff.date()}, giving up")
            break
        current_source_df = trimmed.copy()
        current_source_df["symbol"] = symbol_fname
        current_source_df.to_csv(csv_path, index=False)
        logger.warning(
            f"Normalize failed at {max(_failed_dates).date()}, retrying with data from "
            f"{current_source_df['date'].min().date()} ({len(current_source_df)} rows)"
        )

    if not norm_csv.exists():
        failure_logger.log(
            symbol, start_date, end_date, "normalize_error",
            "Normalized CSV not produced after retries"
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

    # source/ and normalize/ are permanent directories shared with the bulk pipeline.
    # Files are kept (not cleaned up) so the next update_qlib_data.py run can use them.

    # Save failure log
    failure_logger.save()

    logger.info(f"=== {symbol_upper} update complete ===")
    logger.info(f"  Data range: {original_start} ~ {actual_end}")
    logger.info(f"  Feature dir: {feature_dir}")


UP_TO_DATE_TOLERANCE_DAYS = 3


def batch_update(
    symbols_file: str,
    qlib_data_1d_dir: str = "~/.qlib/qlib_data/us_data",
    end_date: str = None,
    region: str = "US",
    delay: float = 1,
    fail_log: str = "./batch_update_fail_log.csv",
):
    """Batch-update stocks listed in a text file.

    Parameters
    ----------
    symbols_file : str
        Path to a text file with one symbol per line.
        Empty lines and lines starting with '#' are ignored.
    qlib_data_1d_dir : str
        qlib data directory
    end_date : str
        End date (exclusive). Default = today.
    region : str
        Market region, default US
    delay : float
        Delay between downloads (seconds)
    fail_log : str
        Path for failure log CSV
    """
    symbols_path = Path(symbols_file).expanduser().resolve()
    if not symbols_path.exists():
        logger.error(f"Symbols file not found: {symbols_path}")
        return

    symbols = []
    for line in symbols_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("["):
            continue                         # skip comments, section/group headers
        symbol = s.split("|")[0].strip().upper()  # support "SYMBOL | description" format
        if symbol:
            symbols.append(symbol)

    if not symbols:
        logger.warning("No symbols found in file.")
        return

    symbols = list(dict.fromkeys(symbols))  # deduplicate, preserve order

    qlib_data_1d_dir = str(Path(qlib_data_1d_dir).expanduser().resolve())

    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if pd.Timestamp(end_date) > pd.Timestamp(today_str):
        end_date = today_str

    instruments_path = Path(qlib_data_1d_dir) / "instruments" / "all.txt"
    inst_df = _read_instruments(instruments_path)
    inst_end_map = {}
    for _, row in inst_df.iterrows():
        inst_end_map[row["symbol"].upper()] = row["end_datetime"]

    updated = 0
    skipped = 0
    failed = 0

    logger.info(f"Batch update: {len(symbols)} symbols, end_date={end_date}")

    pbar = tqdm(symbols, desc="Batch update", unit="sym",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

    for sym in pbar:
        sym_fname = code_to_fname(sym)
        feature_dir = Path(qlib_data_1d_dir) / "features" / sym_fname

        # Pre-check: if the stock exists and its end_datetime is recent enough, skip
        if sym_fname in inst_end_map and feature_dir.exists():
            inst_end = inst_end_map[sym_fname]
            if pd.Timestamp(inst_end) >= pd.Timestamp(end_date) - pd.Timedelta(days=UP_TO_DATE_TOLERANCE_DAYS):
                skipped += 1
                pbar.set_postfix(ok=updated, skip=skipped, fail=failed, refresh=False)
                continue

        try:
            update_single_stock(
                symbol=sym,
                qlib_data_1d_dir=qlib_data_1d_dir,
                end_date=end_date,
                region=region,
                delay=delay,
                fail_log=fail_log,
            )
            updated += 1
        except Exception as e:
            failed += 1
            logger.error(f"[{sym}] batch update failed: {e}")

        pbar.set_postfix(ok=updated, skip=skipped, fail=failed, refresh=False)

    pbar.close()
    logger.info(
        f"Batch update complete: {updated} updated, {skipped} skipped, "
        f"{failed} failed (total {len(symbols)})"
    )


if __name__ == "__main__":
    fire.Fire({
        "single": update_single_stock,
        "batch": batch_update,
    })
