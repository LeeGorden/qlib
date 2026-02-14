# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# MODIFIED: New script — full market update with new stock detection

"""
Full market incremental update for qlib data.
Supports: backfill to end_date, new stock auto-detection, failure logging.

Usage (run from quant_finance/qlib/scripts/data_collector/yahoo/):
    python ../../update_qlib_data.py --qlib_data_1d_dir ~/.qlib/qlib_data/us_data --end_date 2026-02-14 --region US

Typical workflow:
    1. First run init_qlib_data.py to initialize offline data (~2020-11-10)
    2. Run this script with --end_date to backfill to target date
    3. Run daily for incremental updates (without --end_date, defaults to today)
"""

import os
import sys
import csv
import time
import datetime
import importlib
import traceback
import multiprocessing
from pathlib import Path
from typing import Optional, List

import fire
import numpy as np
import pandas as pd
from loguru import logger

# Setup path so that data_collector imports work
SCRIPT_DIR = Path(__file__).resolve().parent
YAHOO_DIR = SCRIPT_DIR / "data_collector" / "yahoo"
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(YAHOO_DIR))

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


def _get_existing_symbols(instruments_path: Path) -> set:
    """Get set of existing symbols from instruments/all.txt."""
    df = _read_instruments(instruments_path)
    return set(df["symbol"].str.upper().tolist())


def update_qlib_data(
    qlib_data_1d_dir: str = "~/.qlib/qlib_data/us_data",
    end_date: str = None,
    region: str = "US",
    delay: float = 1,
    max_workers: int = 1,
    check_data_length: int = None,
    exists_skip: bool = False,
    fail_log: str = "./update_fail_log.csv",
):
    """Full market incremental update for qlib data.

    Parameters
    ----------
    qlib_data_1d_dir : str
        qlib data directory, default ~/.qlib/qlib_data/us_data
    end_date : str
        End date (excluded), default today. e.g. 2026-02-14
    region : str
        Market region, default US
    delay : float
        Delay between requests in seconds, default 1
    max_workers : int
        Max concurrent workers for download, default 1
    check_data_length : int
        Check data length per symbol, default None
    exists_skip : bool
        Skip if qlib data already exists (for init), default False
    fail_log : str
        Path for failure log CSV, default ./update_fail_log.csv
    """
    qlib_data_1d_dir = str(Path(qlib_data_1d_dir).expanduser().resolve())
    failure_logger = FailureLogger(fail_log)

    # Validate qlib data dir
    if not exists_qlib_data(qlib_data_1d_dir):
        logger.error(
            f"Qlib data directory not found or incomplete: {qlib_data_1d_dir}\n"
            f"Please run init_qlib_data.py first."
        )
        return

    # Default end_date
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if end_date is None:
        end_date = today_str
    if pd.Timestamp(end_date) > pd.Timestamp(today_str):
        end_date = today_str
        logger.info(f"Clamped end_date to today: {end_date}")

    logger.info(f"=== Full market update: region={region} ===")
    logger.info(f"qlib_data_1d_dir: {qlib_data_1d_dir}")
    logger.info(f"end_date: {end_date}")

    # ========================================
    # Phase 1: Update existing stocks
    # ========================================
    logger.info("=" * 60)
    logger.info("Phase 1: Updating existing stocks via update_data_to_bin")
    logger.info("=" * 60)

    # Need to change cwd to yahoo dir for collector imports to work
    original_cwd = os.getcwd()
    os.chdir(str(YAHOO_DIR))

    try:
        from collector import Run

        runner = Run(
            source_dir=None,  # use default
            normalize_dir=None,  # use default
            max_workers=max_workers,
            interval="1d",
            region=region,
        )

        # Read calendar to get trading_date
        calendar_path = Path(qlib_data_1d_dir) / "calendars" / "day.txt"
        calendar_df = pd.read_csv(calendar_path)
        trading_date = (pd.Timestamp(calendar_df.iloc[-1, 0]) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(f"Current calendar end: {calendar_df.iloc[-1, 0]}")
        logger.info(f"Trading date (start): {trading_date}")
        logger.info(f"End date: {end_date}")

        if pd.Timestamp(trading_date) < pd.Timestamp(end_date):
            # Download data from yahoo
            logger.info(f"Downloading existing stocks data: {trading_date} ~ {end_date}")
            runner.download_data(
                delay=delay, start=trading_date, end=end_date,
                check_data_length=check_data_length
            )

            # Increase workers for normalize
            runner.max_workers = max(multiprocessing.cpu_count() - 2, 1)

            # Normalize with extend
            logger.info("Normalizing data (extend mode)...")
            runner.normalize_data_1d_extend(qlib_data_1d_dir)

            # Dump to bin
            logger.info("Dumping to bin format...")
            from dump_bin import DumpDataUpdate
            _dump = DumpDataUpdate(
                data_path=str(runner.normalize_dir),
                qlib_dir=qlib_data_1d_dir,
                exclude_fields="symbol,date",
                max_workers=runner.max_workers,
            )
            _dump.dump()

            # Parse index
            _region = region.lower()
            if _region in ["cn", "us"]:
                index_list = ["CSI100", "CSI300"] if _region == "cn" else ["SP500", "NASDAQ100", "DJIA", "SP400"]
                try:
                    get_instruments = getattr(
                        importlib.import_module(f"data_collector.{_region}_index.collector"),
                        "get_instruments"
                    )
                    for _index in index_list:
                        try:
                            get_instruments(str(qlib_data_1d_dir), _index, market_index=f"{_region}_index")
                        except Exception as e:
                            logger.warning(f"Failed to parse index {_index}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to import index collector: {e}")

            logger.info("Phase 1 complete: existing stocks updated.")
        else:
            logger.info("Existing stocks already up to date.")

    except Exception as e:
        logger.error(f"Phase 1 failed: {traceback.format_exc()}")
    finally:
        os.chdir(original_cwd)

    # ========================================
    # Phase 2: Detect and add new stocks
    # ========================================
    logger.info("=" * 60)
    logger.info("Phase 2: Detecting and adding new stocks")
    logger.info("=" * 60)

    os.chdir(str(YAHOO_DIR))
    try:
        from data_collector.utils import get_us_stock_symbols
        from collector import YahooCollector, YahooNormalizeUS1d
        from data_collector.base import Normalize
        from dump_bin import DumpDataUpdate

        instruments_path = Path(qlib_data_1d_dir) / "instruments" / "all.txt"
        existing_symbols = _get_existing_symbols(instruments_path)
        logger.info(f"Existing symbols in database: {len(existing_symbols)}")

        # Get latest symbol list from internet
        try:
            latest_symbols_raw = get_us_stock_symbols()
        except Exception as e:
            logger.error(f"Failed to get latest symbol list: {e}")
            latest_symbols_raw = []

        # Normalize symbol names to match qlib format (uppercase, code_to_fname)
        latest_symbols = set()
        for s in latest_symbols_raw:
            fname = code_to_fname(s).upper()
            latest_symbols.add(fname)

        new_symbols = latest_symbols - existing_symbols
        if not new_symbols:
            logger.info("No new stocks detected.")
        else:
            logger.info(f"Detected {len(new_symbols)} new stocks")

            # Process new stocks in batches
            source_dir_new = YAHOO_DIR / "source_new"
            source_dir_new.mkdir(parents=True, exist_ok=True)
            normalize_dir_new = YAHOO_DIR / "normalize_new"
            normalize_dir_new.mkdir(parents=True, exist_ok=True)

            processed = 0
            for sym_fname in sorted(new_symbols):
                # Convert back to Yahoo symbol format
                yahoo_symbol = fname_to_code(sym_fname)

                logger.info(f"[{processed+1}/{len(new_symbols)}] Downloading new stock: {yahoo_symbol}")
                try:
                    time.sleep(delay)
                    raw_df = YahooCollector.get_data_from_remote(
                        symbol=yahoo_symbol, interval="1d",
                        start="2000-01-01", end=end_date,
                    )

                    if raw_df is None or raw_df.empty:
                        failure_logger.log(
                            yahoo_symbol, "2000-01-01", end_date, "empty_data",
                            "No data returned — possibly delisted or not yet listed"
                        )
                        processed += 1
                        continue

                    raw_df["symbol"] = sym_fname
                    csv_path = source_dir_new / f"{sym_fname}.csv"
                    raw_df.to_csv(csv_path, index=False)
                    processed += 1

                except Exception as e:
                    failure_logger.log(
                        yahoo_symbol, "2000-01-01", end_date, "network_error", str(e)
                    )
                    processed += 1
                    continue

            # Normalize all new stocks
            if list(source_dir_new.glob("*.csv")):
                logger.info("Normalizing new stocks...")
                try:
                    normalizer = Normalize(
                        source_dir=source_dir_new,
                        target_dir=normalize_dir_new,
                        normalize_class=YahooNormalizeUS1d,
                        max_workers=max(multiprocessing.cpu_count() - 2, 1),
                        date_field_name="date",
                        symbol_field_name="symbol",
                    )
                    normalizer.normalize()
                except Exception as e:
                    logger.error(f"Normalize new stocks failed: {e}")

            # Dump new stocks to bin
            if list(normalize_dir_new.glob("*.csv")):
                logger.info("Dumping new stocks to bin...")
                try:
                    _dump = DumpDataUpdate(
                        data_path=str(normalize_dir_new),
                        qlib_dir=qlib_data_1d_dir,
                        exclude_fields="symbol,date",
                        max_workers=max(multiprocessing.cpu_count() - 2, 1),
                    )
                    _dump.dump()
                except Exception as e:
                    logger.error(f"Dump new stocks failed: {e}")

            # Cleanup temp files
            try:
                for f in source_dir_new.glob("*.csv"):
                    f.unlink()
                for f in normalize_dir_new.glob("*.csv"):
                    f.unlink()
            except Exception as e:
                logger.warning(f"Cleanup warning: {e}")

        logger.info("Phase 2 complete.")

    except Exception as e:
        logger.error(f"Phase 2 failed: {traceback.format_exc()}")
    finally:
        os.chdir(original_cwd)

    # Save failure log
    failure_logger.save()

    logger.info("=" * 60)
    logger.info("Full market update complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    fire.Fire(update_qlib_data)
