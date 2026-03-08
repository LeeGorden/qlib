# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# MODIFIED: New script — full market update with new stock detection

"""
Full market incremental update for qlib data.
Supports: backfill to end_date, new stock auto-detection, failure logging.
Data sources (in priority order): Yahoo Finance → Stooq → Nasdaq Data Link.

Usage (run from quant_finance/qlib/):
    python scripts/update_qlib_data.py --qlib_data_1d_dir qlib_data/us_data --end_date 2026-02-14 --region US

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


def _get_existing_symbols(instruments_path: Path) -> set:
    """Get set of existing symbols from instruments/all.txt."""
    df = _read_instruments(instruments_path)
    return set(df["symbol"].str.upper().tolist())


def _clean_csv_dates(csv_dir: Path):
    """Strip timezone info from date columns in source CSVs so normalizer can parse them."""
    for csv_file in csv_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file, nrows=1)
            if "date" not in df.columns:
                continue
            df = pd.read_csv(csv_file)
            df["date"] = pd.to_datetime(df["date"], format="mixed", utc=True).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            df.to_csv(csv_file, index=False)
        except Exception as e:
            logger.warning(f"Failed to clean dates in {csv_file.name}: {e}")


def _clear_csv_dir(dir_path: Path):
    """Remove all CSV files from a directory."""
    if not dir_path.exists():
        return
    for f in dir_path.glob("*.csv"):
        try:
            f.unlink()
        except Exception:
            pass


def _get_csv_last_date(csv_path: Path) -> Optional[str]:
    """Read the last date from a source CSV file.

    Returns the latest date string (YYYY-MM-DD) in the CSV, or None if
    the file is empty / unreadable.
    """
    try:
        df = pd.read_csv(csv_path, usecols=["date"], dtype=str)
        if df.empty:
            return None
        dates = pd.to_datetime(df["date"], format="mixed", utc=True, errors="coerce")
        if dates.isna().all():
            dates = pd.to_datetime(df["date"], format="mixed", errors="coerce")
        if dates.isna().all():
            return None
        return dates.max().strftime("%Y-%m-%d")
    except Exception:
        return None


# =====================================================================
# Multi-source data fetcher: Yahoo → Stooq → Nasdaq Data Link fallback
# =====================================================================

def _fetch_stock_data_multi_source(
    symbol_yahoo: str,
    symbol_fname: str,
    start: str,
    end: str,
) -> Optional[pd.DataFrame]:
    """Try downloading stock data from multiple sources: Yahoo → Stooq → Nasdaq Data Link.

    This is the SAME fallback chain used in Phase 1 batch download
    (YahooCollector.get_data) and in Phase 1.5 / Phase 2 individual downloads.

    Raises ``RateLimitError`` immediately if Yahoo responds with a rate-limit
    signal — the caller should catch it and stop downloading.

    Parameters
    ----------
    symbol_yahoo : str
        Symbol in Yahoo format (e.g. "AAPL", "BRK-B")
    symbol_fname : str
        Symbol in fname format (e.g. "AAPL", "BRK_B")
    start : str
        Start date
    end : str
        End date

    Returns
    -------
    pd.DataFrame or None
        DataFrame with at least 'date' column, or None if all sources fail.
        A '_source' column is added to indicate which source provided the data.
    """
    from collector import YahooCollector
    from data_collector.utils import RateLimitError

    # Source 1: Yahoo Finance (primary)
    try:
        df = YahooCollector.get_data_from_remote(
            symbol=symbol_yahoo, interval="1d", start=start, end=end,
        )
        if df is not None and not df.empty:
            df["_source"] = "yahoo"
            return df
    except RateLimitError:
        raise  # Propagate immediately — caller must handle
    except Exception:
        pass

    # Source 2: Stooq (fallback)
    try:
        df = YahooCollector.get_data_from_stooq(symbol_fname, start, end)
        if df is not None and not df.empty:
            df["_source"] = "stooq"
            logger.info(f"  [Stooq fallback] Got {len(df)} rows for {symbol_fname}")
            return df
    except Exception:
        pass

    # Source 3: Nasdaq Data Link (fallback #2 — free WIKI dataset, data up to ~2018-03)
    try:
        df = YahooCollector.get_data_from_nasdaq_data_link(symbol_fname, start, end)
        if df is not None and not df.empty:
            df["_source"] = "nasdaq_data_link"
            return df
    except Exception:
        pass

    return None


def _reconcile_instruments_from_bin(qlib_data_dir: str) -> pd.DataFrame:
    """Scan feature bin files to determine accurate date ranges, rebuild instruments/all.txt.

    This is the KEY fix: DumpDataUpdate only updates end_datetime for stocks that received
    new data in the normalize CSV. Stocks missed by the download keep stale dates.
    This function reads the actual bin files — the ground truth — and rebuilds all.txt
    so that every stock's date range matches its real data.

    Returns
    -------
    pd.DataFrame : reconciled instruments DataFrame
    """
    qlib_data_dir = Path(qlib_data_dir)
    calendar_path = qlib_data_dir / "calendars" / "day.txt"
    features_dir = qlib_data_dir / "features"
    instruments_path = qlib_data_dir / "instruments" / "all.txt"

    if not calendar_path.exists() or not features_dir.exists():
        logger.warning("Calendar or features dir missing, skipping reconciliation")
        return pd.DataFrame()

    # Read calendar
    calendar_df = pd.read_csv(calendar_path, header=None)
    calendar_list = sorted(pd.to_datetime(calendar_df[0]).tolist())
    cal_len = len(calendar_list)

    if cal_len == 0:
        logger.warning("Calendar is empty, skipping reconciliation")
        return pd.DataFrame()

    instruments_data = []
    errors = 0

    for stock_dir in sorted(features_dir.iterdir()):
        if not stock_dir.is_dir():
            continue

        # Find a representative bin file (prefer close.day.bin)
        bin_file = stock_dir / "close.day.bin"
        if not bin_file.exists():
            bin_files = list(stock_dir.glob("*.day.bin"))
            if not bin_files:
                continue
            bin_file = bin_files[0]

        try:
            file_size = bin_file.stat().st_size
            if file_size < 8:  # need at least date_index (4B) + 1 value (4B)
                continue

            # Bin format: [start_calendar_index_f32, val1_f32, val2_f32, ...]
            data = np.fromfile(str(bin_file), dtype="<f")
            if len(data) < 2:
                continue

            start_index = int(data[0])
            num_data = len(data) - 1

            if start_index < 0 or start_index >= cal_len or num_data <= 0:
                errors += 1
                continue

            end_index = start_index + num_data - 1
            if end_index >= cal_len:
                # Data extends beyond calendar — clamp to calendar end
                end_index = cal_len - 1

            start_date = calendar_list[start_index].strftime("%Y-%m-%d")
            end_date = calendar_list[end_index].strftime("%Y-%m-%d")

            symbol = fname_to_code(stock_dir.name).upper()
            instruments_data.append({
                "symbol": symbol,
                "start_datetime": start_date,
                "end_datetime": end_date,
            })
        except Exception as e:
            errors += 1
            logger.debug(f"Failed to read bin for {stock_dir.name}: {e}")

    if instruments_data:
        df = pd.DataFrame(instruments_data)
        df = df.sort_values("symbol").reset_index(drop=True)
        instruments_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(instruments_path, sep="\t", header=False, index=False)
        logger.info(
            f"Reconciled instruments: {len(df)} stocks written to all.txt"
            + (f" ({errors} read errors)" if errors else "")
        )
        return df

    logger.warning("No valid bin data found for reconciliation")
    return pd.DataFrame()



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
    """Incremental market update for qlib data (two-step approach).

    Step 1: Merge symbol lists (existing instruments + online sources).
    Step 2: For each symbol, check source CSV last date, download only
            incremental data, then normalize + dump.

    Parameters
    ----------
    qlib_data_1d_dir : str
        qlib data directory, default ~/.qlib/qlib_data/us_data
    end_date : str
        End date (excluded), default today. e.g. 2026-03-07
    region : str
        Market region, default US
    delay : float
        Delay between requests in seconds, default 1
    max_workers : int
        Max concurrent workers for normalize, default 1
    check_data_length : int
        (unused, kept for CLI compat)
    exists_skip : bool
        (unused, kept for CLI compat)
    fail_log : str
        Path for failure log CSV, default ./update_fail_log.csv
    """
    qlib_data_1d_dir = str(Path(qlib_data_1d_dir).expanduser().resolve())
    failure_logger = FailureLogger(fail_log)

    if not exists_qlib_data(qlib_data_1d_dir):
        logger.error(
            f"Qlib data directory not found or incomplete: {qlib_data_1d_dir}\n"
            f"Please run init_qlib_data.py first."
        )
        return

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if end_date is None:
        end_date = today_str
    if pd.Timestamp(end_date) > pd.Timestamp(today_str):
        end_date = today_str
        logger.info(f"Clamped end_date to today: {end_date}")

    logger.info(f"=== Incremental market update: region={region} ===")
    logger.info(f"qlib_data_1d_dir: {qlib_data_1d_dir}")
    logger.info(f"end_date: {end_date}")

    original_cwd = os.getcwd()
    os.chdir(str(ALL_SOURCE_DIR))

    try:
        from collector import Run
        from data_collector.utils import get_us_stock_symbols, RateLimitError

        # ========================================================
        # Step 1: Build complete symbol list (existing ∪ online)
        # ========================================================
        logger.info("=" * 60)
        logger.info("Step 1: Building complete symbol list")
        logger.info("=" * 60)

        instruments_path = Path(qlib_data_1d_dir) / "instruments" / "all.txt"
        existing_symbols = _get_existing_symbols(instruments_path)
        logger.info(f"Existing symbols in database: {len(existing_symbols)}")

        inst_df = _read_instruments(instruments_path)
        inst_end_map = {}
        for _, row in inst_df.iterrows():
            sym = str(row["symbol"]).upper()
            inst_end_map[sym] = str(row["end_datetime"])

        try:
            online_raw = get_us_stock_symbols(qlib_data_path=qlib_data_1d_dir)
        except Exception:
            try:
                online_raw = get_us_stock_symbols()
            except Exception:
                online_raw = []
        online_raw += ["^GSPC", "^NDX", "^DJI"]
        online_symbols = {code_to_fname(s).upper() for s in online_raw}

        all_symbols = existing_symbols | online_symbols
        new_symbols = all_symbols - existing_symbols
        logger.info(
            f"Online: {len(online_symbols)}, "
            f"New: {len(new_symbols)}, "
            f"Total: {len(all_symbols)}"
        )

        # ========================================================
        # Step 2: Incremental download — one loop for all symbols
        # ========================================================
        logger.info("=" * 60)
        logger.info("Step 2: Incremental download")
        logger.info("=" * 60)

        runner = Run(
            source_dir=None, normalize_dir=None,
            max_workers=max_workers, interval="1d", region=region,
        )
        source_dir = runner.source_dir

        UP_TO_DATE_TOLERANCE_DAYS = 3
        MAX_CONSECUTIVE_FAILURES = 15

        total = len(all_symbols)
        downloaded = 0
        skipped = 0
        failed = 0
        consecutive_failures = 0

        logger.info(f"Scanning {total} symbols for incremental download...")

        for i, sym_fname in enumerate(sorted(all_symbols), 1):
            csv_path = source_dir / f"{sym_fname}.csv"
            yahoo_symbol = fname_to_code(sym_fname.lower())

            # --- Determine download range ---
            csv_last = None
            if csv_path.exists() and csv_path.stat().st_size > 100:
                csv_last = _get_csv_last_date(csv_path)

            if csv_last:
                if pd.Timestamp(csv_last) >= pd.Timestamp(end_date) - pd.Timedelta(days=UP_TO_DATE_TOLERANCE_DAYS):
                    skipped += 1
                    continue
                dl_start = csv_last
            elif sym_fname in inst_end_map:
                dl_start = inst_end_map[sym_fname]
            else:
                dl_start = "2000-01-01"

            if pd.Timestamp(dl_start) >= pd.Timestamp(end_date):
                skipped += 1
                continue

            if i % 500 == 0:
                logger.info(
                    f"  Progress: {i}/{total} "
                    f"(downloaded={downloaded}, skipped={skipped}, failed={failed})"
                )

            try:
                time.sleep(delay)
                raw_df = _fetch_stock_data_multi_source(
                    symbol_yahoo=yahoo_symbol,
                    symbol_fname=sym_fname,
                    start=dl_start,
                    end=end_date,
                )

                if raw_df is None or raw_df.empty:
                    consecutive_failures += 1
                    failed += 1
                    failure_logger.log(
                        yahoo_symbol, dl_start, end_date, "empty_data",
                        "All sources returned no data"
                    )
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.error(
                            f"Rate limit likely ({consecutive_failures} consecutive "
                            f"failures). Stopping download loop."
                        )
                        break
                    continue

                consecutive_failures = 0

                if "_source" in raw_df.columns:
                    raw_df = raw_df.drop(columns=["_source"])

                if "date" not in raw_df.columns and hasattr(raw_df.index, "name") and raw_df.index.name == "date":
                    raw_df = raw_df.reset_index()

                if "date" in raw_df.columns:
                    try:
                        raw_df["date"] = pd.to_datetime(
                            raw_df["date"], utc=True
                        ).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
                    except Exception:
                        raw_df["date"] = pd.to_datetime(
                            raw_df["date"]
                        ).dt.strftime("%Y-%m-%d")

                raw_df["symbol"] = sym_fname

                # Append to existing CSV (dedup by date) or create new
                if csv_last and csv_path.exists():
                    try:
                        existing_df = pd.read_csv(csv_path, dtype={"symbol": str})
                        combined = pd.concat([existing_df, raw_df], ignore_index=True)
                        combined["_dt"] = pd.to_datetime(combined["date"], format="mixed")
                        combined = combined.drop_duplicates(subset=["_dt"], keep="last")
                        combined = combined.sort_values("_dt").reset_index(drop=True)
                        combined["date"] = combined["_dt"].dt.strftime("%Y-%m-%d")
                        combined = combined.drop(columns=["_dt"])
                        combined.to_csv(csv_path, index=False)
                    except Exception:
                        raw_df.to_csv(csv_path, index=False)
                else:
                    raw_df.to_csv(csv_path, index=False)

                downloaded += 1

            except RateLimitError:
                logger.error("Rate limit detected. Stopping download loop.")
                break

            except Exception as e:
                consecutive_failures += 1
                failed += 1
                failure_logger.log(yahoo_symbol, dl_start, end_date, "error", str(e))
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        f"Rate limit likely ({consecutive_failures} consecutive "
                        f"failures). Stopping download loop."
                    )
                    break

        logger.info(
            f"Download complete: {downloaded} downloaded, "
            f"{skipped} skipped, {failed} failed (out of {total})"
        )

        # ========================================================
        # Step 3: Clean dates → Normalize → Dump
        # ========================================================
        logger.info("=" * 60)
        logger.info("Step 3: Normalize and dump")
        logger.info("=" * 60)

        logger.info("Cleaning date formats in source CSVs...")
        _clean_csv_dates(source_dir)

        _clear_csv_dir(runner.normalize_dir)

        normalize_workers = min(max(multiprocessing.cpu_count() - 2, 1), 4)
        runner.max_workers = normalize_workers
        logger.info(f"Normalizing data (extend mode, workers={normalize_workers})...")
        runner.normalize_data_1d_extend(qlib_data_1d_dir)

        logger.info("Dumping to bin format...")
        from dump_bin import DumpDataUpdate
        _dump = DumpDataUpdate(
            data_path=str(runner.normalize_dir),
            qlib_dir=qlib_data_1d_dir,
            exclude_fields="symbol,date",
            max_workers=normalize_workers,
        )
        _dump.dump()

        # Parse index
        _region = region.lower()
        if _region in ["cn", "us"]:
            index_list = (
                ["CSI100", "CSI300"] if _region == "cn"
                else ["SP500", "NASDAQ100", "DJIA", "SP400"]
            )
            try:
                get_instruments = getattr(
                    importlib.import_module(f"data_collector.{_region}_index.collector"),
                    "get_instruments",
                )
                for _index in index_list:
                    try:
                        get_instruments(
                            str(qlib_data_1d_dir), _index,
                            market_index=f"{_region}_index",
                        )
                    except Exception as e:
                        logger.warning(f"Failed to parse index {_index}: {e}")
            except Exception as e:
                logger.warning(f"Failed to import index collector: {e}")

    except Exception:
        logger.error(f"Update encountered an error: {traceback.format_exc()}")
        logger.info("Attempting emergency normalize+dump on existing source CSVs...")
        try:
            os.chdir(str(ALL_SOURCE_DIR))
            from collector import Run
            runner = Run(
                source_dir=None, normalize_dir=None,
                max_workers=1, interval="1d", region=region,
            )
            if list(runner.source_dir.glob("*.csv")):
                _clear_csv_dir(runner.normalize_dir)
                _clean_csv_dates(runner.source_dir)
                nw = min(max(multiprocessing.cpu_count() - 2, 1), 4)
                runner.max_workers = nw
                runner.normalize_data_1d_extend(qlib_data_1d_dir)
                from dump_bin import DumpDataUpdate
                DumpDataUpdate(
                    data_path=str(runner.normalize_dir),
                    qlib_dir=qlib_data_1d_dir,
                    exclude_fields="symbol,date",
                    max_workers=nw,
                ).dump()
                logger.info("Emergency normalize+dump complete.")
        except Exception as inner_e:
            logger.error(f"Emergency normalize+dump also failed: {inner_e}")
    finally:
        os.chdir(original_cwd)

    # ========================================================
    # Step 4: Reconcile instruments/all.txt from bin data
    # ========================================================
    logger.info("=" * 60)
    logger.info("Reconciling instruments/all.txt from bin data...")
    logger.info("=" * 60)
    final_df = _reconcile_instruments_from_bin(qlib_data_1d_dir)

    if not final_df.empty:
        updated_count = len(final_df[final_df["end_datetime"] >= end_date])
        stale_count = len(final_df[final_df["end_datetime"] < end_date])
        logger.info(f"Summary: {len(final_df)} total stocks")
        logger.info(f"  Up-to-date (end >= {end_date}): {updated_count}")
        logger.info(f"  Stale (end < {end_date}): {stale_count}")
        if stale_count > 0:
            logger.info(
                f"  Stale stocks may be delisted or had download failures. "
                f"Check {failure_logger.fail_log_path} for details."
            )

    failure_logger.save()

    logger.info("=" * 60)
    logger.info("Incremental market update complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    fire.Fire(update_qlib_data)
