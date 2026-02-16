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
from typing import Optional, List

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


def _get_uptodate_symbols(qlib_data_dir: str, end_date: str, tolerance_days: int = 5) -> set:
    """Scan bin files to find symbols whose data is already up-to-date.

    A stock is considered "up-to-date" if its bin data ends within
    ``tolerance_days`` of ``end_date``.  This allows us to skip
    re-downloading stocks that were already successfully updated in a
    previous (possibly interrupted) run.

    Parameters
    ----------
    qlib_data_dir : str
        Path to the qlib data directory (e.g. us_data)
    end_date : str
        Target end date for the update
    tolerance_days : int
        Number of trading days tolerance for "up-to-date" check.
        Default 5 (≈ 1 trading week) to account for weekends/holidays.

    Returns
    -------
    set
        Uppercase fname-format symbols that are already up-to-date.
    """
    qlib_data_dir = Path(qlib_data_dir)
    calendar_path = qlib_data_dir / "calendars" / "day.txt"
    features_dir = qlib_data_dir / "features"

    if not calendar_path.exists() or not features_dir.exists():
        return set()

    calendar_df = pd.read_csv(calendar_path, header=None)
    calendar_list = sorted(pd.to_datetime(calendar_df[0]).tolist())
    cal_len = len(calendar_list)
    if cal_len == 0:
        return set()

    target_ts = pd.Timestamp(end_date)
    # Find the calendar date that is tolerance_days before end_date
    threshold_ts = target_ts - pd.Timedelta(days=tolerance_days + 2)  # +2 for weekends

    uptodate = set()
    for stock_dir in features_dir.iterdir():
        if not stock_dir.is_dir():
            continue
        bin_file = stock_dir / "close.day.bin"
        if not bin_file.exists():
            bin_files = list(stock_dir.glob("*.day.bin"))
            if not bin_files:
                continue
            bin_file = bin_files[0]
        try:
            data = np.fromfile(str(bin_file), dtype="<f")
            if len(data) < 2:
                continue
            start_index = int(data[0])
            num_data = len(data) - 1
            end_index = start_index + num_data - 1
            if end_index < 0 or end_index >= cal_len:
                continue
            bin_end_date = calendar_list[end_index]
            if bin_end_date >= threshold_ts:
                sym = fname_to_code(stock_dir.name).upper()
                uptodate.add(sym)
        except Exception:
            continue

    return uptodate


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


def _download_supplement_stocks(
    missed_symbols: set,
    source_dir: Path,
    download_start: str,
    end_date: str,
    delay: float,
    failure_logger: FailureLogger,
    per_stock_start: dict = None,
):
    """Download data for existing stocks that the batch download missed.

    These are typically ETFs, SPACs, or stocks on exchanges not covered by
    get_us_stock_symbols(). We download them individually using Yahoo API.

    NOTE: We always use ``end_date`` (the global target date) as the download
    end for every stock.  We do NOT try to pre-cap the end date for "likely
    delisted" stocks because ``end_datetime`` in all.txt only tells us
    "last date we have data for", NOT "actual delisting date".  Almost every
    stock that needs updating will satisfy ``end_datetime < end_date``.
    Yahoo / Stooq handle delisted stocks gracefully — they return data up to
    the actual last trading day or return empty.  After downloading,
    ``_reconcile_instruments_from_bin()`` rebuilds all.txt from the real bin
    data, so ``end_datetime`` will reflect the true data range.

    Parameters
    ----------
    missed_symbols : set
        Set of uppercase fname-format symbols to download
    source_dir : Path
        Directory to save CSV files (same as Phase 1 source_dir)
    download_start : str
        Default start date for download
    end_date : str
        End date for download (always the global target date)
    delay : float
        Delay between requests
    failure_logger : FailureLogger
        Logger for failures
    per_stock_start : dict, optional
        Mapping of symbol → custom start date for stale outlier stocks
        whose end_datetime is earlier than the batch download_start

    Raises
    ------
    RateLimitError
        If Yahoo responds with a rate-limit signal or too many consecutive
        failures are detected.  The caller should catch this and stop.
    """
    from data_collector.utils import RateLimitError

    MAX_CONSECUTIVE_FAILURES = 15  # likely rate-limited if this many fail in a row

    if per_stock_start is None:
        per_stock_start = {}

    total = len(missed_symbols)
    success = 0
    skipped = 0
    stooq_hits = 0
    consecutive_failures = 0
    logger.info(f"Supplementing {total} stocks (Yahoo → Stooq → NDL fallback)...")

    for i, sym_fname in enumerate(sorted(missed_symbols), 1):
        # Resume: skip if CSV already exists from a previous run
        csv_path = source_dir / f"{sym_fname}.csv"
        if csv_path.exists() and csv_path.stat().st_size > 100:
            skipped += 1
            continue

        yahoo_symbol = fname_to_code(sym_fname.lower())
        stock_start = per_stock_start.get(sym_fname, download_start)

        # Skip if start >= end (no data range to download)
        if pd.Timestamp(stock_start) >= pd.Timestamp(end_date):
            logger.info(f"  Skipping {sym_fname}: start ({stock_start}) >= end ({end_date})")
            continue

        if i % 200 == 0 or i == total:
            logger.info(
                f"  Supplement progress: {i}/{total} "
                f"({success} success, {stooq_hits} from Stooq)"
            )

        try:
            time.sleep(delay)
            raw_df = _fetch_stock_data_multi_source(
                symbol_yahoo=yahoo_symbol,
                symbol_fname=sym_fname,
                start=stock_start,
                end=end_date,
            )

            if raw_df is None or raw_df.empty:
                consecutive_failures += 1
                failure_logger.log(
                    yahoo_symbol, stock_start, end_date, "empty_data",
                    f"Supplement: Yahoo + Stooq + NDL all returned no data (requested {stock_start}~{end_date})"
                )
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        f"{consecutive_failures} consecutive empty results — likely rate-limited. "
                        f"Stopping supplement download loop."
                    )
                    logger.info("Successfully downloaded data will still be normalized and dumped.")
                    break  # Exit download loop but DON'T raise
                continue

            consecutive_failures = 0  # Reset on success

            # Track which source provided the data
            if "_source" in raw_df.columns:
                if (raw_df["_source"] == "stooq").any():
                    stooq_hits += 1
                raw_df = raw_df.drop(columns=["_source"])

            # Ensure 'date' is a column (not index) and 'symbol' is set
            if "date" not in raw_df.columns and raw_df.index.name == "date":
                raw_df = raw_df.reset_index()
            raw_df["symbol"] = sym_fname

            csv_path = source_dir / f"{sym_fname}.csv"
            raw_df.to_csv(csv_path, index=False)
            success += 1

        except RateLimitError:
            logger.error("RATE LIMIT detected during supplement download — stopping download loop.")
            logger.info("Successfully downloaded data will still be normalized and dumped.")
            break  # Exit download loop but DON'T raise — let caller normalize what we have

        except Exception as e:
            consecutive_failures += 1
            failure_logger.log(
                yahoo_symbol, stock_start, end_date, "supplement_error", str(e)
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    f"{consecutive_failures} consecutive failures — likely rate-limited. "
                    f"Stopping supplement download loop."
                )
                logger.info("Successfully downloaded data will still be normalized and dumped.")
                break  # Exit download loop but DON'T raise

    logger.info(
        f"Supplement download complete: {success}/{total} succeeded "
        f"({stooq_hits} from Stooq fallback)"
        + (f", {skipped} skipped (CSV cache)" if skipped else "")
    )
    return success


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

    # ----------------------------------------------------------
    # Pre-check: snapshot existing symbols BEFORE any changes
    # ----------------------------------------------------------
    instruments_path = Path(qlib_data_1d_dir) / "instruments" / "all.txt"
    original_existing_symbols = _get_existing_symbols(instruments_path)
    logger.info(f"Original symbols in database: {len(original_existing_symbols)}")

    # Determine download start for the batch download.
    # Using the absolute minimum end_datetime is dangerous — one outlier stock
    # (e.g. end=2012) would force downloading 13+ years for ALL 12k stocks.
    # Instead we use the 5th-percentile end_datetime so the batch covers ~95%
    # of stocks. Stocks below that threshold are handled individually in the
    # supplement phase (Phase 1.5) with their own per-stock start dates.
    inst_df = _read_instruments(instruments_path)
    calendar_path = Path(qlib_data_1d_dir) / "calendars" / "day.txt"
    calendar_df = pd.read_csv(calendar_path)
    calendar_end = pd.Timestamp(calendar_df.iloc[-1, 0])

    end_dates = pd.to_datetime(inst_df["end_datetime"])
    p5_end = end_dates.quantile(0.05)          # 5th-percentile
    earliest_end = end_dates.min()
    batch_start_base = min(p5_end, calendar_end)
    download_start = (batch_start_base - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Current calendar end: {calendar_end.strftime('%Y-%m-%d')}")
    logger.info(f"Earliest instrument end_datetime: {earliest_end.strftime('%Y-%m-%d')}")
    logger.info(f"5th-percentile end_datetime: {p5_end.strftime('%Y-%m-%d')}")
    logger.info(f"Batch download start (with 1-day overlap): {download_start}")

    # ========================================
    # Phase 1: Batch download + supplement + normalize + dump
    # ========================================
    logger.info("=" * 60)
    logger.info("Phase 1: Updating existing stocks")
    logger.info("=" * 60)

    original_cwd = os.getcwd()
    os.chdir(str(ALL_SOURCE_DIR))

    try:
        from collector import Run, YahooCollector

        runner = Run(
            source_dir=None,
            normalize_dir=None,
            max_workers=max_workers,
            interval="1d",
            region=region,
        )

        if pd.Timestamp(download_start) < pd.Timestamp(end_date):
            # ----------------------------------------------------------
            # Resume logic: detect stocks whose bin data is already
            # up-to-date so we can skip redundant downloads on re-run.
            # ----------------------------------------------------------
            uptodate_symbols = _get_uptodate_symbols(qlib_data_1d_dir, end_date)
            logger.info(f"Stocks already up-to-date in bin data: {len(uptodate_symbols)}")

            # Count existing CSVs from a previous (possibly interrupted) run.
            # We do NOT clear them — they serve as a download cache.
            existing_csvs = {f.stem.upper() for f in runner.source_dir.glob("*.csv")}
            if existing_csvs:
                logger.info(
                    f"Found {len(existing_csvs)} existing source CSVs from previous run (resume mode)"
                )

            # Always clear normalize dir (it's cheap to regenerate from source CSVs)
            _clear_csv_dir(runner.normalize_dir)

            # --- Step 1a: Batch download from online symbol list ---
            # Skip Phase 1a entirely if we already have enough CSVs from a previous
            # run (threshold: 80% of original existing symbols). This means Phase 1a
            # was likely completed before the interruption.
            phase1a_threshold = int(len(original_existing_symbols) * 0.8)
            if len(existing_csvs) >= phase1a_threshold and existing_csvs:
                logger.info(
                    f"Step 1a: SKIPPED — {len(existing_csvs)} CSVs already exist "
                    f"(threshold: {phase1a_threshold}). Using cached data from previous run."
                )
            else:
                logger.info(f"Step 1a: Batch downloading stocks: {download_start} ~ {end_date}")
                runner.download_data(
                    delay=delay, start=download_start, end=end_date,
                    check_data_length=check_data_length
                )

            # --- Step 1b: Supplement download for missed existing stocks ---
            # Two categories need supplementing:
            #   (a) Stocks not in the online list (ETFs, SPACs, etc.)
            #   (b) Stocks whose end_datetime < download_start (stale outliers
            #       that need an earlier start to create an overlap point)
            downloaded_fnames = {f.stem.upper() for f in runner.source_dir.glob("*.csv")}
            missed_existing = original_existing_symbols - downloaded_fnames

            # Remove already up-to-date stocks from missed list — no need to
            # re-download them; their bin data is already current.
            if uptodate_symbols:
                skipped_count = len(missed_existing & uptodate_symbols)
                missed_existing -= uptodate_symbols
                if skipped_count:
                    logger.info(
                        f"  Skipped {skipped_count} stocks already up-to-date in bin data"
                    )

            # Build per-stock start dates for stale outliers.
            stale_start_map = {}  # symbol → per-stock download start
            
            for _, row in inst_df.iterrows():
                sym = str(row["symbol"]).upper()
                sym_end = pd.Timestamp(row["end_datetime"])
                
                if sym_end < pd.Timestamp(download_start):
                    # This stock's end is earlier than the batch start;
                    # it needs its own start date to create the overlap point.
                    stale_start_map[sym] = (sym_end - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

            # Stocks in the batch but stale — re-download with correct range
            # Also exclude up-to-date stocks (they don't need re-downloading)
            stale_in_batch = set(stale_start_map.keys()) & downloaded_fnames - uptodate_symbols
            if stale_in_batch:
                logger.info(
                    f"Step 1b-stale: {len(stale_in_batch)} stale stocks need "
                    f"wider download range (end < {download_start})"
                )
                _download_supplement_stocks(
                    missed_symbols=stale_in_batch,
                    source_dir=runner.source_dir,
                    download_start=download_start,
                    end_date=end_date,
                    delay=delay,
                    failure_logger=failure_logger,
                    per_stock_start=stale_start_map,
                )

            if missed_existing:
                # Merge stale start dates for missed symbols
                missed_stale_map = {s: stale_start_map[s] for s in missed_existing if s in stale_start_map}
                logger.info(
                    f"Step 1b-missed: {len(missed_existing)} existing symbols not in batch download"
                    + (f" ({len(missed_stale_map)} stale)" if missed_stale_map else "")
                )
                _download_supplement_stocks(
                    missed_symbols=missed_existing,
                    source_dir=runner.source_dir,
                    download_start=download_start,
                    end_date=end_date,
                    delay=delay,
                    failure_logger=failure_logger,
                    per_stock_start=missed_stale_map if missed_stale_map else None,
                )
            else:
                logger.info("Step 1b: All existing symbols covered by batch download")

            # --- Step 2: Clean date formats in source CSVs before normalize ---
            logger.info("Step 2: Cleaning date formats in source CSVs...")
            _clean_csv_dates(runner.source_dir)

            # --- Step 3: Normalize with extend mode ---
            normalize_workers = min(max(multiprocessing.cpu_count() - 2, 1), 4)
            runner.max_workers = normalize_workers
            logger.info(f"Step 3: Normalizing data (extend mode, workers={normalize_workers})...")
            runner.normalize_data_1d_extend(qlib_data_1d_dir)

            # --- Step 4: Dump to bin ---
            logger.info("Step 4: Dumping to bin format...")
            from dump_bin import DumpDataUpdate
            _dump = DumpDataUpdate(
                data_path=str(runner.normalize_dir),
                qlib_dir=qlib_data_1d_dir,
                exclude_fields="symbol,date",
                max_workers=normalize_workers,
            )
            _dump.dump()

            # --- Step 5: Parse index ---
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
        logger.error(f"Phase 1 encountered an error: {traceback.format_exc()}")
        logger.info("Attempting to normalize and dump whatever data was downloaded so far...")
        # Even on error, try to normalize/dump/reconcile whatever CSVs exist
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
                normalize_workers = min(max(multiprocessing.cpu_count() - 2, 1), 4)
                runner.max_workers = normalize_workers
                logger.info("Emergency normalize...")
                runner.normalize_data_1d_extend(qlib_data_1d_dir)
                logger.info("Emergency dump to bin...")
                from dump_bin import DumpDataUpdate
                _dump = DumpDataUpdate(
                    data_path=str(runner.normalize_dir),
                    qlib_dir=qlib_data_1d_dir,
                    exclude_fields="symbol,date",
                    max_workers=normalize_workers,
                )
                _dump.dump()
                logger.info("Emergency normalize+dump complete.")
        except Exception as inner_e:
            logger.error(f"Emergency normalize+dump also failed: {inner_e}")
    finally:
        os.chdir(original_cwd)

    # ========================================
    # Reconcile all.txt from actual bin data
    # ========================================
    logger.info("=" * 60)
    logger.info("Reconciling instruments/all.txt from bin data (mid-point)...")
    logger.info("=" * 60)
    _reconcile_instruments_from_bin(qlib_data_1d_dir)

    # ========================================
    # Phase 2: Detect and add new stocks
    # ========================================
    logger.info("=" * 60)
    logger.info("Phase 2: Detecting and adding new stocks")
    logger.info("=" * 60)

    os.chdir(str(ALL_SOURCE_DIR))
    try:
        from data_collector.utils import get_us_stock_symbols
        from collector import YahooCollector, YahooNormalizeUS1d
        from data_collector.base import Normalize
        from dump_bin import DumpDataUpdate

        # Re-read current symbols (after Phase 1 + reconciliation)
        current_symbols = _get_existing_symbols(instruments_path)
        logger.info(f"Current symbols after Phase 1 + reconciliation: {len(current_symbols)}")

        # Get latest symbol list from internet.
        # Pass qlib_data_path so index component stocks are included.
        try:
            latest_symbols_raw = get_us_stock_symbols(qlib_data_path=qlib_data_1d_dir)
        except Exception as e:
            logger.warning(f"get_us_stock_symbols with qlib_data_path failed, trying without: {e}")
            try:
                latest_symbols_raw = get_us_stock_symbols()
            except Exception as e2:
                logger.error(f"Failed to get latest symbol list: {e2}")
                latest_symbols_raw = []

        logger.info(f"Online symbol sources returned {len(latest_symbols_raw)} symbols")

        latest_symbols = set()
        for s in latest_symbols_raw:
            fname = code_to_fname(s).upper()
            latest_symbols.add(fname)

        # Also check feature directories: Phase 1 DumpDataUpdate may have created
        # feature dirs for stocks that exist in Yahoo but aren't yet in all.txt.
        # These would have been picked up by reconciliation, but let's be safe.
        features_dir = Path(qlib_data_1d_dir) / "features"
        if features_dir.exists():
            for stock_dir in features_dir.iterdir():
                if stock_dir.is_dir():
                    sym = fname_to_code(stock_dir.name).upper()
                    if sym not in current_symbols:
                        # Already has bin data but not in all.txt (reconciliation
                        # should have caught this, but if bin was too small it
                        # might have been skipped). Not truly "new".
                        pass

        new_symbols = latest_symbols - current_symbols

        if not new_symbols:
            logger.info("No new stocks to add.")
        else:
            logger.info(f"Detected {len(new_symbols)} new stocks to download")

            source_dir_new = ALL_SOURCE_DIR / "source_new"
            source_dir_new.mkdir(parents=True, exist_ok=True)
            normalize_dir_new = ALL_SOURCE_DIR / "normalize_new"
            normalize_dir_new.mkdir(parents=True, exist_ok=True)

            # Don't clear source CSVs — they act as a download cache for resume.
            # Only clear normalize dir (cheap to regenerate).
            _clear_csv_dir(normalize_dir_new)

            from data_collector.utils import RateLimitError as _RLE

            MAX_CONSECUTIVE_FAILURES = 15
            processed = 0
            success = 0
            skipped_csv = 0
            stooq_hits = 0
            consecutive_failures = 0
            rate_limited = False

            for sym_fname in sorted(new_symbols):
                yahoo_symbol = fname_to_code(sym_fname)

                processed += 1

                # Resume: skip if CSV already exists from a previous run
                csv_path = source_dir_new / f"{sym_fname}.csv"
                if csv_path.exists() and csv_path.stat().st_size > 100:
                    skipped_csv += 1
                    success += 1  # count as success for progress
                    continue

                if processed % 200 == 0 or processed == len(new_symbols):
                    logger.info(
                        f"  New stock progress: {processed}/{len(new_symbols)} "
                        f"({success} success, {stooq_hits} from Stooq, {skipped_csv} cached)"
                    )

                try:
                    time.sleep(delay)
                    raw_df = _fetch_stock_data_multi_source(
                        symbol_yahoo=yahoo_symbol,
                        symbol_fname=sym_fname,
                        start="2000-01-01",
                        end=end_date,
                    )

                    if raw_df is None or raw_df.empty:
                        consecutive_failures += 1
                        failure_logger.log(
                            yahoo_symbol, "2000-01-01", end_date, "empty_data",
                            "Yahoo + Stooq + NDL all returned no data — possibly delisted or not yet listed"
                        )
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                            logger.error(
                                f"RATE LIMIT DETECTED: {consecutive_failures} consecutive "
                                f"failures in Phase 2. Stopping new-stock download."
                            )
                            rate_limited = True
                            break
                        continue

                    consecutive_failures = 0  # Reset on success

                    # Track source
                    if "_source" in raw_df.columns:
                        if (raw_df["_source"] == "stooq").any():
                            stooq_hits += 1
                        raw_df = raw_df.drop(columns=["_source"])

                    # Ensure 'date' is a column (not index)
                    if "date" not in raw_df.columns and hasattr(raw_df.index, "name") and raw_df.index.name == "date":
                        raw_df = raw_df.reset_index()

                    # Clean date column: strip timezone, keep YYYY-MM-DD only
                    if "date" in raw_df.columns:
                        try:
                            raw_df["date"] = pd.to_datetime(
                                raw_df["date"], utc=True
                            ).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
                        except Exception:
                            # Stooq dates may already be plain strings
                            raw_df["date"] = pd.to_datetime(
                                raw_df["date"]
                            ).dt.strftime("%Y-%m-%d")

                    raw_df["symbol"] = sym_fname
                    csv_path = source_dir_new / f"{sym_fname}.csv"
                    raw_df.to_csv(csv_path, index=False)
                    success += 1

                except _RLE as e:
                    logger.error(f"RATE LIMIT DETECTED in Phase 2: {e}")
                    rate_limited = True
                    break

                except Exception as e:
                    consecutive_failures += 1
                    failure_logger.log(
                        yahoo_symbol, "2000-01-01", end_date, "network_error", str(e)
                    )
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.error(
                            f"RATE LIMIT DETECTED: {consecutive_failures} consecutive "
                            f"failures in Phase 2. Stopping new-stock download."
                        )
                        rate_limited = True
                        break
                    continue

            logger.info(
                f"New stock download: {success}/{len(new_symbols)} succeeded "
                f"({stooq_hits} from Stooq, {skipped_csv} from cache)"
            )

            # Normalize + dump whatever we downloaded (even if rate-limited mid-way)
            if list(source_dir_new.glob("*.csv")):
                logger.info("Normalizing new stocks...")
                normalizer = Normalize(
                    source_dir=source_dir_new,
                    target_dir=normalize_dir_new,
                    normalize_class=YahooNormalizeUS1d,
                    max_workers=min(max(multiprocessing.cpu_count() - 2, 1), 4),
                    date_field_name="date",
                    symbol_field_name="symbol",
                )
                normalizer.normalize()

            if list(normalize_dir_new.glob("*.csv")):
                logger.info("Dumping new stocks to bin...")
                _dump = DumpDataUpdate(
                    data_path=str(normalize_dir_new),
                    qlib_dir=qlib_data_1d_dir,
                    exclude_fields="symbol,date",
                    max_workers=min(max(multiprocessing.cpu_count() - 2, 1), 4),
                )
                _dump.dump()

            # Cleanup: only clear source CSVs AFTER successful dump.
            # This ensures they serve as a resume cache if the script
            # is interrupted before dump completes.
            if not rate_limited:
                _clear_csv_dir(source_dir_new)
                _clear_csv_dir(normalize_dir_new)
            else:
                logger.info(
                    "Keeping source CSVs as cache (rate-limited). "
                    "Re-run later to continue downloading remaining new stocks."
                )

            if rate_limited:
                logger.error(
                    "RATE LIMIT: Phase 2 stopped early. Already-downloaded data has been "
                    "saved. Wait 15-30 minutes then re-run with a higher --delay."
                )

        logger.info("Phase 2 complete.")

    except Exception as e:
        logger.error(f"Phase 2 failed: {traceback.format_exc()}")
    finally:
        os.chdir(original_cwd)

    # ========================================
    # Final reconciliation
    # ========================================
    logger.info("=" * 60)
    logger.info("Final reconciliation of instruments/all.txt from bin data...")
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

    # Save failure log
    failure_logger.save()

    logger.info("=" * 60)
    logger.info("Full market update complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    fire.Fire(update_qlib_data)
