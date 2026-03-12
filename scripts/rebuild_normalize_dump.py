"""One-time script: re-normalize ALL source CSVs with YahooNormalizeUS1d
(split-adjusted, no first-close normalization) and rebuild the entire
qlib binary dataset from scratch using DumpDataAll.

Usage (from data_collector/all_source/):
    python ../../rebuild_normalize_dump.py --qlib_data_dir qlib_data/us_data

This is needed because the previous YahooNormalizeUS1dExtend was missing
the split-adjusted normalize override, producing wrong values (prices
divided by first-day close ≈ 1.0 scale instead of real Yahoo OHLCV).
"""

import os
import sys
import shutil
import multiprocessing
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ALL_SOURCE_DIR = SCRIPT_DIR / "data_collector" / "all_source"
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ALL_SOURCE_DIR))


def dedup_source_csvs(source_dir: Path):
    """Remove duplicate rows (by date) from each source CSV."""
    csv_files = sorted(source_dir.glob("*.csv"))
    fixed = 0
    for f in tqdm(csv_files, desc="Dedup source CSVs", unit="file"):
        try:
            df = pd.read_csv(f, dtype={"symbol": str})
            if "date" not in df.columns:
                continue
            before = len(df)
            df["_dt"] = pd.to_datetime(df["date"], format="mixed", utc=True).dt.tz_localize(None)
            df = df.drop_duplicates(subset=["_dt"], keep="last")
            df = df.sort_values("_dt").reset_index(drop=True)
            df["date"] = df["_dt"].dt.strftime("%Y-%m-%d")
            df = df.drop(columns=["_dt"])
            if len(df) < before:
                df.to_csv(f, index=False)
                fixed += 1
        except Exception as e:
            logger.warning(f"Failed to dedup {f.name}: {e}")
    logger.info(f"Dedup complete: {fixed} files had duplicates removed")


def full_normalize(source_dir: Path, normalize_dir: Path, max_workers: int):
    """Normalize ALL source CSVs using YahooNormalizeUS1d (non-Extend)."""
    from collector import YahooNormalizeUS1d
    from data_collector.base import Normalize

    if normalize_dir.exists():
        for f in normalize_dir.glob("*.csv"):
            f.unlink()
    normalize_dir.mkdir(parents=True, exist_ok=True)

    normalizer = Normalize(
        source_dir=source_dir,
        target_dir=normalize_dir,
        normalize_class=YahooNormalizeUS1d,
        max_workers=max_workers,
        date_field_name="date",
        symbol_field_name="symbol",
    )
    normalizer.normalize()


def full_dump(normalize_dir: Path, qlib_data_dir: str, max_workers: int):
    """Rebuild entire qlib binary dataset from normalize CSVs."""
    from dump_bin import DumpDataAll

    dumper = DumpDataAll(
        data_path=str(normalize_dir),
        qlib_dir=qlib_data_dir,
        freq="day",
        max_workers=max_workers,
        date_field_name="date",
        symbol_field_name="symbol",
        exclude_fields="symbol,date",
    )
    dumper.dump()


def main(qlib_data_dir: str = "qlib_data/us_data"):
    qlib_data_dir = str(Path(qlib_data_dir).expanduser().resolve())
    source_dir = ALL_SOURCE_DIR / "source"
    normalize_dir = ALL_SOURCE_DIR / "normalize"
    workers = min(max(multiprocessing.cpu_count() - 2, 1), 8)

    logger.info("=" * 60)
    logger.info("REBUILD: full re-normalize + re-dump")
    logger.info(f"  source_dir:    {source_dir}")
    logger.info(f"  normalize_dir: {normalize_dir}")
    logger.info(f"  qlib_data_dir: {qlib_data_dir}")
    logger.info(f"  workers:       {workers}")
    logger.info("=" * 60)

    logger.info("Step 1/3: Dedup source CSVs")
    dedup_source_csvs(source_dir)

    logger.info("Step 2/3: Full normalize (YahooNormalizeUS1d)")
    full_normalize(source_dir, normalize_dir, workers)

    logger.info("Step 3/3: Full dump (DumpDataAll)")
    full_dump(normalize_dir, qlib_data_dir, workers)

    logger.info("=" * 60)
    logger.info("REBUILD COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    import fire
    fire.Fire(main)
