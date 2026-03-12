# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""
从远程数据源（Yahoo -> Stooq -> Nasdaq Data Link）拉取 NVDA 指定日期的股价，不读本地 qlib 数据。
与 update_qlib_data.py 使用相同的多源拉取逻辑。

Usage (run from quant_finance/qlib):
    python scripts/fetch_nvda_remote_dates.py
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import update_qlib_data

SYMBOL_YAHOO = "NVDA"
SYMBOL_FNAME = "nvda"
DATES = [
    ("2020-11-02", "2020-11-03"),
    ("2026-02-09", "2026-02-10"),
]


def main():
    print("Fetching NVDA from remote (Yahoo -> Stooq -> Nasdaq Data Link), not local qlib.\n")
    for start, end in DATES:
        print(f"--- {start} ---")
        try:
            df = update_qlib_data._fetch_stock_data_multi_source(
                symbol_yahoo=SYMBOL_YAHOO,
                symbol_fname=SYMBOL_FNAME,
                start=start,
                end=end,
            )
            if df is None or df.empty:
                print(f"  No data from any source for {start}\n")
                continue
            src = df.get("_source", "?")
            if hasattr(src, "iloc"):
                src = src.iloc[0] if len(src) else "?"
            print(f"  Source: {src}")
            print(df.to_string(index=False))
        except Exception as e:
            print(f"  Error: {e}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
