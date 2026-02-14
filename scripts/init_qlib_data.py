# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# MODIFIED: New script — initialize qlib data from offline package

"""
Initialize Qlib data directory by downloading the offline data package.

Usage (run from quant_finance/qlib/scripts/):
    python init_qlib_data.py --target_dir ~/.qlib/qlib_data/us_data --region us

Notes:
    - US offline data is up to ~2020-11-10
    - First run will delete existing data in target_dir
"""

import fire
from pathlib import Path
from loguru import logger
from qlib.tests.data import GetData


def init_qlib_data(
    target_dir: str = "~/.qlib/qlib_data/us_data",
    region: str = "us",
    interval: str = "1d",
    delete_old: bool = True,
    exists_skip: bool = False,
):
    """Download and initialize qlib data from offline package.

    Parameters
    ----------
    target_dir : str
        Directory to store qlib data, default ~/.qlib/qlib_data/us_data
    region : str
        Market region, value from [cn, us], default us
    interval : str
        Data frequency, default 1d
    delete_old : bool
        Whether to delete existing data, default True
    exists_skip : bool
        Skip if data already exists, default False
    """
    target_dir = str(Path(target_dir).expanduser().resolve())
    logger.info(f"Initializing qlib data: region={region}, interval={interval}")
    logger.info(f"Target directory: {target_dir}")

    GetData().qlib_data(
        target_dir=target_dir,
        region=region,
        interval=interval,
        delete_old=delete_old,
        exists_skip=exists_skip,
    )

    logger.info(f"Initialization complete. Data saved to: {target_dir}")


if __name__ == "__main__":
    fire.Fire(init_qlib_data)
