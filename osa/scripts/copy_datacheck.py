#!/usr/bin/env python
"""
Script to copy analysis products to datacheck webserver creating new
directories whenever they are needed.
"""

import logging
import subprocess as sp
from pathlib import Path
from typing import List

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import copy_datacheck_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import (
    lstdate_to_dir,
    is_day_closed,
    set_no_observations_flag,
)


log = myLogger(logging.getLogger())


DATACHECK_FILE_PATTERNS = {
    "PEDESTAL": "drs4*.pdf",
    "CALIB": "calibration*.pdf",
    "DL1": "datacheck_dl1*.pdf",
    "LONGTERM": "DL1_datacheck_*.*"
}


def get_datacheck_files(pattern: str, directory: Path) -> list:
    """Return a list of files matching the pattern."""
    return [file for file in directory.glob(pattern)]


def create_directory_in_datacheck_webserver(host: str, datacheck_type: str) -> Path:
    """Create directories in the datacheck web server and setup index.php file."""
    date = lstdate_to_dir(options.date)
    datacheck_dir = Path(cfg.get("WEBSERVER", "DATACHECK"))

    DATACHECK_WEB_DIRS = {
        "PEDESTAL": f"drs4/{options.prod_id}/{date}",
        "CALIB": f"enf_calibration/{options.prod_id}/{date}",
        "DL1": f"dl1/{options.prod_id}/{date}/pdf",
        "LONGTERM": f"dl1/{options.prod_id}/{date}"
    }

    destination_dir = datacheck_dir / DATACHECK_WEB_DIRS[datacheck_type]

    index_php = cfg.get("WEBSERVER", "INDEX_PHP")
    remote_mkdir = ["ssh", host, "mkdir", "-p", destination_dir]
    copy_index_php = ["scp", index_php, f"{host}:{destination_dir}/."]
    sp.run(remote_mkdir, check=True)
    sp.run(copy_index_php, check=True)

    return destination_dir


def copy_to_webserver(files: List[Path], datacheck_type: str):
    """Copy files to the webserver in host."""
    host = cfg.get("WEBSERVER", "HOST")
    destination_dir = create_directory_in_datacheck_webserver(host, datacheck_type)

    for file in files:
        log.info(f"Copying {file}")
        copy_file = ["scp", file, f"{host}:{destination_dir}/."]
        sp.run(copy_file, check=True)


def datacheck_directory(data_type: str, date: str) -> Path:
    if data_type in {"PEDESTAL", "CALIB"}:
        directory = Path(cfg.get("LST1", f"{data_type}_DIR")) / date / "pro/log"
    elif data_type == "DL1":
        directory = (
            Path(cfg.get("LST1", f"{data_type}_DIR"))
            / date
            / options.prod_id
            / options.dl1_prod_id
        )
    elif data_type == "LONGTERM":
        directory = Path(cfg.get("LST1", f"{data_type}_DIR")) / options.prod_id / date
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    return directory


def main():
    """Copy datacheck products to the webserver."""
    log.setLevel(logging.INFO)

    copy_datacheck_parsing()
    nightdir = lstdate_to_dir(options.date)

    lists_datacheck_files = []

    for data, pattern in DATACHECK_FILE_PATTERNS.items():
        log.info(f"Looking for {pattern}")
        directory = datacheck_directory(data_type=data, date=nightdir)
        files = get_datacheck_files(pattern, directory)
        copy_to_webserver(files, data)
        lists_datacheck_files.append(files)

    # Flatten the list of lists
    datacheck_files = [item for sublist in lists_datacheck_files for item in sublist]

    print(datacheck_files)

    if not datacheck_files and is_day_closed():
        log.warning("No observations. No files to be copied")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)


if __name__ == "__main__":
    main()
