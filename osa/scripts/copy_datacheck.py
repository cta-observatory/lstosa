#!/usr/bin/env python
"""
Script to copy analysis products to datacheck webserver creating new
directories whenever they are needed.
"""

import logging

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import copy_datacheck_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import (
    lstdate_to_dir,
    is_day_closed,
    datacheck_directory,
    get_datacheck_files,
)
from osa.webserver.utils import copy_to_webserver, set_no_observations_flag
from osa.utils.utils import DATACHECK_FILE_PATTERNS


log = myLogger(logging.getLogger())


def main():
    """Copy datacheck products to the webserver."""
    log.setLevel(logging.INFO)

    log.info(
        "Expected PDF datacheck files: DRS4, ENF calibration & run-wise DL1.\n"
        "Additionally daily DL1 log, HTML and h5 files should be copied.\n"
        "If any of these are missing it might happen that they are not produced yet."
    )

    copy_datacheck_parsing()
    nightdir = lstdate_to_dir(options.date)

    lists_datacheck_files = []

    for data_type, pattern in DATACHECK_FILE_PATTERNS.items():
        log.info(f"Looking for {pattern}")
        directory = datacheck_directory(data_type=data_type, date=nightdir)
        files = get_datacheck_files(pattern, directory)
        lists_datacheck_files.append(files)
        copy_to_webserver(files, data_type, nightdir, options.prod_id)

    # Flatten the list of lists for easy check of no_observations flag
    datacheck_files = [item for sublist in lists_datacheck_files for item in sublist]

    if not datacheck_files and is_day_closed():
        log.warning("No observations. Setting no_observations flag.")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)


if __name__ == "__main__":
    main()
