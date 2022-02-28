#!/usr/bin/env python
"""
Script to copy analysis products to datacheck webserver creating new
directories whenever they are needed.
"""

import logging

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import (
    datacheck_directory,
    get_datacheck_files, destination_dir,
)
from osa.utils.cliopts import copy_datacheck_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import DATACHECK_FILE_PATTERNS, lstdate_to_dir, is_day_closed
from osa.webserver.utils import copy_to_webserver, set_no_observations_flag

log = myLogger(logging.getLogger())


def are_files_copied(pattern: str, files: list) -> bool:
    """Check if all files of a given data type are copied."""
    n_files = len(files)

    if n_files == 0:
        log.warning(f"No {pattern} files found.")
        return False

    elif pattern == "PEDESTAL" and n_files != 1:
        log.warning(f"Expected at least 1 PDF file, {n_files} found.")
        return False

    elif pattern == "CALIB" and n_files != 1:
        log.warning(f"Expected at least 1 PDF file, {n_files} found.")
        return False

    elif pattern == "DL1" and n_files != get_number_of_runs():
        log.warning(f"Expected {get_number_of_runs()} PDF files, {n_files} found.")
        return False

    elif pattern == "LONGTERM" and n_files != 3:
        log.warning(f"Expected 3 DL1 check files (HTML, h5 and log), {n_files} found.")
        return False


def main():
    """Copy datacheck products to the webserver."""
    log.setLevel(logging.INFO)

    log.info(
        "Expected PDF datacheck files: DRS4, ENF calibration & run-wise DL1.\n"
        "Additionally, daily DL1 log, HTML and h5 files should be copied.\n"
        "If any of these are missing it might happen that they are not produced yet.\n"
    )

    copy_datacheck_parsing()
    nightdir = lstdate_to_dir(options.date)

    lists_datacheck_files = []

    all_files_are_copied = False

    for data_type, pattern in DATACHECK_FILE_PATTERNS.items():
        log.info(f"Looking for {pattern}")
        directory = datacheck_directory(data_type=data_type, date=nightdir)
        files = get_datacheck_files(pattern, directory)
        lists_datacheck_files.append(files)
        copy_to_webserver(files, data_type, nightdir, options.prod_id)

        # Check if all files are copied
        all_files_are_copied = are_files_copied(pattern, files)

    # Flatten the list of lists for easy check of no_observations flag
    datacheck_files = [item for sublist in lists_datacheck_files for item in sublist]

    if not datacheck_files and is_day_closed():
        log.warning("No observations. Setting no_observations flag.")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)

    if all_files_are_copied:
        log.info("All datacheck files copied. No more files are expected.")
    else:
        log.warning("Not all datacheck files were copied. Check for problems.")


def get_number_of_runs():
    """
    Get the run sequence processed list for the given date by globbing the
    run-wise DL1 files.
    """
    dl1_directory = destination_dir("DL1", create_dir=False)
    list_files = list(dl1_directory.glob("dl1_LST-1.Run?????.h5"))
    return len(list_files)


if __name__ == "__main__":
    main()
