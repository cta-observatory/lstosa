#!/usr/bin/env python
"""
Script to copy analysis products to datacheck webserver creating new
directories whenever they are needed.
"""

import itertools
import logging
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import copy_datacheck_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import (
    lstdate_to_dir,
    is_day_closed,
    create_directories_datacheck_web,
    set_no_observations_flag,
    copy_files_datacheck_web,
)

log = myLogger(logging.getLogger())

__all__ = ["look_for_datacheck_files"]


def look_for_datacheck_files(nightdir):
    """Look for the datacheck files."""
    drs4_baseline_dir = (
        Path(cfg.get("LST1", "PEDESTAL_DIR")) / nightdir / "pro"
    ).resolve()
    calibration_dir = (
        Path(cfg.get("LST1", "CALIB_DIR")) / nightdir / "pro"
    ).resolve()
    dl1_dir = (
        Path(cfg.get("LST1", "DL1_DIR"))
        / nightdir
        / options.prod_id
        / options.dl1_prod_id
    ).resolve()
    dl1_longterm_daily = (
        Path(cfg.get("LST1", "LONGTERM_DIR")) / options.prod_id / nightdir
    ).resolve()

    drs4_pdf = list(drs4_baseline_dir.rglob("drs4*.pdf"))
    calib_pdf = list(calibration_dir.rglob("calibration*.pdf"))
    dl1_pdf = list(dl1_dir.rglob("datacheck*.pdf"))
    dl1_longterm_daily = list(dl1_longterm_daily.rglob("DL1_datacheck*"))
    list_of_files = [drs4_pdf, calib_pdf, dl1_pdf, dl1_longterm_daily]

    return list(itertools.chain(*list_of_files))


def main():
    """
    Get analysis products to be copied to the webserver,
    create directories and eventually copy the files.
    """
    log.setLevel(logging.INFO)

    copy_datacheck_parsing()
    nightdir = lstdate_to_dir(options.date)

    # Look for the datacheck files
    files_to_transfer = look_for_datacheck_files(nightdir)

    log.debug("Creating directories")
    create_directories_datacheck_web(
        cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id
    )
    if not files_to_transfer and is_day_closed():
        log.warning("No observations. No files to be copied")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
    else:
        log.debug("Transferring files")
        copy_files_datacheck_web(
            cfg.get("WEBSERVER", "HOST"), nightdir, files_to_transfer
        )


if __name__ == "__main__":
    main()
