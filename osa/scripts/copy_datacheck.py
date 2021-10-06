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
from osa.utils.utils import (
    lstdate_to_dir,
    is_day_closed,
    create_directories_datacheck_web,
    set_no_observations_flag,
    copy_files_datacheck_web,
)

log = myLogger(logging.getLogger(__name__))


def main():
    """
    Get analysis products to be copied to the webserver,
    create directories and eventually copy the files.
    """
    log.setLevel(logging.INFO)

    # Set cli options and arguments
    copy_datacheck_parsing()

    # Check if files exists in local disk
    nightdir = lstdate_to_dir(options.date)
    analysis_log_dir = Path(cfg.get("LST1", "ANALYSISDIR")) / nightdir / options.prod_id / "log"
    dl1_dir = Path(cfg.get("LST1", "DL1DIR")) / nightdir / options.prod_id / options.dl1_prod_id
    dl1_longterm_daily = Path("/fefs/aswg/data/real/OSA/DL1DataCheck_LongTerm") / "v0.7" / nightdir

    drs4_pdf = [file for file in analysis_log_dir.glob("drs4*.pdf")]
    calib_pdf = [file for file in analysis_log_dir.glob("calibration*.pdf")]
    dl1_pdf = [file for file in dl1_dir.glob("*datacheck*.pdf")]
    dl1_longterm_daily = [file for file in dl1_longterm_daily.glob("*datacheck*")]
    list_of_files = [drs4_pdf, calib_pdf, dl1_pdf, dl1_longterm_daily]
    files_to_transfer = list(itertools.chain(*list_of_files))

    log.debug("Creating directories")
    create_directories_datacheck_web(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
    if not files_to_transfer and is_day_closed():
        log.warning("No observations. No files to be copied")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
    else:
        log.debug("Transferring files")
        copy_files_datacheck_web(cfg.get("WEBSERVER", "HOST"), nightdir, files_to_transfer)


if __name__ == "__main__":
    main()
