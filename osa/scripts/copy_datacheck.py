#!/usr/bin/env python
"""
Script to copy analysis products to datacheck webserver creating new
directories whenever they are needed.
"""

import itertools
import logging
import subprocess
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import copy_datacheck_parsing
from osa.utils.logging import MyFormatter
from osa.utils.utils import lstdate_to_dir, is_day_closed


__all__ = [
    "copy_files",
    "create_destination_dir",
    "set_no_observations_flag"
]

log = logging.getLogger(__name__)

# Logging
fmt = MyFormatter()
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logging.root.addHandler(handler)
log.setLevel(logging.INFO)


analysis_products = ["drs4", "enf_calibration", "dl1"]
datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))


def main():
    """
    Get analysis products to be copied to the webserver,
    create directories and eventually copy the files.
    """
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
    create_destination_dir(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
    if not files_to_transfer and is_day_closed():
        log.warning("No observations, hence no files to be copied")
        set_no_observations_flag(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
    else:
        log.debug("Transferring files")
        copy_files(cfg.get("WEBSERVER", "HOST"), nightdir, files_to_transfer)


def create_destination_dir(host, datedir, prod_id):
    """Create destination directories for drs4, enf_calibration
    and dl1 products in the data-check webserver via ssh. It also copies
    the index.php file needed to build the directory tree structure.

    Parameters
    ----------
    host
    datedir
    prod_id
    """

    # Create directory and copy the index.php to each directory
    for product in analysis_products:
        destination_dir = datacheck_basedir / product / prod_id / datedir
        cmd = ["ssh", host, "mkdir", "-p", destination_dir]
        subprocess.run(cmd)
        cmd = ["scp", cfg.get("WEBSERVER", "INDEXPHP"), f"{host}:{destination_dir}/."]
        subprocess.run(cmd)


def set_no_observations_flag(host, datedir, prod_id):
    """Create a flag file indicating that are no
    observations on a given date.

    Parameters
    ----------
    host
    datedir
    prod_id
    """
    for product in analysis_products:
        destination_dir = datacheck_basedir / product / prod_id / datedir
        no_observations_flag = destination_dir / "no_observations"
        cmd = ["ssh", host, "touch", no_observations_flag]
        subprocess.run(cmd)


def copy_files(host, datedir, file_list):
    """

    Parameters
    ----------
    host
    datedir
    files
    """
    # FIXME: Check if files exists already at webserver CHECK HASH
    datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))
    # Copy files to server
    for file_to_transfer in file_list:
        if "drs4" in str(file_to_transfer):
            destination_dir = datacheck_basedir / "drs4" / options.prod_id / datedir
            cmd = ["scp", str(file_to_transfer), f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "calibration" in str(file_to_transfer):
            destination_dir = datacheck_basedir / "enf_calibration" / options.prod_id / datedir
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "datacheck" in str(file_to_transfer):
            destination_dir = datacheck_basedir / "dl1" / options.prod_id / datedir
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)


if __name__ == "__main__":
    main()
