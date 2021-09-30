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
from osa.utils.utils import lstdate_to_dir

__all__ = ["copy_files", "create_destination_dir", "is_merge_process_finished"]

log = logging.getLogger(__name__)

# Logging
fmt = MyFormatter()
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logging.root.addHandler(handler)
log.setLevel(logging.INFO)


def main():
    """
    Get analysis products to be copied to the webserver,
    create directories and eventually copy the files.
    """
    # Set cli options and arguments
    copy_datacheck_parsing()

    # Check of merging process has already finished
    if is_merge_process_finished():
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
        if not files_to_transfer:
            log.warning("No files to be copied")
        else:
            log.debug("Transferring files")
            copy_files(cfg.get("WEBSERVER", "HOST"), nightdir, files_to_transfer)
    else:
        log.warning("Files still not produced")


def create_destination_dir(host, datedir, prod_id):
    """

    Parameters
    ----------
    host
    datedir
    prod_id
    """
    datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))

    # Create directory and copy the index.php to each directory
    for product in ["drs4", "enf_calibration", "dl1"]:
        destination_dir = datacheck_basedir / product / prod_id / datedir
        cmd = ["ssh", host, "mkdir", "-p", destination_dir]
        subprocess.run(cmd)
        cmd = ["scp", cfg.get("WEBSERVER", "INDEXPHP"), f"{host}:{destination_dir}/."]
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
    # Copy PDF files
    datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))
    # Scopy files
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


def is_merge_process_finished():
    # TODO: still not implemented
    #  check that no lstchain_check_dl1 is still running and pdf files already exist
    return True


if __name__ == "__main__":
    main()
