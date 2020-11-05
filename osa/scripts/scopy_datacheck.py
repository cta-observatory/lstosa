#!/usr/bin/env python

import argparse
import itertools
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import scopy_datacheck_parsing
from osa.utils.standardhandle import error, gettag, output, verbose, warning
from osa.utils.utils import get_prod_id, lstdate_to_dir


def copy_to_webserver():
    tag = gettag()

    # Check of merging process has already finished
    if is_merge_process_finished():
        # Check if files exists in local disk
        nightdir = lstdate_to_dir(options.date)
        analysis_log_dir = (
            Path(cfg.get("LST1", "ANALYSISDIR")) / nightdir / options.prod_id / "log"
        )
        dl1_dir = Path(cfg.get("LST1", "DL1DIR")) / nightdir / options.prod_id

        drs4_pdf = [file for file in analysis_log_dir.glob("drs4*.pdf")]
        calib_pdf = [file for file in analysis_log_dir.glob("calibration*.pdf")]
        dl1_pdf = [file for file in dl1_dir.glob("datacheck*.pdf")]
        list_of_files = [drs4_pdf, calib_pdf, dl1_pdf]
        files_to_transfer = list(itertools.chain(*list_of_files))

        verbose(tag, "Creating directories")
        create_destination_dir(cfg.get("WEBSERVER", "HOST"), nightdir, options.prod_id)
        if not files_to_transfer:
            warning(tag, "No files to be copied")
        else:
            verbose(tag, "Transferring files")
            scopy_files(cfg.get("WEBSERVER", "HOST"), nightdir, files_to_transfer)
    else:
        warning(tag, "Files still not produced")


def create_destination_dir(host, datedir, prod_id):
    datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))

    # Create directory and copy the index.php to each directory
    for product in ["drs4", "enf_calibration", "dl1"]:
        destination_dir = datacheck_basedir / product / prod_id / datedir
        cmd = ["ssh", host, "mkdir", "-p", destination_dir]
        subprocess.run(cmd)
        cmd = ["scp", cfg.get("WEBSERVER", "INDEXPHP"), f"{host}:{destination_dir}/."]
        subprocess.run(cmd)


def scopy_files(host, datedir, files):
    # FIXME: Check if files exists already at webserver CHECK HASH
    # Copy PDF files
    datacheck_basedir = Path(cfg.get("WEBSERVER", "DATACHECK"))
    # Scopy files
    for pdf_file in files:
        if "drs4" in str(pdf_file):
            destination_dir = datacheck_basedir / "drs4" / options.prod_id / datedir
            cmd = ["scp", str(pdf_file), f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "calibration" in str(pdf_file):
            destination_dir = (
                datacheck_basedir / "enf_calibration" / options.prod_id / datedir
            )
            cmd = ["scp", pdf_file, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "datacheck_dl1" in str(pdf_file):
            destination_dir = datacheck_basedir / "dl1" / options.prod_id / datedir
            cmd = ["scp", pdf_file, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)


def is_merge_process_finished():
    # TODO:check that no lstchain_check_dl1 is still running
    return True


if __name__ == "__main__":
    tag = gettag()
    scopy_datacheck_parsing()

    copy_to_webserver()
