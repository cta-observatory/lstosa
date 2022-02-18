"""Utility functions for dealing with the copy of datacheck files to webserver."""

import logging
import subprocess as sp
from pathlib import Path
from typing import List

from osa.configs.config import cfg
from osa.paths import DATACHECK_WEB_BASEDIR
from osa.utils.logging import myLogger

__all__ = ["directory_in_webserver", "copy_to_webserver", "set_no_observations_flag"]

log = myLogger(logging.getLogger(__name__))


def directory_in_webserver(
        host: str,
        datacheck_type: str,
        date: str,
        prod_id: str
) -> Path:
    """
    Create directories in the datacheck web server.

    Parameters
    ----------
    host : str
        Hostname of the server to which the datacheck products will be copied
    datacheck_type : str
        Type of datacheck product (PEDESTAL, CALIB, DL1, LONGTERM)
    date : str
        Date in the format YYYYMMDD
    prod_id : str
        Production ID

    Returns
    -------
    Path
        Path to the directory in the web server
    """
    DATACHECK_WEB_DIRS = {
        "PEDESTAL": f"drs4/{prod_id}/{date}",
        "CALIB": f"enf_calibration/{prod_id}/{date}",
        "DL1": f"dl1/{prod_id}/{date}/pdf",
        "LONGTERM": f"dl1/{prod_id}/{date}",
        "HIGH_LEVEL": f"high_level/{prod_id}/{date}",
    }

    destination_dir = DATACHECK_WEB_BASEDIR / DATACHECK_WEB_DIRS[datacheck_type]

    remote_mkdir = ["ssh", host, "mkdir", "-p", destination_dir]
    sp.run(remote_mkdir, check=True)

    return destination_dir


def copy_to_webserver(
        files: List[Path],
        datacheck_type: str,
        date: str,
        prod_id: str
) -> None:
    """
    Copy files to the webserver in host.

    Parameters
    ----------
    files : List[Path]
        List of files to be copied
    datacheck_type : str
        Type of datacheck product
    date : str
        Date in the format YYYYMMDD
    prod_id : str
        Production ID
    """
    host = cfg.get("WEBSERVER", "HOST")
    destination_dir = directory_in_webserver(host, datacheck_type, date, prod_id)

    for file in files:
        log.info(f"Copying {file}")
        copy_file = ["scp", file, f"{host}:{destination_dir}/."]
        sp.run(copy_file, check=True)


def set_no_observations_flag(host, datedir, prod_id):
    """
    Create a file in the webserver indicating that there are no observations
    on a given date.

    Parameters
    ----------
    host : str
        Hostname of the server to which the datacheck products will be copied
    datedir : str
        Date in the format YYYYMMDD
    prod_id : str
        Production ID
    """
    for product in ["PEDESTAL", "CALIB", "LONGTERM"]:
        dest_directory = directory_in_webserver(host, product, datedir, prod_id)
        no_observations_flag = dest_directory / "no_observations"
        cmd = ["ssh", host, "touch", no_observations_flag]
        sp.run(cmd, check=True)
