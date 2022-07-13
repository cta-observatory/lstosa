"""Handle the creation and reading of the run summary files."""

import logging
import subprocess
from pathlib import Path

from astropy.table import Table

from osa.configs.config import cfg
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir, stringify

__all__ = ["produce_run_summary_file", "get_run_summary_file", "run_summary_table"]


log = myLogger(logging.getLogger(__name__))


def produce_run_summary_file(date) -> None:
    """
    Produce the run summary using the lstchain script.

    Parameters
    ----------
    date : datetime.datetime
    """
    nightdir = date_to_dir(date)
    r0_dir = Path(cfg.get("LST1", "R0_DIR"))
    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))

    command = [
        "lstchain_create_run_summary",
        f"--date={nightdir}",
        f"--r0-path={r0_dir}",
        f"--output-dir={run_summary_dir}",
    ]

    try:
        subprocess.run(command, check=True)

    except OSError as error:
        log.exception(f"Command {stringify(command)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")


def run_summary_table(date) -> Table:
    """
    Reads the run summary ECSV file containing as a Table with the following content:
     - run_id, datatype: int64
     - n_subruns, datatype: int64
     - run_type, datatype: string
     - ucts_timestamp, datatype: int64
     - run_start, datatype: int64
     - dragon_reference_time, datatype: int64
     - dragon_reference_module_id, datatype: int16
     - dragon_reference_module_index, datatype: int16
     - dragon_reference_counter, datatype: uint64
     - dragon_reference_source, datatype: string

    Parameters
    ----------
    date : datetime.datetime

    Returns
    -------
    table : astropy.Table
        Table with the content of the run summary ECSV file.
    """
    night_summary_file = get_run_summary_file(date)
    log.debug(f"Looking for run summary file {night_summary_file}")

    if not night_summary_file.exists():
        log.info(f"Run summary file {night_summary_file} not found. Producing it.")
        produce_run_summary_file(date)

    table = Table.read(night_summary_file)
    table.add_index(["run_id"])
    return table


def get_run_summary_file(date) -> Path:
    """
    Builds the file name of the run summary ECSV file.

    Parameters
    ----------
    date : datetime.datetime

    Returns
    -------
    run_summary_file : pathlib.Path
        File name of the run summary ECSV file
    """
    nightdir = date_to_dir(date)
    return Path(cfg.get("LST1", "RUN_SUMMARY_DIR")) / f"RunSummary_{nightdir}.ecsv"
