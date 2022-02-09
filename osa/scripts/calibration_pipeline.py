#!/usr/bin/env python

"""
Calibration pipeline

Script to process the pedestal and calibration runs to produce the
DRS4 pedestal and charge calibration files. It pipes together the two
onsite calibration scripts from lstchain.
"""

import logging
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.job import historylevel
from osa.job import run_program_with_history_logging
from osa.provenance.capture import trace
from osa.utils.cliopts import calibration_pipeline_cliparsing
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_dir

__all__ = [
    "calibration_sequence",
    "calibrate_charge",
    "drs4_pedestal",
    "drs4_pedestal_command",
    "calibration_file_command",
]

log = myLogger(logging.getLogger())


def is_calibration_produced(drs4_pedestal_run_id: str, pedcal_run_id: str) -> bool:
    """
    Check if both daily calibration (DRS4 baseline and
    charge calibration) files are already produced.
    """
    return (
        drs4_pedestal_exists(drs4_pedestal_run_id)
        and calibration_file_exists(pedcal_run_id)
    )


def drs4_pedestal_exists(drs4_pedestal_run_id: str) -> bool:
    """Return true if drs4 pedestal file was already produced."""
    pedestal_dir = Path(cfg.get(options.tel_id, "PEDESTAL_DIR"))
    date_obs = lstdate_to_dir(options.date)

    drs4_pedestal_file = (
        pedestal_dir / date_obs / options.calib_prod_id /
        f"drs4_pedestal.Run{drs4_pedestal_run_id}.0000.h5"
    )

    return drs4_pedestal_file.exists()


def calibration_file_exists(pedcal_run_id: str) -> bool:
    """Return true if calibration file was already produced."""
    calib_dir = Path(cfg.get(options.tel_id, "CALIB_DIR"))
    date_obs = lstdate_to_dir(options.date)

    pedcal_file = (
        calib_dir / date_obs / options.calib_prod_id /
        f"calibration_filters_52.Run{pedcal_run_id}.0000.h5"
    )

    return pedcal_file.exists()


def drs4_pedestal_command(drs4_pedestal_run_id: str) -> list:
    """Build the create_drs4_pedestal command."""
    base_dir = Path(cfg.get("LST1", "BASE")).resolve()
    return [
        "onsite_create_drs4_pedestal_file",
        f"--run_number={drs4_pedestal_run_id}",
        f"--base_dir={base_dir}",
        "--no-progress",
    ]


def calibration_file_command(pedestal_run_id: str, pedcal_run_id: str) -> list:
    """Build the create_calibration_file command."""
    base_dir = Path(cfg.get("LST1", "BASE")).resolve()
    return [
        "onsite_create_calibration_file",
        f"--pedestal_run={pedestal_run_id}",
        f"--run_number={pedcal_run_id}",
        f"--base_dir={base_dir}",
        "--filters=52",
    ]


def calibration_sequence(drs4_pedestal_run_id: str, pedcal_run_id: str) -> int:
    """
    Handle the two stages for creating the daily calibration products:
    DRS4 pedestal and charge calibration files.

    Parameters
    ----------
    drs4_pedestal_run_id : str
    pedcal_run_id : str

    Returns
    -------
    rc : int
        Return code
    """
    analysis_dir = Path(options.directory)
    history_file = analysis_dir / f"sequence_LST1_{pedcal_run_id}.history"

    level, rc = (2, 0) if options.simulate else historylevel(history_file, "PEDCALIB")

    log.info(f"Going to level {level}")

    if level == 2:
        rc = drs4_pedestal(drs4_pedestal_run_id, pedcal_run_id, history_file)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 1:
        rc = calibrate_charge(drs4_pedestal_run_id, pedcal_run_id, history_file)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 0:
        log.info(f"Job for sequence {pedcal_run_id} finished without fatal errors")

    return rc


@trace
def drs4_pedestal(
    drs4_pedestal_run_id: str, pedcal_run_id: str, history_file: Path
) -> int:
    """
    Create a DRS4 pedestal file for baseline correction.

    Parameters
    ----------
    drs4_pedestal_run_id : str
        String with run number of the pedestal run
    pedcal_run_id : str
        String with run number of the pedcal run
    history_file : `pathlib.Path`
        Path to the history file

    Returns
    -------
    rc : int
        Return code
    """
    if options.simulate or drs4_pedestal_exists(drs4_pedestal_run_id):
        return 0

    cmd = drs4_pedestal_command(drs4_pedestal_run_id)

    return run_program_with_history_logging(
        command_args=cmd,
        history_file=history_file,
        run=drs4_pedestal_run_id,
        prod_id=options.calib_prod_id,
        command=cmd[0],
    )


@trace
def calibrate_charge(
    drs4_pedestal_run_id: str, pedcal_run_id: str, history_file: Path
) -> int:
    """
    Create the calibration file to transform from ADC counts to photo-electrons

    Parameters
    ----------
    drs4_pedestal_run_id : str
        String with run number of the pedestal run
    pedcal_run_id : str
        String with run number of the pedcal run
    history_file : `pathlib.Path`
        Path to the history file

    Returns
    -------
    rc: int
        Return code
    """
    if options.simulate or calibration_file_exists(pedcal_run_id):
        return 0

    cmd = calibration_file_command(
        pedestal_run_id=drs4_pedestal_run_id,
        pedcal_run_id=pedcal_run_id
    )

    return run_program_with_history_logging(
        command_args=cmd,
        history_file=history_file,
        run=pedcal_run_id,
        prod_id=options.calib_prod_id,
        command=cmd[0],
    )


def main():
    """
    Performs the calibration steps (obtain the drs4 baseline correction
    and ADC to photo-electron coefficients)
    """
    drs4_pedestal_run, pedcal_run = calibration_pipeline_cliparsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    if is_calibration_produced(
        drs4_pedestal_run_id=drs4_pedestal_run,
        pedcal_run_id=pedcal_run
    ):
        log.info(
            f"Calibration already produced, runs {drs4_pedestal_run} and {pedcal_run}"
        )
        sys.exit(0)

    rc = calibration_sequence(drs4_pedestal_run, pedcal_run)
    sys.exit(rc)


if __name__ == "__main__":
    main()
