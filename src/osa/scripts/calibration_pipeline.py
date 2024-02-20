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
from osa.paths import drs4_pedestal_exists, calibration_file_exists
from osa.provenance.capture import trace
from osa.utils.cliopts import calibration_pipeline_cliparsing
from osa.utils.logging import myLogger
from osa.workflow.stages import DRS4PedestalStage, ChargeCalibrationStage

__all__ = [
    "calibration_sequence",
    "calibrate_charge",
    "drs4_pedestal",
    "drs4_pedestal_command",
    "calibration_file_command",
]

log = myLogger(logging.getLogger())


def is_calibration_produced(drs4_pedestal_run_id: int, pedcal_run_id: int) -> bool:
    """
    Check if both daily calibration (DRS4 baseline and
    charge calibration) files are already produced.
    """
    return drs4_pedestal_exists(drs4_pedestal_run_id, options.prod_id) \
        and calibration_file_exists(pedcal_run_id, options.prod_id)


def drs4_pedestal_command(drs4_pedestal_run_id: int) -> list:
    """Build the create_drs4_pedestal command."""
    base_dir = Path(cfg.get("LST1", "BASE")).resolve()
    return [
        "onsite_create_drs4_pedestal_file",
        f"--run_number={drs4_pedestal_run_id}",
        f"--base_dir={base_dir}",
        "--no-progress",
    ]


def calibration_file_command(drs4_pedestal_run_id: int, pedcal_run_id: int) -> list:
    """Build the create_calibration_file command."""
    base_dir = Path(cfg.get("LST1", "BASE")).resolve()
    cmd = [
        "onsite_create_calibration_file",
        f"--pedestal_run={drs4_pedestal_run_id}",
        f"--run_number={pedcal_run_id}",
        f"--base_dir={base_dir}",
    ]
    # In case of problems with trigger tagging:
    if cfg.getboolean("lstchain", "use_ff_heuristic_id"):
        cmd.append("--flatfield-heuristic")
    return cmd


def calibration_sequence(drs4_pedestal_run_id: int, pedcal_run_id: int) -> int:
    """
    Handle the two stages for creating the daily calibration products:
    DRS4 pedestal and charge calibration files.

    Parameters
    ----------
    drs4_pedestal_run_id : int
    pedcal_run_id : int

    Returns
    -------
    rc : int
        Return code
    """
    analysis_dir = Path(options.directory)
    history_file = analysis_dir / f"sequence_LST1_{pedcal_run_id:05d}.history"

    level, rc = (2, 0) if options.simulate else historylevel(history_file, "PEDCALIB")

    log.info(f"Going to level {level}")

    if level == 2:
        rc = drs4_pedestal(drs4_pedestal_run_id, pedcal_run_id)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 1:
        rc = calibrate_charge(drs4_pedestal_run_id, pedcal_run_id)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 0:
        log.info(f"Job for sequence {pedcal_run_id} finished without fatal errors")

    return rc


@trace
def drs4_pedestal(
    drs4_pedestal_run_id: int,
    pedcal_run_id: int,
) -> int:
    """
    Create a DRS4 pedestal file for baseline correction.

    Parameters
    ----------
    drs4_pedestal_run_id : int
        DRS4 pedestal run number
    pedcal_run_id : int
        PEDCALIB run number

    Returns
    -------
    rc : int
        Return code
    """
    if options.simulate or drs4_pedestal_exists(drs4_pedestal_run_id, options.prod_id):
        return 0

    cmd = drs4_pedestal_command(drs4_pedestal_run_id)

    analysis_step = DRS4PedestalStage(
        run=f"{drs4_pedestal_run_id:05d}", run_pedcal=f"{pedcal_run_id:05d}", command_args=cmd
    )
    analysis_step.execute()
    return analysis_step.rc


@trace
def calibrate_charge(
    drs4_pedestal_run_id: int,
    pedcal_run_id: int,
) -> int:
    """
    Create the calibration file to transform from ADC counts to photo-electrons

    Parameters
    ----------
    drs4_pedestal_run_id : int
        String with run number of the pedestal run
    pedcal_run_id : int
        String with run number of the pedcal run

    Returns
    -------
    rc: int
        Return code
    """
    if options.simulate or calibration_file_exists(pedcal_run_id, options.prod_id):
        return 0

    cmd = calibration_file_command(
        drs4_pedestal_run_id=drs4_pedestal_run_id, pedcal_run_id=pedcal_run_id
    )
    analysis_step = ChargeCalibrationStage(run=f"{pedcal_run_id:05d}", command_args=cmd)

    try:
        analysis_step.execute()
        return analysis_step.rc

    except Exception:
        log.info(f"Failed. Return code {analysis_step.rc}")
        cmd.append("--filters=52")
        log.info("Trying again by setting filters 52")
        analysis_step = ChargeCalibrationStage(run=f"{pedcal_run_id:05d}", command_args=cmd)
        analysis_step.execute()
        return analysis_step.rc


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

    if is_calibration_produced(drs4_pedestal_run_id=drs4_pedestal_run, pedcal_run_id=pedcal_run):
        log.info(
            f"Calibration files already produced from "
            f"runs {drs4_pedestal_run:05d} and {pedcal_run:05d}"
        )
        sys.exit(0)

    rc = calibration_sequence(drs4_pedestal_run, pedcal_run)
    sys.exit(rc)


if __name__ == "__main__":
    main()
