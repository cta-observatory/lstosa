"""
Script to process the calibration runs and produce the
DRS4 pedestal, charge and time calibration files.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import lstchain.visualization.plot_calib as calib
import lstchain.visualization.plot_drs4 as drs4
import matplotlib.pyplot as plt

from osa.configs import options
from osa.configs.config import cfg
from osa.job import historylevel
from osa.report import history
from osa.utils.cliopts import calibration_sequence_cliparsing
from osa.utils.logging import myLogger
from osa.utils.utils import stringify, get_input_file

__all__ = [
    "calibration_sequence",
    "calibrate_charge",
    "calibrate_time",
    "drs4_pedestal",
    "plot_calibration_checks",
    "plot_drs4_pedestal_check",
    "drs4_pedestal_command",
    "calibration_file_command",
    "time_calibration_command"
]

log = myLogger(logging.getLogger())


def calibration_sequence(
        pedestal_filename: Path,
        calibration_filename: Path,
        ped_run_number: str,
        cal_run_number: str,
        run_summary_file: Path,
) -> int:
    """
    Handle the three steps for creating the calibration products:
    DRS4 pedestal, charge calibration and time calibration files

    Parameters
    ----------
    pedestal_filename: pathlib.Path
    calibration_filename: pathlib.Path
    ped_run_number: str
    cal_run_number: str
    run_summary_file: pathlib.Path

    Returns
    -------
    rc: int
        Return code
    """
    history_file = \
        Path(options.directory) / f"sequence_{options.tel_id}_{cal_run_number}.history"

    level, rc = (3, 0) if options.simulate else historylevel(history_file, "PEDCALIB")

    log.info(f"Going to level {level}")
    if level == 3:
        rc = drs4_pedestal(ped_run_number, pedestal_filename, history_file)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 2:
        rc = calibrate_time(
            cal_run_number,
            pedestal_filename,
            calibration_filename,
            run_summary_file,
            history_file,
        )
        level -= 1
        log.info(f"Going to level {level}")
    if level == 1:
        rc = calibrate_charge(
            ped_run_number,
            cal_run_number,
            pedestal_filename,
            calibration_filename,
            run_summary_file,
            history_file,
        )
        level -= 1
        log.info(f"Going to level {level}")
    if level == 0:
        log.info(f"Job for sequence {ped_run_number} finished without fatal errors")
    return rc


def drs4_pedestal_command(
        input_file: Path,
        output_file: Path,
        max_events=20000
) -> list:
    """Build the command to run the drs4 pedestal calibration."""
    return [
        cfg.get("lstchain", "drs4_baseline"),
        f"--input-file={input_file}",
        f"--output-file={output_file}",
        f"--max-events={max_events}",
        "--overwrite"
    ]


def calibration_file_command(
        calibration_run: str,
        calibration_configfile: Path,
        drs4_pedestal_path: Path,
        calibration_output_file: Path,
        run_summary_file: Path,
) -> list:
    """Build the command to run the calibration file creation."""
    calibration_run_file = get_input_file(calibration_run)
    ffactor_systematics = Path(cfg.get("lstchain", "ffactor_systematics"))
    time_file = Path(options.directory) / f"time_{calibration_output_file.name}"
    log_dir = Path(options.directory) / "log"
    log_file = log_dir / f"calibration.Run{calibration_run}.0000.log"

    return [
        cfg.get("lstchain", "charge_calibration"),
        f"--input_file={calibration_run_file}",
        f"--output_file={calibration_output_file}",
        "--EventSource.default_trigger_type=tib",
        f"--LSTCalibrationCalculator.systematic_correction_path={ffactor_systematics}",
        f"--LSTEventSource.EventTimeCalculator.run_summary_path={run_summary_file}",
        f"--LSTEventSource.LSTR0Corrections.drs4_time_calibration_path={time_file}",
        f"--LSTEventSource.LSTR0Corrections.drs4_pedestal_path={drs4_pedestal_path}",
        f"--log-file={log_file}",
        f"--config={calibration_configfile}"
    ]


def time_calibration_command(
        calibration_run: str,
        time_calibration_file: Path,
        drs4_pedestal_file: Path,
        run_summary: Path,
) -> list:
    """Build the command to run the time calibration."""
    r0_path = Path(cfg.get("LST1", "R0_DIR")).resolve()
    calibration_data_file = f"{r0_path}/*/LST-1.1.Run{calibration_run}.000*.fits.fz"

    return [
        cfg.get("lstchain", "time_calibration"),
        f"--input-file={calibration_data_file}",
        f"--output-file={time_calibration_file}",
        f"--pedestal-file={drs4_pedestal_file}",
        f"--run-summary-path={run_summary}",
        "--max-events=53000"
    ]


def drs4_pedestal(
        run_ped: str,
        pedestal_output_file: Path,
        history_file: Path,
        max_events=20000
):
    """
    Create a DRS4 pedestal file for baseline correction.

    Parameters
    ----------
    run_ped: str
        String with run number of the pedestal run
    pedestal_output_file: pathlib.Path
        Path to the output file to be created
    history_file: pathlib.Path
        Path to the history file
    max_events: int

    Returns
    -------
    Return code
    """
    input_file = get_input_file(run_ped)
    output_file = Path(options.directory) / pedestal_output_file
    calib_configfile = "Default"

    command_args = drs4_pedestal_command(input_file, output_file, max_events)

    if options.simulate:
        return 0

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.run(command_args, check=True).returncode
    except subprocess.CalledProcessError as error:
        rc = error.returncode
        history(
            run_ped,
            options.calib_prod_id,
            command_args[0],
            pedestal_output_file.name,
            calib_configfile,
            rc,
            history_file,
        )
        log.exception(f"Could not execute {stringify(command_args)}, error: {rc}")
    else:
        history(
            run_ped,
            options.calib_prod_id,
            command_args[0],
            pedestal_output_file.name,
            calib_configfile,
            rc,
            history_file,
        )

    if rc != 0:
        sys.exit(rc)

    plot_drs4_pedestal_check(input_file, output_file, run_ped)

    return rc


def plot_drs4_pedestal_check(
        input_file: Path,
        output_file: Path,
        drs4_run: str
) -> None:
    """Plot the check for the drs4 baseline correction."""
    analysis_log_directory = Path(options.directory) / "log"
    plot_file = analysis_log_directory / f"drs4_pedestal.Run{drs4_run}.0000.pdf"
    log.info(f"Producing plots in {plot_file}")
    drs4.plot_pedestals(input_file, output_file, drs4_run, plot_file)
    plt.close("all")


def calibrate_charge(
        run_ped: str,
        calibration_run: str,
        pedestal_file: Path,
        calibration_output_file: Path,
        run_summary: Path,
        history_file: Path,
) -> int:
    """
    Create a charge calibration file to transform from ADC counts to photo-electrons

    Parameters
    ----------
    run_ped
    calibration_run
    pedestal_file
    calibration_output_file
    history_file
    run_summary: pathlib.Path
        Path name of the run summary file

    Returns
    -------
    rc: str
        Return code
    """
    calib_configfile = Path(cfg.get("lstchain", "calibration_config"))
    command_args = calibration_file_command(
        calibration_run,
        calib_configfile,
        pedestal_file,
        calibration_output_file,
        run_summary
    )

    if options.simulate:
        return 0

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.run(command_args, check=True).returncode
    except subprocess.CalledProcessError as error:
        rc = error.returncode
        history(
            calibration_run,
            options.calib_prod_id,
            command_args[0],
            calibration_output_file.name,
            calib_configfile.name,
            rc,
            history_file,
        )
        log.exception(f"Could not execute {stringify(command_args)}, error: {error}")
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            command_args[0],
            calibration_output_file.name,
            calib_configfile.name,
            rc,
            history_file,
        )

    if rc != 0:
        sys.exit(rc)

    plot_calibration_checks(calibration_run, run_ped, calibration_output_file)

    return rc


def plot_calibration_checks(
        calibration_run: str,
        drs4_run: str,
        calibration_output_file: Path
) -> None:
    """Produce check plots."""
    analysis_log_directory = Path(options.directory) / "log"
    plot_file = analysis_log_directory / \
                f"calibration.Run{calibration_run}.0000.pedestal.Run{drs4_run}.0000.pdf"
    calib.read_file(calibration_output_file, tel_id=1)
    log.info(f"Producing plots in {plot_file}")
    calib.plot_all(
        calib.ped_data, calib.ff_data, calib.calib_data, calibration_run, plot_file
    )
    plt.close("all")


def calibrate_time(
        calibration_run: str,
        pedestal_file: Path,
        calibration_output_file: Path,
        run_summary: Path,
        history_file: Path
) -> int:
    """
    Create a time calibration file

    Parameters
    ----------
    calibration_run: str
        Run number of the calibration run
    pedestal_file: pathlib.Path
        Path to the pedestal file
    calibration_output_file: pathlib.Path
        Path to the output calibration file
    run_summary: pathlib.Path
        Path to the run summary file
    history_file: pathlib.Path
        Path to the history file

    Returns
    -------
    rc: int
        Return code
    """
    time_calibration_file = Path(options.directory) / \
                            f"time_{calibration_output_file.name}"

    command_args = time_calibration_command(
        calibration_run,
        time_calibration_file,
        pedestal_file,
        run_summary
    )

    calib_configfile = "Default"

    if options.simulate:
        return 0

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.run(command_args, check=True).returncode
    except subprocess.CalledProcessError as error:
        rc = error.returncode
        history(
            calibration_run,
            options.calib_prod_id,
            command_args[0],
            time_calibration_file.name,
            calib_configfile,
            rc,
            history_file,
        )
        log.exception(
            f"Could not execute {stringify(command_args)}, error: {rc}"
        )
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            command_args[0],
            time_calibration_file.name,
            calib_configfile,
            rc,
            history_file,
        )

    if rc != 0:
        log.warning(
            "Not able to create time calibration file. Trying to use an existing file"
        )
        rc = link_ref_time_calibration(
            time_calibration_file,
            calibration_run,
            history_file
        )

    return rc


def link_ref_time_calibration(
        time_calibration_file: Path,
        calibration_run: str,
        history_file: Path,
        command="link_time_calibration",
        calibration_configfile="Default",
) -> int:
    """
    Link the reference time calibration file to the current time calibration file.
    The reference time calibration file is defined in the cfg file.
    """

    ref_time_calibration_run = int(
        cfg.get("lstchain", "reference_time_calibration_run")
    )
    calibration_path = Path(cfg.get("LST1", "CALIB_DIR"))
    output_file = time_calibration_file
    log.info(
        f"Searching for file "
        f"*/{options.calib_prod_id}/time_calibration.Run{ref_time_calibration_run:05d}*"
    )
    file_list = list(
        calibration_path.rglob(
            f'*/{options.calib_prod_id}/'
            f'time_calibration.Run{ref_time_calibration_run:05d}*'
        )
    )

    if file_list:
        log.info(
            f"Creating a symlink to an already produce time calibration "
            f"file corresponding to run {ref_time_calibration_run:05d}"
        )
        ref_time_calibration_file = file_list[0]
        os.symlink(ref_time_calibration_file, output_file)
        rc = 0
        history(
            calibration_run,
            options.calib_prod_id,
            command,
            time_calibration_file.name,
            calibration_configfile,
            rc,
            history_file,
        )
    else:
        log.error("Default time calibration file not found. Create it first.")
        sys.exit(1)

    return rc


def main():
    """
    Performs the calibration steps (obtain the drs4 baseline correction,
    drs4 time correction and ADC to photo-electron coefficients).
    """
    (
        pedoutfile,
        caloutfile,
        calib_run_number,
        ped_run_number,
        run_summary,
    ) = calibration_sequence_cliparsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # run the routine
    rc = calibration_sequence(
        pedoutfile, caloutfile, calib_run_number, ped_run_number, run_summary
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
