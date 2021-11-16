"""
Script to process the calibration runs and produce the
DRS4 pedestal, charge and time calibration files.
"""

import logging
import os
import subprocess
import sys
from os import path
from pathlib import Path

import lstchain.visualization.plot_calib as calib
import lstchain.visualization.plot_drs4 as drs4
import matplotlib.pyplot as plt

from osa.configs import options
from osa.configs.config import cfg
from osa.job import historylevel
from osa.report import history
from osa.utils.cliopts import calibrationsequencecliparsing
from osa.utils.logging import myLogger
from osa.utils.utils import stringify, get_input_file


__all__ = [
    "calibrationsequence",
    "calibrate_charge",
    "calibrate_time",
    "drs4_pedestal",
]

log = myLogger(logging.getLogger())


def calibrationsequence(
        pedestal_filename,
        calibration_filename,
        ped_run_number,
        cal_run_number,
        run_summary_file,
):
    """
    Handle the three steps for creating the calibration products:
    DRS4 pedestal, charge calibration and time calibration files

    Parameters
    ----------
    pedestal_filename
    calibration_filename
    ped_run_number
    cal_run_number
    run_summary_file

    Returns
    -------
    rc

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


def drs4_pedestal(run_ped, pedestal_output_file, history_file, max_events=20000):
    """
    Create a DRS4 pedestal file

    Parameters
    ----------
    run_ped: str
        String with run number of the pedestal run
    pedestal_output_file
    history_file
    max_events

    Returns
    -------
    Return code

    """
    if options.simulate:
        return 0

    input_file = get_input_file(run_ped)

    calib_configfile = None
    output_file = path.join(options.directory, pedestal_output_file)

    command = "drs4_baseline"
    command_args = [
        cfg.get("lstchain", command),
        f"--input-file={input_file}",
        f"--output-file={output_file}",
        f"--max-events={max_events}",
        "--overwrite"
    ]

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            run_ped,
            options.calib_prod_id,
            command,
            pedestal_output_file,
            calib_configfile,
            error,
            history_file,
        )
        log.exception(f"Could not execute {stringify(command_args)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"{error}, {rc}")
    else:
        history(
            run_ped,
            options.calib_prod_id,
            command,
            pedestal_output_file,
            calib_configfile,
            rc,
            history_file,
        )

    if rc != 0:
        sys.exit(rc)

    analysis_directory = Path(options.directory)
    plot_file = analysis_directory / "log", f"drs4_pedestal.Run{run_ped}.0000.pdf"
    log.info(f"Producing plots in {plot_file}")
    drs4.plot_pedestals(input_file, output_file, run_ped, plot_file)
    plt.close("all")

    return rc


def calibrate_charge(
        run_ped,
        calibration_run,
        pedestal_file,
        calibration_output_file,
        run_summary,
        history_file,
):
    """
    Create a charge calibration file to transform from ADC counts to photo-electrons

    Parameters
    ----------
    run_ped
    calibration_run
    pedestal_file
    calibration_output_file
    history_file
    run_summary: str
        Path name of the run summary file

    Returns
    -------
    rc: str
        Return code
    """
    if options.simulate:
        return 0

    calibration_run_file = get_input_file(calibration_run)

    calib_configfile = cfg.get("lstchain", "calibration_config_file")
    ffactor_systematics = cfg.get("lstchain", "ffactor_systematics")
    drs4_pedestal_path = path.join(options.directory, pedestal_file)
    calib_output_file = path.join(options.directory, calibration_output_file)
    time_file = path.join(options.directory, f"time_{calibration_output_file}")
    log_output_file = path.join(
        options.directory, "log", f"calibration.Run{calibration_run}.0000.log"
    )

    command = "charge_calibration"
    command_args = [
        cfg.get("lstchain", command),
        f"--input_file={calibration_run_file}",
        f"--output_file={calib_output_file}",
        "--EventSource.default_trigger_type=tib",
        f"--LSTCalibrationCalculator.systematic_correction_path={ffactor_systematics}",
        f"--LSTEventSource.EventTimeCalculator.run_summary_path={run_summary}",
        f"--LSTEventSource.LSTR0Corrections.drs4_time_calibration_path={time_file}",
        f"--LSTEventSource.LSTR0Corrections.drs4_pedestal_path={drs4_pedestal_path}",
        f"--log-file={log_output_file}",
        f"--config={calib_configfile}"
    ]

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            calibration_run,
            options.calib_prod_id,
            command,
            calibration_output_file,
            path.basename(calib_configfile),
            error,
            history_file,
        )
        log.exception(f"Could not execute {stringify(command_args)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"{error}, {rc}")
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            command,
            calibration_output_file,
            path.basename(calib_configfile),
            rc,
            history_file,
        )

    if rc != 0:
        sys.exit(rc)

    plot_file = path.join(
        options.directory,
        "log",
        f"calibration.Run{calibration_run}.0000.pedestal.Run{run_ped}.0000.pdf",
    )
    calib.read_file(calib_output_file, tel_id=1)
    log.info(f"Producing plots in {plot_file}")
    calib.plot_all(
        calib.ped_data, calib.ff_data, calib.calib_data, calibration_run, plot_file
    )
    plt.close("all")

    return rc


def calibrate_time(
        calibration_run, pedestal_file, calibration_output_file, run_summary, history_file
):
    """
    Create a time calibration file

    Parameters
    ----------
    calibration_run
    pedestal_file
    calibration_output_file
    run_summary
    history_file

    Returns
    -------
    rc: int
        Return code
    """
    if options.simulate:
        return 0

    r0_path = Path(cfg.get("LST1", "RAWDIR")).absolute()
    calibration_data_file = f"{r0_path}/*/LST-1.1.Run{calibration_run}.000*.fits.fz"

    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    time_calibration_file = Path(options.directory) / f"time_{calibration_output_file}"
    pedestal_file_path = Path(options.directory) / pedestal_file

    command = "time_calibration"
    command_args = [
        cfg.get("lstchain", command),
        f"--input-file={calibration_data_file}",
        f"--output-file={time_calibration_file}",
        f"--pedestal-file={pedestal_file_path}",
        f"--run-summary-path={run_summary}",
        "--max-events=53000"
    ]

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            calibration_run,
            options.calib_prod_id,
            command,
            time_calibration_file.name,
            calib_configfile.name,
            error,
            history_file,
        )
        log.exception(f"Could not execute {stringify(command_args)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"{error}, {rc}")
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            command,
            time_calibration_file.name,
            calib_configfile.name,
            rc,
            history_file,
        )

    if rc == 1:
        log.warning(
            "Not able to create time calibration file. Trying to use an existing file"
        )
        # FIXME: take latest available time calibration file (eg from day before)
        def_time_calib_run = int(cfg.get("LSTOSA", "DEFAULT-TIME-CALIB-RUN"))
        calibpath = Path(cfg.get("LST1", "CALIBDIR"))
        outputf = time_calibration_file
        log.info(
            f"Searching for file "
            f"*/{options.calib_prod_id}/time_calibration.Run{def_time_calib_run:05d}*"
        )
        file_list = list(
            calibpath.rglob(
                f'*/{options.calib_prod_id}/'
                f'time_calibration.Run{def_time_calib_run:05d}*'
            )
        )

        if file_list:
            log.info(
                f"Creating a symlink to an already produce time calibration "
                f"file corresponding to run {def_time_calib_run:05d}"
            )
            inputf = file_list[0]
            os.symlink(inputf, outputf)
            rc = 0
            history(
                calibration_run,
                options.calib_prod_id,
                command,
                time_calibration_file.name,
                calib_configfile.name,
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
    ) = calibrationsequencecliparsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # run the routine
    rc = calibrationsequence(
        pedoutfile, caloutfile, calib_run_number, ped_run_number, run_summary
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
