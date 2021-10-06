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
from osa.jobs.job import historylevel
from osa.reports.report import history
from osa.utils.cliopts import calibrationsequencecliparsing
from osa.utils.logging import myLogger
from osa.utils.standardhandle import stringify

__all__ = [
    "calibrationsequence",
    "calibrate_charge",
    "calibrate_time",
    "drs4_pedestal",
]

log = myLogger(logging.getLogger(__name__))


def calibrationsequence(
    pedestal_filename, calibration_filename, ped_run_number, cal_run_number, run_summary_file
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
    history_file = path.join(
        options.directory, f"sequence_{options.tel_id}_{cal_run_number}.history"
    )
    level, rc = (3, 0) if options.simulate else historylevel(history_file, "PEDCALIB")

    log.info(f"Going to level {level}")
    if level == 3:
        rc = drs4_pedestal(ped_run_number, pedestal_filename, history_file)
        level -= 1
        log.info(f"Going to level {level}")
    if level == 2:
        rc = calibrate_time(
            cal_run_number, pedestal_filename, calibration_filename, run_summary_file, history_file
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


def drs4_pedestal(run_ped, pedestal_output_file, history_file, max_events=20000, tel_id=1):
    """
    Create a DRS4 pedestal file

    Parameters
    ----------
    run_ped
    pedestal_output_file
    history_file
    max_events
    tel_id

    Returns
    -------
    Return code

    """
    if options.simulate:
        return 0

    rawdata_path = Path(cfg.get("LST1", "RAWDIR"))
    # Get raw data run regardless when was taken
    run_drs4_file_list = [
        file for file in rawdata_path.rglob(f'*/{cfg.get("LSTOSA", "R0PREFIX")}.Run{run_ped}.0000*')
    ]
    if run_drs4_file_list:
        input_file = str(run_drs4_file_list[0])
    else:
        log.error(f"Files corresponding to DRS4 pedestal run {run_ped} not found")
        sys.exit(1)

    calib_configfile = None
    output_file = path.join(options.directory, pedestal_output_file)
    command_args = [
        cfg.get("PROGRAM", "PEDESTAL"),
        "--input-file=" + input_file,
        "--output-file=" + output_file,
        f"--max-events={max_events}",
    ]
    command_concept = "drs4_pedestal"

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            run_ped,
            options.calib_prod_id,
            command_concept,
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
            command_concept,
            pedestal_output_file,
            calib_configfile,
            rc,
            history_file,
        )

    if rc != 0:
        sys.exit(rc)

    plot_file = path.join(options.directory, "log", f"drs4_pedestal.Run{run_ped}.0000.pdf")
    log.info(f"Producing plots in {plot_file}")
    drs4.plot_pedestals(input_file, output_file, run_ped, plot_file)
    plt.close("all")

    return rc


def calibrate_charge(
    run_ped, calibration_run, pedestal_file, calibration_output_file, run_summary, history_file
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
    Return code

    """
    if options.simulate:
        return 0

    rawdata_path = Path(cfg.get("LST1", "RAWDIR"))
    # Get raw data run regardless when was taken
    run_calib_file_list = [
        file
        for file in rawdata_path.rglob(
            f'*/{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.0000*'
        )
    ]
    if run_calib_file_list:
        calibration_run_file = str(run_calib_file_list[0])
    else:
        log.error(f"Files corresponding to calibration run {calibration_run} not found")
        sys.exit(1)

    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    drs4_pedestal_path = path.join(options.directory, pedestal_file)
    calib_output_file = path.join(options.directory, calibration_output_file)
    time_file = path.join(options.directory, f"time_{calibration_output_file}")
    log_output_file = path.join(
        options.directory, "log", f"calibration.Run{calibration_run}.0000.log"
    )
    max_events = 1000000
    min_ff = 4000
    max_ff = 12000
    stat_events = 10000

    command = "lstchain_create_calibration_file"

    command_args = [
        command,
        "--input_file=" + calibration_run_file,
        "--output_file=" + calib_output_file,
        f"--EventSource.max_events={max_events}",
        f"--EventSource.default_trigger_type=tib",
        f"--EventSource.min_flatfield_adc={min_ff}",
        f"--EventSource.max_flatfield_adc={max_ff}",
        "--LSTEventSource.EventTimeCalculator.run_summary_path=" + run_summary,
        "--LSTEventSource.LSTR0Corrections.drs4_time_calibration_path=" + time_file,
        "--LSTEventSource.LSTR0Corrections.drs4_pedestal_path=" + drs4_pedestal_path,
        f"--FlatFieldCalculator.sample_size={stat_events}",
        f"--PedestalCalculator.sample_size={stat_events}",
        "--log-file=" + log_output_file,
        "--config=" + calib_configfile,
    ]
    command_concept = "charge_calibration"

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            calibration_run,
            options.calib_prod_id,
            command_concept,
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
            command_concept,
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
    calib.plot_all(calib.ped_data, calib.ff_data, calib.calib_data, calibration_run, plot_file)
    plt.close("all")

    return rc


def calibrate_time(
    calibration_run, pedestal_file, calibration_output_file, run_summary, history_file, subrun=0
):
    """
    Create a time calibration file

    Parameters
    ----------
    calibration_run
    pedestal_file
    calibration_output_file
    history_file
    subrun

    Returns
    -------
    Return code

    """
    if options.simulate:
        return 0

    rawdata_path = Path(cfg.get("LST1", "RAWDIR"))
    # Get raw data run regardless when was taken
    run_calib_file_list = [
        file
        for file in rawdata_path.rglob(
            f'*/{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.{subrun:04d}.fits.fz'
        )
    ]

    if run_calib_file_list:
        calibration_data_file = str(run_calib_file_list[0])
    else:
        log.error(f"Files corresponding to calibration run {calibration_run} not found")
        sys.exit(1)

    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    time_calibration_output_file = path.join(options.directory, f"time_{calibration_output_file}")
    pedestal_file_path = path.join(options.directory, pedestal_file)

    command = "lstchain_data_create_time_calibration_file"
    command_args = [
        command,
        "--input-file=" + calibration_data_file,
        "--output-file=" + time_calibration_output_file,
        "--pedestal-file=" + pedestal_file_path,
        "--config=" + calib_configfile,
        "--run-summary-path=" + run_summary,
    ]
    command_concept = "time_calibration"

    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.call(command_args)
    except OSError as error:
        history(
            calibration_run,
            options.calib_prod_id,
            command_concept,
            path.basename(time_calibration_output_file),
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
            command_concept,
            path.basename(time_calibration_output_file),
            path.basename(calib_configfile),
            rc,
            history_file,
        )

    if rc == 1:
        log.warning("Not able to create time calibration file. Trying to use an existing file")
        # FIXME: take latest available time calibration file (eg from day before)
        def_time_calib_run = int(cfg.get("LSTOSA", "DEFAULT-TIME-CALIB-RUN"))
        calibpath = Path(cfg.get("LST1", "CALIBDIR"))
        outputf = time_calibration_output_file
        log.info(
            f"Searching for file */{options.calib_prod_id}/time_calibration.Run{def_time_calib_run:05d}*")
        file_list = [
            file
            for file in calibpath.rglob(
                f"*/{options.calib_prod_id}/time_calibration.Run{def_time_calib_run:05d}*"
            )
        ]
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
                command_concept,
                path.basename(time_calibration_output_file),
                path.basename(calib_configfile),
                rc,
                history_file,
            )
        else:
            log.error("Default time calibration file not found. Create it first.")
            sys.exit(1)

    return rc


if __name__ == "__main__":
    # Set the options through cli parsing
    (
        pedoutfile,
        caloutfile,
        calib_run_number,
        ped_run_number,
        run_summary,
    ) = calibrationsequencecliparsing()

    if options.verbose:
        logging.root.setLevel(logging.DEBUG)
    else:
        logging.root.setLevel(logging.INFO)

    # run the routine
    rc = calibrationsequence(pedoutfile, caloutfile, calib_run_number, ped_run_number, run_summary)
    sys.exit(rc)
