import os
import subprocess
import sys
from os import path
from pathlib import Path

import lstchain.visualization.plot_calib as calib
import lstchain.visualization.plot_drs4 as drs4

from osa.configs import options
from osa.configs.config import cfg
from osa.jobs.job import historylevel
from osa.reports.report import history
from osa.utils.cliopts import calibrationsequencecliparsing
from osa.utils.standardhandle import error, gettag, stringify, verbose, warning


def calibrationsequence(args):

    pedestal_filename = args[0]
    calibration_filename = args[1]
    ped_run_number = args[2]
    cal_run_number = args[3]

    historyfile = path.join(
        options.directory, f"sequence_{options.tel_id }_{cal_run_number}.history"
    )
    level, rc = historylevel(historyfile, "CALIBRATION")
    verbose(tag, f"Going to level {level}")
    if level == 3:
        rc = drs4_pedestal(ped_run_number, pedestal_filename, historyfile)
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 2:
        rc = calibrate_charge(
            ped_run_number,
            cal_run_number,
            pedestal_filename,
            calibration_filename,
            historyfile,
        )
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 1:
        rc = calibrate_time(cal_run_number, pedestal_filename, calibration_filename, historyfile)
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 0:
        verbose(tag, f"Job for sequence {ped_run_number} finished without fatal errors")
    return rc


def drs4_pedestal(run_ped, pedestal_output_file, historyfile):

    rawdata_path = Path(cfg.get("LST1", "RAWDIR"))
    # Get raw data run no matter when was taken
    run_drs4_file_list = [
        file for file in rawdata_path.rglob(f'*/{cfg.get("LSTOSA", "R0PREFIX")}.Run{run_ped}.0000*')
    ]
    if run_drs4_file_list:
        input_file = str(run_drs4_file_list[0])
    else:
        error(
            tag,
            f"Files corresponding to DRS4 pedestal run {run_ped} not found",
            1,
        )
        sys.exit(1)

    calib_configfile = None
    output_file = path.join(options.directory, pedestal_output_file)
    commandargs = [
        cfg.get("PROGRAM", "PEDESTAL"),
        "--input-file=" + input_file,
        "--output-file=" + output_file,
    ]
    commandconcept = "drs4_pedestal"

    # Error handling, for now no nonfatal errors are implemented for CALIBRATION
    try:
        verbose(tag, f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except OSError as ValueError:
        history(
            run_ped,
            options.calib_prod_id,
            commandconcept,
            pedestal_output_file,
            calib_configfile,
            ValueError,
            historyfile,
        )
        error(tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(
            run_ped,
            options.calib_prod_id,
            commandconcept,
            pedestal_output_file,
            calib_configfile,
            rc,
            historyfile,
        )

    if rc != 0:
        sys.exit(rc)

    plot_file = path.join(options.directory, "log", f"drs4_pedestal.Run{run_ped}.0000.pdf")
    verbose(tag, f"Producing plots in {plot_file}")
    drs4.plot_pedestals(
        input_file,
        output_file,
        run_ped,
        plot_file,
        tel_id=1,
        offset_value=300,
    )

    return rc


def calibrate_charge(run_ped, calibration_run, pedestal_file, calibration_output_file, historyfile):

    rawdata_path = Path(cfg.get("LST1", "RAWDIR"))
    # Get raw data run no matter when was taken
    run_calib_file_list = [
        file
        for file in rawdata_path.rglob(
            f'*/{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.0000*'
        )
    ]
    if run_calib_file_list:
        calibration_run_file = str(run_calib_file_list[0])
    else:
        error(
            tag,
            f"Files corresponding to calibration run {calibration_run} not found",
            1,
        )
        sys.exit(1)

    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    output_file = path.join(options.directory, calibration_output_file)
    log_output_file = path.join(
        options.directory, "log", f"calibration.Run{calibration_run}.0000.log"
    )
    commandargs = [
        cfg.get("PROGRAM", "CALIBRATION"),
        "--input_file=" + calibration_run_file,
        "--output_file=" + output_file,
        "--pedestal_file=" + pedestal_file,
        "--config=" + calib_configfile,
        "--log_file=" + log_output_file,
    ]
    commandconcept = "charge_calibration"

    try:
        verbose(tag, f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except OSError as ValueError:
        history(
            calibration_run,
            options.calib_prod_id,
            commandconcept,
            calibration_output_file,
            path.basename(calib_configfile),
            ValueError,
            historyfile,
        )
        error(tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            commandconcept,
            calibration_output_file,
            path.basename(calib_configfile),
            rc,
            historyfile,
        )

    if rc != 0:
        sys.exit(rc)

    plot_file = path.join(
        options.directory,
        "log",
        f"calibration.Run{calibration_run}.0000.pedestal.Run{run_ped}.0000.pdf",
    )
    calib.read_file(output_file, tel_id=1)
    verbose(tag, f"Producing plots in {plot_file}")
    calib.plot_all(calib.ped_data, calib.ff_data, calib.calib_data, calibration_run, plot_file)

    return rc


def calibrate_time(calibration_run, pedestal_file, calibration_output_file, historyfile):

    # A regular expression is used to fetch several input subruns
    calibration_data_files = (
        f'{cfg.get("LST1", "RAWDIR")}/*/'
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.*{cfg.get("LSTOSA", "R0SUFFIX")}'
    )
    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    time_calibration_output_file = path.join(options.directory, f"time_{calibration_output_file}")
    commandargs = [
        cfg.get("PROGRAM", "TIME_CALIBRATION"),
        "--input-file=" + calibration_data_files,
        "--output-file=" + time_calibration_output_file,
        "--pedestal-file=" + pedestal_file,
        "--config=" + calib_configfile,
    ]
    commandconcept = "time_calibration"

    try:
        verbose(tag, f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except OSError as ValueError:
        history(
            calibration_run,
            options.calib_prod_id,
            commandconcept,
            path.basename(time_calibration_output_file),
            path.basename(calib_configfile),
            ValueError,
            historyfile,
        )
        error(tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(
            calibration_run,
            options.calib_prod_id,
            commandconcept,
            path.basename(time_calibration_output_file),
            path.basename(calib_configfile),
            rc,
            historyfile,
        )

    if rc != 0:
        warning(
            tag,
            "Not able to create time calibration file. Creating a link "
            "to default calibration time file corresponding to run 1625",
        )
        def_time_calib_run = cfg.get("LSTOSA", "DEFAULT-TIME-CALIB-RUN")
        calibpath = Path(cfg.get("LST1", "CALIBDIR"))
        outputf = time_calibration_output_file
        file_list = [
            file
            for file in calibpath.rglob(
                f"*/{options.calib_prod_id}/time_calibration.Run{def_time_calib_run}*"
            )
        ]
        if file_list:
            verbose(
                tag,
                "Creating a link to default calibration " "time file corresponding to run 1625",
            )
            inputf = file_list[0]
            os.symlink(inputf, outputf)
            rc = 0
            history(
                calibration_run,
                options.calib_prod_id,
                commandconcept,
                path.basename(time_calibration_output_file),
                path.basename(calib_configfile),
                rc,
                historyfile,
            )
        else:
            error(
                tag,
                f"Default time calibration file {inputf} not found. Create it first.",
                1,
            )

    return rc


if __name__ == "__main__":

    tag = gettag()
    # set the options through cli parsing
    args = calibrationsequencecliparsing(sys.argv[0])
    # run the routine
    rc = calibrationsequence(args)
    sys.exit(rc)
