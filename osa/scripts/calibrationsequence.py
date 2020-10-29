import subprocess
import sys
from os import path

from osa.configs.config import cfg
from osa.jobs.job import historylevel
from osa.reports.report import history
from osa.configs import options
from osa.utils.cliopts import calibrationsequencecliparsing
from osa.utils.standardhandle import error, gettag, stringify, verbose
from osa.utils.utils import lstdate_to_dir

import lstchain.visualization.plot_drs4 as drs4
import lstchain.visualization.plot_calib as calib


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
        rc = calibrate_time(
            cal_run_number, pedestal_filename, calibration_filename, historyfile
        )
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 0:
        verbose(tag, f"Job for sequence {ped_run_number} finished without fatal errors")
    return rc


def drs4_pedestal(run_ped, pedestal_output_file, historyfile):

    nightdir = lstdate_to_dir(options.date)
    calib_configfile = None
    input_file = path.join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{run_ped}.0000{cfg.get("LSTOSA", "R0SUFFIX")}',
    )
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
        error(
            tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError
        )
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

    plot_file = path.join(
        options.directory, "log", f"drs4_pedestal.Run{run_ped}.0000.pdf"
    )
    verbose(tag, f"Producing plots in {plot_file} ...")
    drs4.plot_pedestals(
        input_file,
        output_file,
        run_ped,
        plot_file,
        tel_id=1,
        offset_value=300,
    )

    return rc


def calibrate_charge(
    run_ped, calibration_run, pedestal_file, calibration_output_file, historyfile
):

    nightdir = lstdate_to_dir(options.date)
    calibration_run_file = path.join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.0000{cfg.get("LSTOSA", "R0SUFFIX")}',
    )
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

    # Error handling, for now no nonfatal errors are implemented for CALIBRATION
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
        error(
            tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError
        )
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
    verbose(tag, f"Producing plots in {plot_file} ...")
    calib.plot_all(
        calib.ped_data, calib.ff_data, calib.calib_data, calibration_run, plot_file
    )

    return rc


def calibrate_time(
    calibration_run, pedestal_file, calibration_output_file, historyfile
):

    nightdir = lstdate_to_dir(options.date)
    calibration_data_file = path.join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run}.0000{cfg.get("LSTOSA", "R0SUFFIX")}',
    )
    calib_configfile = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    time_calibration_output_file = path.join(
        options.directory, f"time_{calibration_output_file}"
    )
    # calculate_time_run = cfg.get('LSTOSA', 'CALCULATE_TIME_RUN')  #def: '1625'
    commandargs = [
        cfg.get("PROGRAM", "TIME_CALIBRATION"),
        "--input-file=" + calibration_data_file,
        "--output-file=" + time_calibration_output_file,
        "--pedestal-file=" + pedestal_file,
        "--config=" + calib_configfile,
    ]
    commandconcept = "time_calibration"

    # error handling, for now no nonfatal errors are implemented for CALIBRATION
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
        error(
            tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError
        )
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
        sys.exit(rc)

    return rc


if __name__ == "__main__":

    tag = gettag()
    # set the options through cli parsing
    args = calibrationsequencecliparsing(sys.argv[0])
    # run the routine
    rc = calibrationsequence(args)
    sys.exit(rc)
