"""
Script that is called from the batch system to process a run
"""

import logging
import os
import subprocess
import sys
from os.path import basename, join

from osa.configs import options
from osa.configs.config import cfg
from osa.job import historylevel
from osa.provenance.capture import trace
from osa.report import history
from osa.utils.cliopts import datasequencecliparsing
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_dir, stringify

__all__ = ["datasequence", "r0_to_dl1", "dl1_to_dl2", "dl1ab", "dl1_datacheck"]

log = myLogger(logging.getLogger())


def datasequence(
        calibrationfile, pedestalfile, time_calibration, drivefile, run_summary, run_str
):
    """
    Performs all the steps to process a whole run.

    Parameters
    ----------
    calibrationfile
    pedestalfile
    time_calibration
    drivefile
    run_summary
    run_str

    Returns
    -------
    rc: int
        Return code of the last executed command.
    """
    historysuffix = cfg.get("LSTOSA", "HISTORYSUFFIX")
    sequenceprebuild = join(options.directory, f"sequence_{options.tel_id}_{run_str}")
    historyfile = sequenceprebuild + historysuffix
    level, rc = (4, 0) if options.simulate else historylevel(historyfile, "DATA")
    log.info(f"Going to level {level}")

    if level == 4:
        rc = r0_to_dl1(
            calibrationfile,
            pedestalfile,
            time_calibration,
            drivefile,
            run_summary,
            run_str,
            historyfile,
        )
        level -= 1
        log.info(f"Going to level {level}")

    if level == 3:
        rc = dl1ab(run_str, historyfile)
        if cfg.getboolean("lstchain", "store_image_dl1ab"):
            level -= 1
            log.info(f"Going to level {level}")
        else:
            level -= 2
            log.info(f"No images stored in dl1ab. Producing DL2. Going to level {level}")

    if level == 2:
        rc = dl1_datacheck(run_str, historyfile)
        if options.nodl2:
            level = 0
            log.info(f"No DL2 are going to be produced. Going to level {level}")
        else:
            level -= 1
            log.info(f"Going to level {level}")

    if level == 1:
        rc = dl1_to_dl2(run_str, historyfile)
        level -= 1
        log.info(f"Going to level {level}")

    if level == 0:
        log.info(f"Job for sequence {run_str} finished without fatal errors")
    return rc


# FIXME: Parse all different arguments via config file or sequence_list.txt
@trace
def r0_to_dl1(
        calibrationfile,
        pedestalfile,
        time_calibration,
        drivefile,
        run_summary,
        run_str,
        historyfile,
):
    """
    Prepare and launch the actual lstchain script that is performing
    the low and high-level calibration to raw camera images.
    It also applies the image cleaning and obtains shower parameters.

    Parameters
    ----------
    calibrationfile
    pedestalfile
    time_calibration
    drivefile
    run_summary: str
        Path to the run summary file
    run_str: str
        XXXXX.XXXX (run_number.subrun_number)
    historyfile

    Returns
    -------
    rc: int
        Return code of the executed command.
    """

    if options.simulate:
        return 0

    command = cfg.get("LSTOSA", "R0-DL1")
    nightdir = lstdate_to_dir(options.date)
    datafile = join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "R0SUFFIX")}',
    )

    # Prepare and launch the actual lstchain script
    commandargs = [
        command,
        "--input-file=" + datafile,
        "--output-dir=" + options.directory,
        "--pedestal-file=" + pedestalfile,
        "--calibration-file=" + calibrationfile,
        "--time-calibration-file=" + time_calibration,
        "--pointing-file=" + drivefile,
        "--run-summary-path=" + run_summary,
    ]
    return run_program_with_logging(
        commandargs,
        historyfile,
        run_str,
        options.prod_id,
        command,
        basename(calibrationfile),
        basename(pedestalfile),
    )


def dl1ab(run_str, historyfile):
    """
    Prepare and launch the actual lstchain script that is performing
    the the image cleaning considering the interleaved pedestal information
    and obtains shower parameters. It keeps the shower images.

    Parameters
    ----------
    run_str
    historyfile

    Returns
    -------
    rc: int
    """

    if options.simulate:
        return 0

    # Create a new subdirectory for the dl1ab output
    # TODO: create and option directory dl1ab
    dl1ab_subdirectory = os.path.join(options.directory, options.dl1_prod_id)
    os.makedirs(dl1ab_subdirectory, exist_ok=True)
    config_file = cfg.get("LSTOSA", "CONFIGFILE")

    input_dl1_datafile = join(
        options.directory,
        f'{cfg.get("LSTOSA", "DL1PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "DL1SUFFIX")}',
    )

    output_dl1_datafile = join(
        dl1ab_subdirectory,
        f'{cfg.get("LSTOSA", "DL1PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "DL1SUFFIX")}',
    )

    # Prepare and launch the actual lstchain script
    command = "lstchain_dl1ab"

    if cfg.getboolean("lstchain", "store_image_dl1ab"):
        commandargs = [
            command,
            "--input-file=" + input_dl1_datafile,
            "--output-file=" + output_dl1_datafile,
            "--pedestal-cleaning=True",
            "--config=" + config_file,
        ]
    else:
        commandargs = [
            command,
            "--input-file=" + input_dl1_datafile,
            "--output-file=" + output_dl1_datafile,
            "--pedestal-cleaning=True",
            "--no-image=True",
            "--config=" + config_file,
        ]

    return run_program_with_logging(
        commandargs,
        historyfile,
        run_str,
        options.dl1_prod_id,
        command,
        basename(input_dl1_datafile),
        config_file,
    )


def dl1_datacheck(run_str, historyfile):
    """
    Run datacheck script

    Parameters
    ----------
    run_str
    historyfile

    Returns
    -------
    rc
    """

    if options.simulate:
        return 0

    # Create a new subdirectory for the dl1ab output
    dl1ab_subdirectory = os.path.join(options.directory, options.dl1_prod_id)

    input_dl1_datafile = join(
        dl1ab_subdirectory,
        f'{cfg.get("LSTOSA", "DL1PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "DL1SUFFIX")}',
    )
    output_directory = os.path.join(options.directory, options.dl1_prod_id)
    os.makedirs(output_directory, exist_ok=True)

    # Prepare and launch the actual lstchain script
    command = "lstchain_check_dl1"
    commandargs = [
        command,
        "--input-file=" + input_dl1_datafile,
        "--output-dir=" + output_directory,
        "--muons-dir=" + options.directory,
        "--omit-pdf",
        "--batch",
    ]

    return run_program_with_logging(
        commandargs,
        historyfile,
        run_str,
        options.dl1_prod_id,
        command,
        basename(input_dl1_datafile),
        None,
    )


@trace
def dl1_to_dl2(run_str, historyfile):
    """
    It prepares and execute the dl1 to dl2 lstchain scripts that applies
    the already trained RFs models to DL1 files. It identifies the
    primary particle, reconstructs its energy and direction.

    Parameters
    ----------
    run_str
    historyfile
    """

    if options.simulate:
        return 0

    dl1ab_subdirectory = os.path.join(options.directory, options.dl1_prod_id)
    dl2_subdirectory = os.path.join(options.directory, options.dl2_prod_id)

    configfile = cfg.get("LSTOSA", "DL2CONFIGFILE")
    rf_models_directory = cfg.get("LSTOSA", "RF-MODELS-DIR")
    command = cfg.get("LSTOSA", "DL1-DL2")  # FIXME  change LSTOSA by lstchain
    datafile = join(
        dl1ab_subdirectory,
        f'{cfg.get("LSTOSA", "DL1PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "DL1SUFFIX")}',
    )

    commandargs = [
        command,
        "--input-file=" + datafile,
        "--output-dir=" + dl2_subdirectory,
        "--path-models=" + rf_models_directory,
        "--config=" + configfile,
    ]

    return run_program_with_logging(
        commandargs,
        historyfile,
        run_str,
        options.dl2_prod_id,
        command,
        basename(datafile),
        basename(configfile),
    )


def run_program_with_logging(
        commandargs, historyfile, run, prod_id, stage, input_file, config_file
):
    """
    Run the program and log the output in the history file

    Returns
    -------
    rc: int
        Return code of the program
    """

    try:
        log.info(f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")
    except OSError as error:
        log.exception(f"Command {stringify(commandargs)} failed, {error}")
    else:
        history(run, prod_id, stage, input_file, config_file, rc, historyfile)
        if rc != 0:
            sys.exit(rc)
        return rc


def main():
    """Performs the analysis steps to convert raw data into DL2 files."""
    (
        calib_file,
        drs4_ped_file,
        time_calib_file,
        drive_log_file,
        run_summary_file,
        run_number,
    ) = datasequencecliparsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # run the routine
    rc = datasequence(
        calib_file,
        drs4_ped_file,
        time_calib_file,
        drive_log_file,
        run_summary_file,
        run_number,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
