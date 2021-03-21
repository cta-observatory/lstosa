"""
Script that is called from the batch system to process a run
"""

import logging
import subprocess
import sys
from os.path import basename, join

from osa.configs import options
from osa.configs.config import cfg
from osa.jobs.job import historylevel
from osa.provenance.capture import trace
from osa.reports.report import history
from osa.utils.cliopts import datasequencecliparsing
from osa.utils.logging import MyFormatter
from osa.utils.standardhandle import stringify
from osa.utils.utils import lstdate_to_dir

__all__ = ["datasequence", "r0_to_dl1", "dl1_to_dl2"]

log = logging.getLogger(__name__)

# Logging
fmt = MyFormatter()
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logging.root.addHandler(handler)


def datasequence(
    calibrationfile, pedestalfile, time_calibration, drivefile,
    ucts_t0_dragon, dragon_counter0, ucts_t0_tib, tib_counter0,
    run_str
):
    """
    Performs all the steps to process a whole run

    Parameters
    ----------
    calibrationfile
    pedestalfile
    time_calibration
    drivefile
    ucts_t0_dragon
    dragon_counter0
    ucts_t0_tib
    tib_counter0
    run_str

    Returns
    -------

    """
    historysuffix = cfg.get("LSTOSA", "HISTORYSUFFIX")
    sequenceprebuild = join(options.directory, f"sequence_{options.tel_id}_{run_str}")
    historyfile = sequenceprebuild + historysuffix
    level, rc = (3, 0) if options.simulate else historylevel(historyfile, "DATA")
    log.debug(f"Going to level {level}")

    if level == 3:
        rc = r0_to_dl1(
            calibrationfile,
            pedestalfile,
            time_calibration,
            drivefile,
            ucts_t0_dragon,
            dragon_counter0,
            ucts_t0_tib,
            tib_counter0,
            run_str,
            historyfile,
        )
        level -= 1
        log.debug(f"Going to level {level}")
    if level == 2:
        rc = dl1_to_dl2(run_str, historyfile)
        level -= 2
        log.debug(f"Going to level {level}")
    if level == 0:
        log.debug(f"Job for sequence {run_str} finished without fatal errors")
    return rc


# FIXME: Parse all different arguments via config file or sequence_list.txt
@trace
def r0_to_dl1(calibrationfile, pedestalfile, time_calibration, drivefile, run_summary_file, run_str, historyfile):
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
    run_summary_file: str
        Path to the run summary file
    run_str
    historyfile

    Returns
    -------
    rc
    """

    if options.simulate:
        return 0

    configfile = cfg.get("LSTOSA", "CONFIGFILE")
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
        "--config=" + configfile,
        "--time-calibration-file=" + time_calibration,
        "--pointing-file=" + drivefile,
        "--run-summary-path=" + run_summary_file,
    ]

    try:
        log.debug(f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")
    except OSError as error:
        log.exception(f"Command {stringify(commandargs)} failed, {error}")
    else:
        history(
            run_str,
            options.prod_id,  # TODO: consider only DL2 prod ID?
            command,
            basename(calibrationfile),
            basename(pedestalfile),
            rc,
            historyfile,
        )
        try:
            nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS', 'R0-DL1').split(",")]
        except:
            nonfatalrcs = [0]
        if rc not in nonfatalrcs:
            sys.exit(rc)
        return rc


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

    configfile = cfg.get("LSTOSA", "DL2CONFIGFILE")
    rf_models_directory = cfg.get("LSTOSA", "RF-MODELS-DIR")
    command = cfg.get("LSTOSA", "DL1-DL2")  # FIXME  change LSTOSA by lstchain
    datafile = join(
        options.directory,
        f'{cfg.get("LSTOSA", "DL1PREFIX")}.Run{run_str}{cfg.get("LSTOSA", "DL1SUFFIX")}',
    )

    commandargs = [
        command,
        "--input-file=" + datafile,
        "--output-dir=" + options.directory,
        "--path-models=" + rf_models_directory,
        "--config=" + configfile,
    ]

    try:
        log.debug(f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")
    except OSError as error:
        log.exception(f"Command {stringify(commandargs)} failed, error: {error}")
    else:
        history(
            run_str,
            options.dl2_prod_id,
            command,
            basename(datafile),
            basename(configfile),
            rc,
            historyfile,
        )
        try:
            nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS', 'DL1-DL2').split(",")]
        except:
            nonfatalrcs = [0]
        if rc not in nonfatalrcs:
            sys.exit(rc)
        return rc


if __name__ == "__main__":
    # set the arguments and options through cli parsing
    drs4_ped_file, calib_file, time_calib_file, drive_log_file, run_summary_file, run_number = datasequencecliparsing()

    if options.verbose:
        logging.root.setLevel(logging.DEBUG)
    else:
        logging.root.setLevel(logging.INFO)

    # run the routine
    rc = datasequence(drs4_ped_file, calib_file, time_calib_file, drive_log_file, run_summary_file, run_number)
    sys.exit(rc)
