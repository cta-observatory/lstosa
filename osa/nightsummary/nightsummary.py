"""
It reads the night summary file
"""

import logging
import os
import subprocess
import sys

from astropy.table import Table

from osa.configs.config import cfg
from osa.raw import are_rawfiles_transferred
from osa.utils.utils import lstdate_to_dir, stringify

__all__ = ["build_external", "get_runsummary_file", "run_summary_table"]

log = logging.getLogger(__name__)


def build_external(command, rawdir):
    """
    Calls the create run summary script.

    Parameters
    ----------
    command : str
        The command passed on to the subprocess call
    rawdir : str
        Passed on as argument to the command str.

    Returns
    -------
    stdout : str
        The output of the create nightsummary script.
    """
    commandargs = [command]
    if not are_rawfiles_transferred():
        # ask for an incomplete night summary
        commandargs.append("-i")
    commandargs.append(rawdir)
    try:
        stdout = subprocess.check_output(
            commandargs,
            universal_newlines=True,
            shell=False
        )
    except OSError as error:
        log.exception(f"Command {stringify(commandargs)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")
    else:
        log.debug("Getting output...\n" + stdout.rstrip())
        return stdout


def run_summary_table(date):
    """
    Reads the run summary ECSV file containing an astropy Table with the following content.
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

    It reads an existing RunSummary file.

    Returns
    -------
    table : astropy.Table
        Table with the content of the run summary ECSV file.
    """

    # options.nightsummary = True
    # when executing the closer, 'options.nightsummary' is always True
    # if not options.nightsummary:
    #    # TODO: Run summary script should be launched here
    #    rawdir = get_check_rawdir()
    #    # TODO: Add this parameter to the cfg
    #    command = config.cfg.get("lstchain", "run_summary_script")
    #    log.debug("Executing command " + command)
    #    if not options.simulate:
    #        sp.run(command, rawdir)
    # else:

    nightsummary_file = get_runsummary_file(date)
    log.debug(f"Looking for run summary file {nightsummary_file}")
    if not os.path.isfile(nightsummary_file):
        log.error(f"Run summary file {nightsummary_file} not found")
        sys.exit(1)

    table = Table.read(nightsummary_file)
    table.add_index(["run_id"])
    return table


def get_runsummary_file(date):
    """
    Builds the file name of the run summary ECSV file.

    Returns
    -------
    runsummary_file : str
        File name of the run summary ECSV file
    """
    nightdir = lstdate_to_dir(date)
    return os.path.join(cfg.get("LST1", "RUN_SUMMARY_DIR"), f"RunSummary_{nightdir}.ecsv")
