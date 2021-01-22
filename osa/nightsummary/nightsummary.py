"""
It reads the night summary file
"""

import logging
import subprocess
import sys
from os.path import exists, isfile, join

from osa.configs import config, options
from osa.rawcopy.raw import are_rawfiles_transferred, get_check_rawdir
from osa.utils.iofile import readfromfile, writetofile
from osa.utils.standardhandle import stringify
from osa.utils.utils import build_lstbasename

__all__ = ["build_external", "read_nightsummary", "get_nightsummary_file"]

log = logging.getLogger(__name__)


def build_external(command, rawdir):
    """Calls the create nightsummary script.

    This is usually the `create_nightsummary.py` file.

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
        stdout = subprocess.check_output(commandargs, universal_newlines=True)
    except OSError as error:
        log.exception(f"Command {stringify(commandargs)}, error: {error}")
    except subprocess.CalledProcessError as error:
        log.exception(f"Subprocess error: {error}")
    else:
        log.debug("Getting output...\n" + stdout.rstrip())
    return stdout


def read_nightsummary():
    """
    Reads the nightsummary txt file. It either calls the create nightsummary
    script or simply reads an existing nightsummary file.

    Returns
    -------
    stdout : str
        The content of the nightsummary txt file.
    """

    nightsummary_file = get_nightsummary_file()
    stdout = None
    options.nightsummary = True
    # when executing the closer, 'options.nightsummary' is always True
    if not options.nightsummary:
        rawdir = get_check_rawdir()
        command = config.cfg.get("LSTOSA", "NIGHTSUMMARYSCRIPT")
        log.debug("Executing command " + command + " " + rawdir)
        stdout = build_external(command, rawdir)
        if not options.simulate:
            writetofile(nightsummary_file, stdout)
    else:
        if nightsummary_file:
            if exists(nightsummary_file) and isfile(nightsummary_file):
                try:
                    stdout = readfromfile(nightsummary_file)
                except IOError as err:
                    log.exception(f"Problems with file {nightsummary_file}, {err}")
            else:
                log.error(f"File {nightsummary_file} does not exists")
                sys.exit(1)
        else:
            log.error("No night summary file specified")
    log.debug(f"Night summary file path {nightsummary_file}")
    log.debug(f"Content \n{stdout}")
    return stdout


def get_nightsummary_file():
    """
    Builds the file name of the night summary txt file.

    Returns
    -------
    nightsummary_file : str
        File name of the night summary txt file
    """
    if options.tel_id == "LST1" or options.tel_id == "LST2":
        nightsumprefix = config.cfg.get("LSTOSA", "NIGHTSUMMARYPREFIX")
        nightsumsuffix = config.cfg.get("LSTOSA", "TEXTSUFFIX")
        nightsumdir = config.cfg.get("LSTOSA", "NIGHTSUMDIR")
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        nightsummary_file = join(nightsumdir, basename)
        return nightsummary_file
    # only the closer needs the night summary file in case of 'ST'.
    # since ST has no night summary file, we give him the one from LST1
    elif options.tel_id == "ST":
        nightsumprefix = config.cfg.get("LSTOSA", "NIGHTSUMMARYPREFIX")
        nightsumsuffix = config.cfg.get("LSTOSA", "TEXTSUFFIX")
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        nightsummary_file = join(options.directory, basename)
        return nightsummary_file.replace("ST", "LST1")
    return None
