import subprocess
from os.path import exists, isfile, join

from osa.configs import config
from osa.rawcopy import raw
from osa.utils import iofile, options
from osa.utils.standardhandle import error, gettag, stringify, verbose
from osa.utils.utils import build_lstbasename

__all__ = ["buildexternal", "build", "readnightsummary", "getnightsummaryfile"]


def buildexternal(command, rawdir):
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
    tag = gettag()
    commandargs = [command]
    if not raw.arerawfilestransferred():
        # ask for an incomplete night summary
        commandargs.append('-i')
    commandargs.append(rawdir)
    try:
        stdout = subprocess.check_output(commandargs, universal_newlines=True)
    except OSError(ValueError, NameError):
        error(tag, f"Command {stringify(commandargs)}, {NameError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, 2)
    else:
        verbose(tag, "Getting output...\n" + stdout.rstrip())
    return stdout


def build(rawdir):
    """This is a simple ls -1 -> file with a parsing of the file.
    We use first the database approach and the directory listing as
    a failover.
    """

    tag = gettag()

    error(tag, "This function is not yet implemented", 2)


def readnightsummary():
    """Reads the nightsummary txt file.

    For this it either calls the create nightsummary script or simply reads an
    existing nightsummary file.

    Returns
    -------
    stdout : str
        The content of the nightsummary txt file.
    """
    tag = gettag()
    # At the moment we profit from the output of the nightsummary script
    nightsumfile = getnightsummaryfile()
    stdout = None
    options.nightsum = True
    # When executing the closer, 'options.nightsum' is always True
    if not options.nightsum:
        rawdir = raw.get_check_rawdir()
        command = config.cfg.get('LSTOSA', 'NIGHTSUMMARYSCRIPT')
        verbose(tag, "executing command " + command + ' ' + rawdir)
        stdout = buildexternal(command, rawdir)
        if not options.simulate:
            iofile.writetofile(nightsumfile, stdout)
    else:
        if nightsumfile:
            if exists(nightsumfile) and isfile(nightsumfile):
                try:
                    stdout = iofile.readfromfile(nightsumfile)
                except IOError as NameError:
                    error(tag, f"Problems with file {nightsumfile}, {NameError}", 2)
            else:
                error(tag, f"File {nightsumfile} does not exists", 2)
        else:
            error(tag, "No night summary file specified", 2)
    verbose(tag, f"Night Summary file: {nightsumfile}")
    verbose(tag, f"Night Summary:\n{stdout}")
    return stdout


def getnightsummaryfile():
    """Builds the file name of the nightsummary txt file.

    Returns
    -------
    nightsummaryfile : str
        File name of the nightsummary txt file
    """
    tag = gettag()
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        nightsumprefix = config.cfg.get('LSTOSA', 'NIGHTSUMMARYPREFIX')
        nightsumsuffix = config.cfg.get('LSTOSA', 'TEXTSUFFIX')
        nightsumdir = config.cfg.get('LSTOSA', 'NIGHTSUMDIR')
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        nightsummaryfile = join(nightsumdir, basename)
        return nightsummaryfile
    # Only the closer needs the night summary file in case of 'ST'.
    # Since ST has no night summary file, we give him the one from LST1
    elif options.tel_id == 'ST':
        nightsumprefix = config.cfg.get('LSTOSA', 'NIGHTSUMMARYPREFIX')
        nightsumsuffix = config.cfg.get('LSTOSA', 'TEXTSUFFIX')
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        nightsummaryfile = join(options.directory, basename)
        return nightsummaryfile.replace("ST", "LST1")
    return None
