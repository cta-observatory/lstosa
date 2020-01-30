#!/usr/bin/env python2.7
from osa.utils.standardhandle import verbose, warning, error, output, stringify, gettag
from osa.utils import options, cliopts

__all__ = ["buildexternal", "build", "readnightsummary", "getnightsummaryfile"]
##############################################################################
#
# buildexternal
#
##############################################################################
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
    import subprocess
    import raw
    commandargs = [command]
    if not raw.arerawfilestransferred():
        # ask for an incomplete night summary
        commandargs.append('-i')
    commandargs.append(rawdir)
    try:
        stdout = subprocess.check_output(commandargs, universal_newlines=True)
    #except OSError as (ValueError, NameError):
    except OSError as NameError:
        error(tag, "Command {0}, {1}".format(stringify(commandargs), NameError), ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, 2)
    else:
        verbose(tag, "Getting output...\n" + stdout.rstrip())
    return stdout
##############################################################################
#
# build
#
##############################################################################
def build(rawdir):
    tag = gettag()

    """ This is a simple ls -1 -> file with a parsing of the file.
        We use first the database approach and the directory listing as
        a failover. """

    from osa.configs.config import cfg
#    from mysql import select_db
# We could try to make a mysql call similar to this:
#     [analysis@ana7 ~]$ mysql -s -N -uanalysis -hfcana7 -Dmagic_test -e "select DAQ.RUN, DAQ.SUBRUN, 'DATA', DAQ.END, DAQ.SOURCEWOBBLE, -1, -1, -1, -1, -1, 'No_Test', 'No_Moon' FROM DAQ INNER JOIN STORAGE ON STORAGE.FILE_PATH LIKE CONCAT('%',SUBSTRING_INDEX(DAQ.FILE_PATH, '/', -1),'.gz') WHERE STORAGE.NIGHT='2012-12-10' AND STORAGE.TELESCOPE='M1' ORDER BY DAQ.END;" 

    error(tag, "This function is not yet implemented", 2)
##############################################################################
#
# readnightsummary
#
##############################################################################
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
    from os.path import join, exists, isfile
    import iofile
    from osa.configs import config
    from osa.rawcopy import raw
    nightsumfile = getnightsummaryfile()
    stdout = None
    options.nightsum = True
    print('options.nightsum',options.nightsum)
    # When executing the closer, 'options.nightsum' is always True
    if options.nightsum == False:
        rawdir =  raw.get_check_rawdir() 
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
                    error(tag, "Problems with file {0}, {1}".format(nightsumfile, NameError), 2)
            else:
                error(tag, "File {0} does not exists".format(nightsumfile), 2)
        else:
            error(tag, "No night summary file specified", 2)
    verbose(tag, "Night Summary: {0}".format(nightsumfile))
    return stdout
##############################################################################
#
# getnightsummaryfile
#
##############################################################################
def getnightsummaryfile():
    """Builds the file name of the nightsummary txt file.

    Returns
    -------
    nightsummaryfile : str
        File name of the nightsummary txt file
    """
    tag = gettag()
    from os.path import join
    from osa.configs import config
    from osa.utils.utils import build_lstbasename
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        nightsumprefix = config.cfg.get('LSTOSA', 'NIGHTSUMMARYPREFIX')
        nightsumsuffix = config.cfg.get('LSTOSA', 'TEXTSUFFIX')
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        print("BASENAME",basename,options.directory)
        nightsummaryfile = join(options.directory, basename)
        return nightsummaryfile
    # Only the closer needs the night summary file in case of 'ST'.
    # Since ST has no night summary file, we give him the one from M1 
    elif options.tel_id == 'ST':
        nightsumprefix = config.cfg.get('LSTOSA', 'NIGHTSUMMARYPREFIX')
        nightsumsuffix = config.cfg.get('LSTOSA', 'TEXTSUFFIX')
        basename = build_lstbasename(nightsumprefix, nightsumsuffix)
        nightsummaryfile = join(options.directory, basename)
        return nightsummaryfile.replace("ST","LST1")
    return None 

