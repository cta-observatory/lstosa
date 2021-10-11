"""
Functions to deal with dates, directories and prod IDs
"""

import hashlib
import logging
import os
import subprocess
from datetime import datetime, timedelta
from os import getpid, readlink, symlink
from os.path import dirname, exists, isdir, isfile, islink, join, split
from pathlib import Path
from socket import gethostname

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import writetofile

__all__ = [
    "getcurrentdate",
    "getnightdirectory",
    "get_lstchain_version",
    "create_directories_datacheck_web",
    "set_no_observations_flag",
    "copy_files_datacheck_web",
    "lstdate_to_dir",
    "is_day_closed",
    "get_prod_id",
    "date_in_yymmdd",
    "destination_dir",
    "time_to_seconds"
]

log = logging.getLogger(__name__)

DATACHECK_PRODUCTS = ["drs4", "enf_calibration", "dl1"]
DATACHECK_BASEDIR = Path(cfg.get("WEBSERVER", "DATACHECK"))


def getcurrentdate(sep):
    """
    Get current data following LST data-taking convention in which the date
    changes at 12:00 pm instead of 00:00 or 12:00 am to cover a natural
    data-taking night. This why a night offset is taken into account.

    Parameters
    ----------
    sep: string
        Separator

    Returns
    -------
    stringdate: string
        Date in string format using the given separator

    """
    limitnight = int(cfg.get("LST", "NIGHTOFFSET"))
    now = datetime.utcnow()
    if (now.hour >= limitnight >= 0) or (now.hour < limitnight + 24 and limitnight < 0):
        # today, nothing to do
        pass
    elif limitnight >= 0:
        # yesterday
        gap = timedelta(hours=24)
        now = now - gap
    else:
        # tomorrow
        gap = timedelta(hours=24)
        now = now + gap
    stringdate = now.strftime("%Y" + sep + "%m" + sep + "%d")
    log.debug(f"stringdate by default {stringdate}")
    return stringdate


def getnightdirectory():
    """
    Get the path of the running_analysis directory for a certain night

    Returns
    -------
    directory
        Path of the running_analysis directory for a certain night

    """
    log.debug(f"Getting analysis path for tel_id {options.tel_id}")
    nightdir = lstdate_to_dir(options.date)
    options.prod_id = get_prod_id()
    directory = join(cfg.get(options.tel_id, "ANALYSISDIR"), nightdir, options.prod_id)

    if not exists(directory):
        if options.nightsummary and options.tel_id != "ST":
            log.error(f"Analysis directory {directory} does not exists!")
        elif options.simulate:
            log.debug("SIMULATE the creation of the analysis directory.")
        else:
            os.makedirs(directory, exist_ok=True)
    log.debug(f"Analysis directory: {directory}")
    return directory


def get_lstchain_version():
    """

    Returns
    -------

    """
    from lstchain import __version__

    options.lstchain_version = "v" + __version__
    return options.lstchain_version


def get_prod_id():
    """

    Returns
    -------

    """
    if not options.prod_id:
        if cfg.get("LST1", "PROD-ID") is not None:
            options.prod_id = cfg.get("LST1", "PROD-ID")
        else:
            options.prod_id = get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")

    log.debug(f"Getting the prod ID for the running analysis directory: {options.prod_id}")

    return options.prod_id


def get_calib_prod_id():
    """

    Returns
    -------

    """
    if not options.calib_prod_id:
        if cfg.get("LST1", "CALIB-PROD-ID") is not None:
            options.calib_prod_id = cfg.get("LST1", "CALIB-PROD-ID")
        else:
            options.calib_prod_id = get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")

    log.debug(f"Getting prod ID for calibration products: {options.calib_prod_id}")

    return options.calib_prod_id


def get_dl1_prod_id():
    """

    Returns
    -------

    """
    if not options.dl1_prod_id:
        if cfg.get("LST1", "DL1-PROD-ID") is not None:
            options.dl1_prod_id = cfg.get("LST1", "DL1-PROD-ID")
        else:
            options.dl1_prod_id = get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")

    log.debug(f"Getting prod ID for DL1 products: {options.dl1_prod_id}")

    return options.dl1_prod_id


def get_dl2_prod_id():
    """

    Returns
    -------

    """
    if not options.dl2_prod_id:
        if cfg.get("LST1", "DL2-PROD-ID") is not None:
            options.dl2_prod_id = cfg.get("LST1", "DL2-PROD-ID")
        else:
            options.dl2_prod_id = get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")

    log.debug(f"Getting prod ID for DL2 products: {options.dl2_prod_id}")

    return options.dl2_prod_id


def createlock(lockfile, content):
    """

    Parameters
    ----------
    lockfile
    content

    Returns
    -------

    """
    dir = dirname(lockfile)
    if options.simulate:
        log.debug(f"SIMULATE Creation of lock file {lockfile}")
        return True
    else:
        if exists(lockfile) and isfile(lockfile):
            with open(lockfile, "r") as f:
                hostpid = f.readline()
            log.error(f"Lock by a previous process {hostpid}, exiting!\n")
        else:
            if not exists(dir):
                os.makedirs(dir, exist_ok=True)
                log.debug(f"Creating parent directory {dir} for lock file")
            if isdir(dir):
                pid = str(getpid())
                hostname = gethostname()
                content = f"{hostname}:{pid}"
                writetofile(lockfile, content)
                log.debug(f"Lock file {lockfile} created")
                return True
            else:
                log.error(f"Expecting {dir} to be a directory, not a file")


def getlockfile():
    """

    Returns
    -------

    """
    basename = cfg.get("LSTOSA", "ENDOFACTIVITYPREFIX") + cfg.get("LSTOSA", "TEXTSUFFIX")
    dir = join(
        cfg.get(options.tel_id, "CLOSERDIR"),
        lstdate_to_dir(options.date),
        options.prod_id,
    )
    lockfile = join(dir, basename)
    log.debug(f"Lock file is {lockfile}")
    return lockfile


def lstdate_to_number(night):
    """
    Function to change from YYYY_MM_DD to YYYYMMDD

    Parameters
    ----------
    night

    Returns
    -------

    """
    sepbar = ""
    return night.replace(cfg.get("LST", "DATESEPARATOR"), sepbar)


def lstdate_to_iso(night):
    """
    Function to change from YYYY_MM_DD to YYYY-MM-DD

    Parameters
    ----------
    night: Date in YYYY_MM_DD format

    Returns
    -------
    Date in iso format YYYY-MM-DD
    """
    sepbar = "-"
    return night.replace(cfg.get("LST", "DATESEPARATOR"), sepbar)


def lstdate_to_dir(night):
    """Function to change from YYYY_MM_DD to YYYY/MM/DD

    Parameters
    ----------
    night

    Returns
    -------

    """
    nightdir = night.split(cfg.get("LST", "DATESEPARATOR"))
    if len(nightdir) != 3:
        log.error(f"Night directory structure could not be created from {nightdir}")
    return "".join(nightdir)


def dir_to_lstdate(dir):
    """
    Function to change from WHATEVER/YYYY/MM/DD to YYYY_MM_DD

    Parameters
    ----------
    dir

    Returns
    -------

    """
    sep = cfg.get("LST", "DATESEPARATOR")
    dircopy = dir
    nightdir = ["YYYY", "MM", "DD"]
    for i in reversed(range(3)):
        dircopy, nightdir[i] = split(dircopy)
    night = sep.join(nightdir)
    if len(night) != 10:
        log.error(f"Error: night {night} could not be created from {dir}\n")
    return night


def build_lstbasename(prefix, suffix):
    """

    Parameters
    ----------
    prefix
    suffix

    Returns
    -------

    """
    return f"{prefix}_{lstdate_to_number(options.date)}{suffix}"


def is_defined(variable):
    """

    Parameters
    ----------
    variable

    Returns
    -------

    """
    try:
        variable
    except NameError:
        variable = None
    return variable is not None


def get_night_limit_timestamp():
    """

    Returns
    -------

    """
    from dev.mysql import select_db

    night_limit = None
    server = cfg.get("MYSQL", "server")
    user = cfg.get("MYSQL", "user")
    database = cfg.get("MYSQL", "database")
    table = cfg.get("MYSQL", "nighttimes")
    night = lstdate_to_iso(options.date)
    selections = ["END"]
    conditions = {"NIGHT": night}
    matrix = select_db(server, user, database, table, selections, conditions)
    if len(matrix) > 0:
        night_limit = matrix[0][0]
    else:
        log.warning("No night_limit found")
    log.debug(f"Night limit is {night_limit}")
    return night_limit


def get_md5sum_and_copy(inputf, outputf):
    """

    Parameters
    ----------
    inputf
    outputf

    Returns
    -------

    """
    md5 = hashlib.md5()
    outputdir = dirname(outputf)
    os.makedirs(outputdir, exist_ok=True)
    # in case of being a link we just move it
    if islink(inputf):
        linkto = readlink(inputf)
        symlink(linkto, outputf)
        return None
    else:
        try:
            with open(inputf, "rb") as f, open(outputf, "wb") as o:
                block_size = 8192
                for chunk in iter(lambda: f.read(128 * block_size), b""):
                    md5.update(chunk)
                    # got this error: write() argument must be str, not bytes
                    o.write(chunk)
        # except IOError as (ErrorValue, ErrorName):
        except IOError as error:
            log.exception(f"{error}", 2)
        else:
            return md5.hexdigest()


def is_day_closed():
    """Get the name and Check for the existence of the Closer flag file."""
    flag_file = getlockfile()
    return bool(exists(flag_file))


def time_to_seconds(timestring):
    """Transform HH:MM:SS time format to seconds.

    Parameters
    ----------
    timestring: str
        Time in format HH:MM:SS

    Returns
    -------
    Seconds that correspond to HH:MM:SS

    """
    if timestring is None:
        timestring = "00:00:00"
    hours, minutes, seconds = timestring.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def date_in_yymmdd(datestring):
    """
    Convert date string YYYYMMDD into YY_MM_DD format to be used for
    drive log file names.

    Parameters
    ----------
    datestring: in format YYYYMMDD

    Returns
    -------
    yy_mm_dd: datestring in format YY_MM_DD

    """
    date = list(datestring)
    yy = "".join(date[2:4])
    mm = "".join(date[4:6])
    dd = "".join(date[6:8])
    return f"{yy}_{mm}_{dd}"


def destination_dir(concept, create_dir=True):
    """
    Create final destination directory for each data level.
    See Also osa.utils.register_run_concept_files

    Parameters
    ----------
    concept : str
        Expected: MUON, DL1AB, DATACHECK, DL2, PEDESTAL, CALIB, TIMECALIB
    create_dir : bool
        Set it to True (default) if you want to create the directory.
        Otherwise it just return the path

    """
    nightdir = lstdate_to_dir(options.date)

    if concept == "MUON":
        directory = os.path.join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id
        )
    elif concept in ["DL1AB", "DATACHECK"]:
        directory = os.path.join(
            cfg.get(options.tel_id, concept + "DIR"),
            nightdir,
            options.prod_id,
            options.dl1_prod_id,
        )
    elif concept == "DL2":
        directory = os.path.join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id, options.dl2_prod_id
        )
    elif concept in ["PEDESTAL", "CALIB", "TIMECALIB"]:
        directory = os.path.join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.calib_prod_id
        )
    else:
        log.warning(f"Concept {concept} not known")

    if not options.simulate and create_dir:
        log.debug(f"Destination directory created for {concept}: {directory}")
        os.makedirs(directory, exist_ok=True)
    else:
        log.debug(f"SIMULATING creation of final directory for {concept}")
    return directory


def create_directories_datacheck_web(host, datedir, prod_id):
    """Create directories for drs4, enf_calibration
    and dl1 products in the data-check webserver via ssh. It also copies
    the index.php file needed to build the directory tree structure.

    Parameters
    ----------
    host
    datedir
    prod_id
    """

    # Create directory and copy the index.php to each directory
    for product in DATACHECK_PRODUCTS:
        destination_dir = DATACHECK_BASEDIR / product / prod_id / datedir
        cmd = ["ssh", host, "mkdir", "-p", destination_dir]
        subprocess.run(cmd, capture_output=True)
        cmd = ["scp", cfg.get("WEBSERVER", "INDEXPHP"), f"{host}:{destination_dir}/."]
        subprocess.run(cmd, capture_output=True)


def set_no_observations_flag(host, datedir, prod_id):
    """Create a file indicating that are no observations
    on a given date in the data-check webserver.

    Parameters
    ----------
    host
    datedir
    prod_id
    """

    for product in DATACHECK_PRODUCTS:
        try:
            # Check if destination directory exists, otherwise create it
            destination_dir = DATACHECK_BASEDIR / product / prod_id / datedir
            no_observations_flag = destination_dir / "no_observations"
            cmd = ["ssh", host, "touch", no_observations_flag]
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            log.warning(f"Destination directory does not exists. {e}")


def copy_files_datacheck_web(host, datedir, file_list):
    """

    Parameters
    ----------
    host
    datedir
    file_list
    """
    # FIXME: Check if files exists already at webserver CHECK HASH
    # Copy files to server
    for file_to_transfer in file_list:
        if "drs4" in str(file_to_transfer):
            destination_dir = DATACHECK_BASEDIR / "drs4" / options.prod_id / datedir
            cmd = ["scp", str(file_to_transfer), f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "calibration" in str(file_to_transfer):
            destination_dir = DATACHECK_BASEDIR / "enf_calibration" / options.prod_id / datedir
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "datacheck" in str(file_to_transfer):
            destination_dir = DATACHECK_BASEDIR / "dl1" / options.prod_id / datedir
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)
