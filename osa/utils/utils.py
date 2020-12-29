"""
Functions to deal with dates, directories and prod IDs
"""

import hashlib
import logging
import os
import re
from datetime import datetime, timedelta
from fnmatch import fnmatch
from os import getpid, makedirs, readlink, symlink, walk
from os.path import basename, dirname, exists, isdir, isfile, islink, join, split
from socket import gethostname

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import writetofile

log = logging.getLogger(__name__)


def getdate(date, sep):
    """

    Parameters
    ----------
    date
    sep

    Returns
    -------

    """
    if not options.date:
        stringdate = date.replace(cfg.get("LST", "DATESEPARATOR"), sep)
    else:
        stringdate = getcurrentdate2(sep)
        log.debug(f"date is {stringdate}")
    return stringdate


def getcurrentdate2(sep):
    """

    Parameters
    ----------
    sep

    Returns
    -------

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

    Returns
    -------

    """
    log.debug(f"Getting analysis path for tel_id {options.tel_id}")
    nightdir = lstdate_to_dir(options.date)
    options.prod_id = get_prod_id()
    directory = join(cfg.get(options.tel_id, "ANALYSISDIR"), nightdir, options.prod_id)

    if not exists(directory):
        if options.nightsummary and options.tel_id != "ST":
            log.error(f"Night directory {directory} does not exists!")
        elif options.simulate:
            log.warning(f"Directory {directory} does not exists")
        else:
            make_directory(directory)
    log.debug(f"Analysis directory: {directory}")
    return directory


def get_lstchain_version():
    """

    Returns
    -------

    """
    import warnings

    warnings.simplefilter(action="ignore", category=FutureWarning)
    from lstchain.version import get_version

    options.lstchain_version = "v" + get_version()
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


def make_directory(dir):
    """

    Parameters
    ----------
    dir

    Returns
    -------

    """
    if exists(dir):
        if not isdir(dir):
            # oups!! a file instead of a dir?
            log.error(f"{dir} exists but is not a directory")
        else:
            # it is a directory, OK, we could check for access.
            return False
    else:
        try:
            makedirs(dir)
        except IOError as error:
            log.exception(f"Problems creating {dir}, {error}")
        else:
            log.debug(f"Created {dir}")
            return True


def sorted_nicely(l):
    """Sort the given iterable in the way that humans expect.
    Copied from:
    http://stackoverflow.com/questions/2669059/how-to-sort-alpha-numeric-set-in-pytho

    Parameters
    ----------
    l

    Returns
    -------

    """
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]
    return sorted(l, key=alphanum_key)


def getrawdatadays():
    """

    Returns
    -------

    """
    daqdir = cfg.get(options.tel_id, "RAWDIR")
    validset = []
    try:
        dirs = os.listdir(daqdir)
    except OSError as error:
        log.exception(f"{daqdir}, {error}")
    else:
        for element in dirs:
            if isdir(join(daqdir, element)) and fnmatch(
                element, "20[0-9][0-9]_[0-1][0-9]_[0-3][0-9]"
            ):
                # we check for directory naming
                log.debug(f"Found raw dir {element}")
                validset.append(element)
    return set(validset)


def getstereodatadays():
    """

    Returns
    -------

    """
    stereodir = cfg.get(options.tel_id, "ANALYSISDIR")
    validset = set()
    for root, dirs, files in walk(stereodir):
        for element in dirs:
            month = basename(root)
            year = basename(dirname(root))
            if isdir(join(root, element)) and dirname(dirname(root)) == stereodir:
                # we check for directory naming
                log.debug(f"Found stereo dir {element}, {dirname(dirname(root))} == {stereodir}")
                validset.add("_".join([year, month, element]))
            else:
                log.debug(
                    f"Element {element} not a stereo dir {isdir(element)}, {dirname(dirname(root))} != {stereodir}"
                )
    return validset


def getfinisheddays():
    """

    Returns
    -------

    """
    parent_lockdir = cfg.get(options.tel_id, "CLOSERDIR")
    basename = cfg.get("LSTOSA", "ENDOFACTIVITYPREFIX") + cfg.get("LSTOSA", "TEXTSUFFIX")
    validlist = []
    for root, dirs, files in walk(parent_lockdir):
        for f in files:
            if f == basename:
                # got the day
                night_closed = dir_to_lstdate(root)
                validlist.append(night_closed)
                log.debug(f"Closed night {night_closed} found!")
    return set(validlist)


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
                make_directory(dir)
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
    # dir = join(nightdir[0], nightdir[1], nightdir[2])
    dir = "".join(nightdir)
    return dir


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
    make_directory(outputdir)
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

    answer = False
    flag_file = getlockfile()
    if exists(flag_file):
        answer = True
    return answer
