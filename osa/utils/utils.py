import hashlib
import os
import re
import sys
from datetime import datetime, timedelta
from fnmatch import fnmatch
from os import getpid, makedirs, readlink, symlink, walk
from os.path import basename, dirname, exists, isdir, isfile, islink, join, split
from socket import gethostname

from osa.configs.config import cfg
from osa.utils.iofile import writetofile

from . import options
from .standardhandle import error, errornonfatal, gettag, verbose, warning


def getdate(date, sep):
    tag = gettag()
    if not options.date:
        stringdate = date.replace(cfg.get("LST", "DATESEPARATOR"), sep)
    else:
        stringdate = getcurrentdate2(sep)
        verbose(tag, f"date is {stringdate}")
    return stringdate


def getcurrentdate2(sep):
    tag = gettag()
    limitnight = int(cfg.get("LST", "NIGHTOFFSET"))
    now = datetime.utcnow()
    if (now.hour >= limitnight >= 0) or (now.hour < limitnight + 24 and limitnight < 0):
        # today, nothing to do
        pass
    elif now.hour < limitnight and limitnight >= 0:
        # yesterday
        gap = timedelta(hours=24)
        now = now - gap
    elif now.hour >= limitnight + 24 and limitnight < 0:
        # tomorrow
        gap = timedelta(hours=24)
        now = now + gap
    else:
        error(tag, "ERROR: NIGHTOFFSET should be between range (-24, 24)\n", 2)
        sys.exit(4)
    stringdate = now.strftime("%Y" + sep + "%m" + sep + "%d")
    verbose(tag, f"stringdate by default {stringdate}")
    return stringdate


def getnightdirectory():
    tag = gettag()
    verbose(tag, f"Getting analysis path for tel_id {options.tel_id}")
    nightdir = lstdate_to_dir(options.date)

    if not options.prod_id:
        import warnings

        warnings.simplefilter(action="ignore", category=FutureWarning)
        from lstchain.version import get_version

        #options.lstchain_version = "v" + get_version()
        #options.prod_id = options.lstchain_version + "_" + cfg.get("LST1", "VERSION")
        options.prod_id = cfg.get("LST1", "DL1-PROD-ID")

    directory = join(cfg.get(options.tel_id, "ANALYSISDIR"), nightdir, options.prod_id)

    if not exists(directory):
        if options.nightsum and options.tel_id != "ST":
            error(tag, f"Night directory {directory} does not exists!", 2)
        elif options.simulate:
            warning(tag, f"directory {directory} does not exists")
        else:
            make_directory(directory)
    verbose(tag, f"Analysis directory: {directory}")
    return directory


def make_directory(dir):
    tag = gettag()
    if exists(dir):
        if not isdir(dir):
            # oups!! a file instead of a dir?
            error(tag, f"{dir} exists but is not a directory", 2)
        else:
            # it is a directory, OK, we could check for access.
            return False
    else:
        try:
            makedirs(dir)
        except IOError:
            error(tag, f"Problems creating {dir}, {IOError}", 2)
        else:
            verbose(tag, f"Created {dir}")
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
    tag = gettag()
    daqdir = cfg.get(options.tel_id, "RAWDIR")
    validset = []
    try:
        dirs = os.listdir(daqdir)
    # except OSError as (ValueError, NameError):
    except OSError:
        error(tag, f"{daqdir}, {OSError}", OSError)
    else:
        for element in dirs:
            if isdir(join(daqdir, element)) and fnmatch(element, "20[0-9][0-9]_[0-1][0-9]_[0-3][0-9]"):
                # we check for directory naming
                verbose(tag, f"Found raw dir {element}")
                validset.append(element)
    return set(validset)


def getstereodatadays():
    tag = gettag()
    stereodir = cfg.get(options.tel_id, "ANALYSISDIR")
    validset = set()
    for root, dirs, files in walk(stereodir):
        for element in dirs:
            month = basename(root)
            year = basename(dirname(root))
            if isdir(join(root, element)) and dirname(dirname(root)) == stereodir:
                # we check for directory naming
                verbose(tag, f"Found stereo dir {element}, {dirname(dirname(root))} == {stereodir}")
                validset.add("_".join([year, month, element]))
            else:
                verbose(tag, f"Element {element} not a stereo dir {isdir(element)}, {dirname(dirname(root))} != {stereodir}")
    return validset


def getfinisheddays():
    tag = gettag()
    parent_lockdir = cfg.get(options.tel_id, "CLOSERDIR")
    basename = cfg.get("LSTOSA", "ENDOFACTIVITYPREFIX") + cfg.get("LSTOSA", "TEXTSUFFIX")
    validlist = []
    for root, dirs, files in walk(parent_lockdir):
        for f in files:
            if f == basename:
                # got the day
                night_closed = dir_to_lstdate(root)
                validlist.append(night_closed)
                verbose(tag, f"Closed night {night_closed} found!")
    return set(validlist)


def createlock(lockfile, content):
    tag = gettag()
    dir = dirname(lockfile)
    if options.simulate:
        verbose(tag, f"SIMULATE Creation of lock file {lockfile}")
        return True
    else:
        if exists(lockfile) and isfile(lockfile):
            with open(lockfile, "r") as f:
                hostpid = f.readline()
            error(tag, f"Lock by a previous process {hostpid}, exiting!\n", 4)
        else:
            if not exists(dir):
                make_directory(dir)
                verbose(tag, f"Creating parent directory {dir} for lock file")
            if isdir(dir):
                pid = str(getpid())
                hostname = gethostname()
                content = f"{hostname}:{pid}"
                writetofile(lockfile, content)
                verbose(tag, f"Lock file {lockfile} created")
                return True
            else:
                error(tag, f"Expecting {dir} to be a directory, not a file", 3)


def getlockfile():
    tag = gettag()
    basename = cfg.get("LSTOSA", "ENDOFACTIVITYPREFIX") + cfg.get("LSTOSA", "TEXTSUFFIX")
    dir = join(cfg.get(options.tel_id, "CLOSERDIR"), lstdate_to_dir(options.date), options.prod_id)
    lockfile = join(dir, basename)
    verbose(tag, f"Lock file is {lockfile}")
    return lockfile


def lstdate_to_number(night):
    """Function to change from YYYY_MM_DD to YYYYMMDD
    The iso standard separates year, month and day by a minus sign

    Parameters
    ----------
    night

    Returns
    -------

    """
    sepbar = ""
    numberdate = night.replace(cfg.get("LST", "DATESEPARATOR"), sepbar)
    return numberdate


def lstdate_to_iso(night):
    """Function to change from YYYY_MM_DD to YYYY-MM-DD
    The iso standard separates year, month and day by a minus sign

    Parameters
    ----------
    night: Date in YYYY_MM_DD format

    Returns
    -------
    Date in iso format YYYY-MM-DD
    """
    sepbar = "-"
    isodate = night.replace(cfg.get("LST", "DATESEPARATOR"), sepbar)
    return isodate


def lstdate_to_dir(night):
    """Function to change from YYYY_MM_DD to YYYY/MM/DD

    Parameters
    ----------
    night

    Returns
    -------

    """
    tag = gettag()
    nightdir = night.split(cfg.get("LST", "DATESEPARATOR"))
    if len(nightdir) != 3:
        error(tag, f"Error: night directory structure could not be created from {nightdir}\n", 1)
    # dir = join(nightdir[0], nightdir[1], nightdir[2])
    dir = "".join(nightdir)
    return dir


def dir_to_lstdate(dir):
    """Function to change from WHATEVER/YYYY/MM/DD to YYYY_MM_DD

    Parameters
    ----------
    dir

    Returns
    -------

    """
    tag = gettag()
    sep = cfg.get("LST", "DATESEPARATOR")
    dircopy = dir
    nightdir = ["YYYY", "MM", "DD"]
    for i in reversed(range(3)):
        dircopy, nightdir[i] = split(dircopy)
    night = sep.join(nightdir)
    if len(night) != 10:
        error(tag, f"Error: night {night} could not be created from {dir}\n", 1)
    return night


def build_lstbasename(prefix, suffix):
    basename = f"{prefix}_{lstdate_to_number(options.date)}{suffix}"
    return basename


def is_defined(variable):
    try:
        variable
    except NameError:
        variable = None
    if variable is not None:
        return True
    else:
        return False


def get_night_limit_timestamp():
    from dev.mysql import select_db

    tag = gettag()
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
        errornonfatal(tag, "No night_limit found")
    verbose(tag, f"Night limit is {night_limit}")
    return night_limit


def get_md5sum_and_copy(inputf, outputf):
    tag = gettag()
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
        except IOError as ErrorValue:
            error(tag, f"{ErrorValue}", 2)
        else:
            return md5.hexdigest()
