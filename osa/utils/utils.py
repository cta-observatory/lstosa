from . import options
from .standardhandle import output, warning, verbose, error, gettag, errornonfatal


def getdate(date, sep):
    tag = gettag()
    from osa.configs.config import cfg
    if not options.date:
        stringdate = date.replace(cfg.get('LST', 'DATESEPARATOR'), sep)
    else:
        stringdate = getcurrentdate2(sep)
        verbose(tag, 'date is ' + stringdate)
    return stringdate


def getcurrentdate2(sep):
    tag = gettag()
    from datetime import datetime, timedelta
    from osa.configs.config import cfg
    import sys
    limitnight = int(cfg.get('LST', 'NIGHTOFFSET'))
    now = datetime.utcnow()
    if (now.hour >= limitnight >= 0) or (now.hour < limitnight + 24 and limitnight < 0):
        # Today, nothing to do
        pass
    elif now.hour < limitnight and limitnight >= 0:
        # Yesterday
        gap = timedelta(hours=24)
        now = now - gap
    elif now.hour >= limitnight + 24 and limitnight < 0:
        # Tomorrow
        gap = timedelta(hours=24)
        now = now + gap
    else:
        error(tag, "ERROR: NIGHTOFFSET should be between range (-24, 24)\n")
        sys.exit(4)
    stringdate = now.strftime("%Y" + sep + "%m" + sep + "%d")
    verbose(tag, "stringdate by default " + stringdate)
    return stringdate


def getnightdirectory():
    tag = gettag()
    from os.path import join, exists
    from osa.configs.config import cfg

    verbose(tag, f"Getting analysis path for tel_id {options.tel_id}")
    nightdir = lstdate_to_dir(options.date)

    if not options.prod_id:
        import warnings
        warnings.simplefilter(action='ignore', category=FutureWarning)
        from lstchain.version import get_version
        options.lstchain_version = 'v' + get_version()
        options.prod_id = options.lstchain_version + '_' + cfg.get('LST1', 'VERSION')

    directory = join(
        cfg.get(options.tel_id, 'ANALYSISDIR'),
        nightdir, options.prod_id
    )

    if not exists(directory):
        if options.nightsum and options.tel_id != 'ST':
            error(tag, f"Night directory {directory} does not exists!", 2)
        elif options.simulate:
            warning(tag, f"directory {directory} does not exists")
        else:
            make_directory(directory)
    verbose(tag, f"Analysis directory: {directory}")
    return directory


def make_directory(dir):
    tag = gettag()
    from os import makedirs
    from os.path import exists, isdir
    if exists(dir):
        if not isdir(dir):
            # Ups!! a file instead of a dir?
            error(tag, f"{dir} exists but is not a directory", 2)
        else:
            # It is a directory, OK, we could check for access.
            return False
    else:
        try:
            makedirs(dir)
        except IOError:
            error(tag, f"Problems creating {dir}, {IOError}", 2)
        # except OSError as (ValueError, NameError):
        except OSError:
            error(tag, f"Problems creating {dir}, {OSError}", ValueError)
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
    tag = gettag()
    """ """
    import re
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


def getrawdatadays():
    tag = gettag()
    import os
    from os.path import isdir, join
    from fnmatch import fnmatch
    from osa.configs.config import cfg
    daqdir = cfg.get(options.tel_id, 'RAWDIR')
    validset = []
    try:
        dirs = os.listdir(daqdir)
    # except OSError as (ValueError, NameError):
    except OSError:
        error(tag, f"{daqdir}, {OSError}", ValueError)
    else:
        for element in dirs:
            if isdir(join(daqdir, element)) and fnmatch(element, '20[0-9][0-9]_[0-1][0-9]_[0-3][0-9]'):
                # We check for directory naming
                verbose(tag, f"Found raw dir {element}")
                validset.append(element)
    return set(validset)


def getstereodatadays():
    tag = gettag()

    from os.path import isdir, join, dirname, basename
    from os import walk
    from osa.configs.config import cfg
    stereodir = cfg.get(options.tel_id, 'ANALYSISDIR')
    validset = set()
    for root, dirs, files in walk(stereodir):
        for element in dirs:
            month = basename(root)
            year = basename(dirname(root))
            if isdir(join(root, element)) and dirname(dirname(root)) == stereodir:
                # We check for directory naming
                verbose(
                    tag,
                    f"Found stereo dir {element}, {dirname(dirname(root))} == {stereodir}"
                )
                validset.add('_'.join([year, month, element]))
            else:
                verbose(
                    tag,
                    f"Element {element} not a stereo dir {isdir(element)}, {dirname(dirname(root))} != {stereodir}"
                )
    return validset


def getfinisheddays():
    tag = gettag()
    from os import walk
    from osa.configs.config import cfg
    parent_lockdir = cfg.get(options.tel_id, 'CLOSERDIR')
    basename = cfg.get('LSTOSA', 'ENDOFACTIVITYPREFIX') + cfg.get('LSTOSA', 'TEXTSUFFIX')
    validlist = []
    for root, dirs, files in walk(parent_lockdir):
        for f in files:
            if f == basename:
                # Got the day
                night_closed = dir_to_lstdate(root)
                validlist.append(night_closed)
                verbose(tag, f"Closed night {night_closed} found!")
    return set(validlist)


def createlock(lockfile, content):
    tag = gettag()
    from os import getpid
    from os.path import isfile, isdir, exists, dirname
    from socket import gethostname
    from iofile import writetofile
    dir = dirname(lockfile)
    if options.simulate:
        verbose(tag, f"SIMULATE Creation of lock file {lockfile}")
        return True
    else:
        if exists(lockfile) and isfile(lockfile):
            with open(lockfile, 'r') as f:
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
    from os.path import join
    from osa.configs.config import cfg
    basename = cfg.get('LSTOSA', 'ENDOFACTIVITYPREFIX') + cfg.get('LSTOSA', 'TEXTSUFFIX')
    dir = join(cfg.get(options.tel_id, 'CLOSERDIR'), lstdate_to_dir(options.date), options.prod_id)
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
    tag = gettag()
    from osa.configs.config import cfg
    sepbar = ''
    numberdate = night.replace(cfg.get('LST', 'DATESEPARATOR'), sepbar)
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
    tag = gettag()
    from osa.configs.config import cfg
    sepbar = '-'
    isodate = night.replace(cfg.get('LST', 'DATESEPARATOR'), sepbar)
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
    from osa.configs.config import cfg
    nightdir = night.split(cfg.get('LST', 'DATESEPARATOR'))
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
    from os.path import split
    from osa.configs.config import cfg
    sep = cfg.get('LST', 'DATESEPARATOR')
    dircopy = dir
    nightdir = ['YYYY', 'MM', 'DD']
    for i in reversed(range(3)):
        dircopy, nightdir[i] = split(dircopy)
    night = sep.join(nightdir)

    if len(night) != 10:
        error(tag, f"Error: night {night} could not be created from {dir}\n", 1)
    return night


def build_lstbasename(prefix, suffix):
    tag = gettag()
    basename = f"{prefix}_{lstdate_to_number(options.date)}{suffix}"
    return basename


def is_defined(variable):
    tag = gettag()
    try:
        variable
    except NameError:
        variable = None

    if variable is not None:
        return True
    else:
        return False


def get_night_limit_timestamp():
    tag = gettag()
    from mysql import select_db
    from osa.configs.config import cfg
    night_limit = None
    server = cfg.get('MYSQL', 'server')
    user = cfg.get('MYSQL', 'user')
    database = cfg.get('MYSQL', 'database')
    table = cfg.get('MYSQL', 'nighttimes')
    night = lstdate_to_iso(options.date)
    selections = ['END']
    conditions = {'NIGHT': night}
    matrix = select_db(server, user, database, table, selections, conditions)
    if len(matrix) > 0:
        night_limit = matrix[0][0]
    else:
        errornonfatal(tag, "No night_limit found")
    verbose(tag, f"Night limit is {night_limit}")
    return night_limit


def get_md5sum_and_copy(inputf, outputf):
    tag = gettag()

    from os.path import dirname, islink
    from os import readlink, symlink
    import hashlib

    md5 = hashlib.md5()
    outputdir = dirname(outputf)
    make_directory(outputdir)
    # In case of being a link we just move it
    if islink(inputf):
        linkto = readlink(inputf)
        symlink(linkto, outputf)
        return None
    else:
        try:
            with open(inputf, 'rb') as f, open(outputf, 'wb') as o:
                block_size = 8192
                for chunk in iter(lambda: f.read(128 * block_size), b''):
                    md5.update(chunk)
                    # Got this error: write() argument must be str, not bytes
                    o.write(chunk)
        # except IOError as (ErrorValue, ErrorName):
        except IOError as ErrorValue:
            error(tag, f"{ErrorValue}")
        else:
            return md5.hexdigest()
