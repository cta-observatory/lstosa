from .standardhandle import output, warning, verbose, error, gettag
from . import options, cliopts 
##############################################################################
#
# getdate
#
##############################################################################
def getdate(date, sep):
    tag = gettag()
    from osa.configs.config import cfg
    stringdate = None
    if not options.date:
        stringdate = date.replace(cfg.get('LST', 'DATESEPARATOR'), sep)
    else:
        stringdate = getcurrentdate2(sep)
        verbose(tag, 'date is ' + stringdate)
    return stringdate
##############################################################################
#
# getcurrentdate2
#
##############################################################################
def getcurrentdate2(sep):
    tag = gettag()
    from datetime import datetime, timedelta
    from osa.configs.config import cfg
    limitnight = int(cfg.get('LST', 'NIGHTOFFSET'))
    now =  datetime.utcnow()
    if ( now.hour >= limitnight and limitnight >= 0 ) or (  now.hour < limitnight + 24 and limitnight < 0 ):
    # Today, nothing to do
       pass
    elif  now.hour < limitnight and limitnight >= 0:
    # Yesterday
       gap = timedelta(hours = 24)
       now = now - gap
    elif now.hour >= limitnight + 24 and limitnight < 0:
    # Tomorrow
       gap = timedelta(hours = 24)
       now = now + gap
    else:
       error(tag, "ERROR: NIGHTOFFSET should be between range (-24, 24)\n")
       sys.exit(4)
    stringdate = now.strftime("%Y" + sep + "%m" + sep + "%d")
    verbose(tag, "stringdate by default " + stringdate)
    return stringdate
##############################################################################
#
# getnightdirectory
#
##############################################################################
def getnightdirectory():
    tag = gettag()
    from os.path import join, exists
    from osa.utils.utils import lstdate_to_dir
    from osa.configs.config import cfg

    verbose(tag, "Getting analysis path for tel_id {0}".format(options.tel_id))
    date_subdir = lstdate_to_dir(options.date)
    directory = join(cfg.get(options.tel_id, 'ANALYSISDIR'),
                     date_subdir,
                     options.lstchain_version + '_' +
                     cfg.get(options.tel_id, 'VERSION')
                     )
#    directory = join(cfg.get(options.tel_id, 'ANALYSISDIR'), subdir)
    if not exists(directory):
        if options.nightsum == True and options.tel_id != 'ST':
            error(tag, "Night directory {0} does not exists!".format(directory), 2)
        elif options.simulate == True:
            warning(tag, "directory {0} does not exists".format(directory))
        else:
            make_directory(directory)
    verbose(tag, "Analysis directory: {0}".format(directory))
    return directory
##############################################################################
#
# make_directory
#
##############################################################################
def make_directory(dir):
    tag = gettag()
    from os import makedirs
    from os.path import exists, isdir
    if exists(dir):
        if not isdir(dir):
            # Ups!! a file instead of a dir?
            error(tag, "{0} exists but is not a directory".format(dir), 2)
        else:
            # It is a directory, OK, we could check for access.
            return False 
    else:
        try:
            makedirs(dir)
        except IOError as NameError:
            error(tag, "Problems creating {0}, {1}".format(dir, NameError), 2)
        #except OSError as (ValueError, NameError):
        except OSError as NameError:
            error(tag, "Problems creating {0}, {1}".format(dir, NameError), ValueError)
        else:
            verbose(tag, "Created {0}".format(dir))
            return True
##############################################################################
#
# sorted_nicely
# Copied from
# http://stackoverflow.com/questions/2669059/how-to-sort-alpha-numeric-set-in-pytho
#
##############################################################################
def sorted_nicely(l): 
    tag = gettag()
    """ Sort the given iterable in the way that humans expect.""" 
    import re
    convert = lambda text: int(text) if text.isdigit() else text 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(l, key = alphanum_key)
##############################################################################
#
# getrawdatadays
#
##############################################################################
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
    #except OSError as (ValueError, NameError):
    except OSError as NameError:
        error(tag, "{0}, {1}".format(daqdir, NameError), ValueError)
    else:
        for element in dirs:
            if isdir(join(daqdir, element)) and fnmatch(element, '20[0-9][0-9]_[0-1][0-9]_[0-3][0-9]'):
                # We check for directory naming
                verbose(tag, "Found raw dir {0}".format(element))
                validset.append(element)
    return set(validset)
##############################################################################
#
# getstereodatadays
#
##############################################################################
def getstereodatadays():
    tag = gettag()

    import os
    from os.path import isdir, join, dirname, basename
    from os import walk
    from fnmatch import fnmatch
    from osa.configs.config import cfg
    stereodir = cfg.get(options.tel_id, 'ANALYSISDIR')
    validset = set()
    for root, dirs, files in walk(stereodir):
        for element in dirs:
            month = basename(root)
            year = basename(dirname(root))
            if isdir(join(root,element)) and dirname(dirname(root)) == stereodir:
                # We check for directory naming
                verbose(tag, "Found stereo dir {0}, {1} == {2} ".format(element, dirname(dirname(root)), stereodir))
                validset.add('_'.join([year, month, element]))
            else:
                verbose(tag, "Element {0} not a stereo dir {1}, {2} != {3} ".format(element, isdir(element), dirname(dirname(root)), stereodir))
    return validset
##############################################################################
#
# getfinisheddays
#
##############################################################################
def getfinisheddays():
    tag = gettag()
    from os import walk
    from os.path import isfile, join
    from osa.utils.utils import dir_to_lstdate
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
                verbose(tag, "Closed night {0} found!".format(night_closed))
    return set(validlist)
##############################################################################
#
# createlock
#
##############################################################################
def createlock(lockfile, content):
    tag = gettag()
    from os import getpid
    from os.path import isfile, isdir, exists, dirname
    from socket import gethostname
    from iofile import writetofile
    dir = dirname(lockfile)
    if options.simulate:
         verbose(tag, "SIMULATE Creation of lock file {0}".format(lockfile))
         return True
    else:
        if exists(lockfile) and isfile(lockfile):
            with open(lockfile, 'r') as f:
                hostpid = f.readline()
            error(tag, "Lock by a previous process {0}, exiting!\n".format(hostpid), 4)
        else:
            if not exists(dir):
                make_directory(dir)
                verbose(tag, "Creating parent directory {0} for lock file".format(dir))
            if isdir(dir):
                pid = str(getpid())
                hostname = gethostname()
                content = "{0}:{1}".format(hostname, pid)
                writetofile(lockfile, content)
                verbose(tag, "Lock file {0} created".format(lockfile))
                return True 
            else:
                error(tag, "Expecting {0} to be a directory, not a file".format(dir), 3)
##############################################################################
#
# getlockfile
#
##############################################################################
def getlockfile():
    tag = gettag()
    from os.path import join
    from osa.configs.config import cfg
    from osa.utils.utils import lstdate_to_dir
    basename = cfg.get('LSTOSA', 'ENDOFACTIVITYPREFIX') + cfg.get('LSTOSA', 'TEXTSUFFIX')
    dir = join(cfg.get(options.tel_id, 'CLOSERDIR'), lstdate_to_dir(options.date))
    lockfile = join(dir, basename)
    verbose(tag, "Lock file is {0}".format(lockfile))
    return lockfile
##############################################################################
#
# def lstdate_to_number
#
##############################################################################
def lstdate_to_number(night):
    tag = gettag()
    from osa.configs.config import cfg
    # Function to change from YYYY_MM_DD to YYYYMMDD
    # The iso standard separates year, month and day by a minus sign
    sepbar = ''
    numberdate = night.replace(cfg.get('LST', 'DATESEPARATOR'), sepbar)
    return numberdate
##############################################################################
#
# def lstdate_to_iso
#
##############################################################################
def lstdate_to_iso(night):
    tag = gettag()
    from osa.configs.config import cfg
    # Function to change from YYYY_MM_DD to YYYY-MM-DD
    # The iso standard separates year, month and day by a minus sign
    sepbar = '-'
    isodate = night.replace(cfg.get('LST', 'DATESEPARATOR'), sepbar)
    return isodate
##############################################################################
#
# def lstdate_to_dir
#
##############################################################################
def lstdate_to_dir(night):
    tag = gettag()
    from os.path import join
    from osa.configs.config import cfg
    # Function to change from YYYY_MM_DD to YYYY/MM/DD
    nightdir = night.split(cfg.get('LST', 'DATESEPARATOR'))
    if len(nightdir) != 3:
        error(tag, "Error: night directory structure could not be created from {0}\n".format(nightdir), 1)
   # dir = join(nightdir[0], nightdir[1], nightdir[2])
    dir = "".join(nightdir)
    return dir
##############################################################################
#
# def lstcdate_to_lstdir
#
##############################################################################
def dir_to_lstdate(dir):
    tag = gettag()
    from os.path import split
    from osa.configs.config import cfg
    # Function to change from WHATEVER/YYYY/MM/DD to YYYY_MM_DD
    sep = cfg.get('LST', 'DATESEPARATOR')
    dircopy = dir
    nightdir = ['YYYY','MM','DD']
    for i in reversed(range(3)):
        dircopy, nightdir[i] = split(dircopy)
    night = sep.join(nightdir)

    if len(night) != 10:
        error(tag, "Error: night {0} could not be created from {1}\n".format(night, dir), 1)
    return night

##############################################################################
#
# def build_lstbasename
#
##############################################################################
def build_lstbasename(prefix, suffix):
    tag = gettag()
    from osa.utils.utils import lstdate_to_number
    basename ="{0}_{1}{2}".format(prefix, lstdate_to_number(options.date), suffix)
    return basename
##############################################################################
#
# def is_defined
#
##############################################################################
def is_defined(variable):
    tag = gettag()
    try:
        variable
    except NameError:
        variable = None

    if variable != None:
        return True
    else:
        return False
##############################################################################
#
# def get_night_limit_timestamp
#
##############################################################################
def get_night_limit_timestamp():
    tag = gettag()
    from mysql import select_db
    from osa.configs.config import cfg
    night_limit = None
    server = cfg.get('MYSQL','server')
    user = cfg.get('MYSQL','user')
    database = cfg.get('MYSQL','database')
    table = cfg.get('MYSQL', 'nighttimes')
    night = lstdate_to_iso(options.date)
    selections = ['END']
    conditions = {'NIGHT': night}
    matrix = select_db(server, user, database, table, selections, conditions)
    if len(matrix) > 0:
        night_limit = matrix[0][0]
    else:
        errornonfatal(tag, "No night_limit found")
    verbose(tag, "Night limit is {0}".format(night_limit))
    return night_limit
##############################################################################
#
# def get_md5sum_and_copy
#
##############################################################################
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
            with open(inputf, 'rb') as f, open(outputf, 'w') as o:
                block_size=8192
                for chunk in iter(lambda: f.read(128*block_size), b''):
                    md5.update(chunk)
                    o.write(chunk)
        #except IOError as (ErrorValue, ErrorName):
        except IOError as ErrorValue:
            output(tag, "{0}".format(ErrorName, ErrorValue))
        else:
            return md5.hexdigest()
##############################################################################
#
# def is_empty_root_file
#
##############################################################################
def is_empty_root_file(input_str):
    tag = gettag()

    import ROOT as root
    from os.path import expandvars

    marssys = expandvars("$MARSSYS")
    # if var does not exists, it will not get expanded ...
    if marssys == "$MARSSYS":
        warning(tag, "$MARSSYS variable not set in bash!\n")
        return False
    if root.gSystem.Load(marssys + "/libmars.so") < 0:
        warning(tag, "Could not load Mars library")
        return False

    # check if file/s is/are empty
    ch = root.TChain("Events")
    ch.Add(input_str)
    if ch.GetEntries() > 0:
        verbose(tag, "Root files {0} has more than 0 events".format(input_str))
        return False
    return True
