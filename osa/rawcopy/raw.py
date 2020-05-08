from osa.configs import config
from osa.utils import options
from osa.utils.standardhandle import verbose, warning, error, output, gettag


def arerawfilestransferredfortel(tel_id):
    tag = gettag()
    from os.path import join, exists
    from osa.utils.utils import lstdate_to_dir
    nightdir = lstdate_to_dir(options.date)
    dir = join(config.cfg.get(tel_id, 'ENDOFRAWTRANSFERDIR'), nightdir)
    flagfile = join(dir, config.cfg.get('LSTOSA', 'ENDOFACTIVITYPREFIX'))

    if exists(flagfile):
        output(tag, f"Files for {options.date} {tel_id} are completely transferred to raid")
        return True
    else:
        warning(tag, f"File {flagfile} not found!")
        output(tag, f"Files for {options.date} {tel_id} are not yet transferred to raid. Expecting more raw data")
        return False


def arerawfilestransferred():
    tag = gettag()
    answer = None
    if options.tel_id == 'ST':
        answer_lst1 = arerawfilestransferredfortel('LST1')
        answer_lst2 = arerawfilestransferredfortel('LST2')
        answer = answer_lst1 * answer_lst2
    else:
        answer = arerawfilestransferredfortel(options.tel_id)
    return answer


def get_check_rawdir():
    tag = gettag()
    from os.path import exists, join
    rawdir = getrawdir()
    rawsuffix = config.cfg.get('LSTOSA', 'RAWSUFFIX')
    verbose(tag, f"raw suffix = {rawsuffix}")
    compressedsuffix = config.cfg.get('LSTOSA', 'COMPRESSEDSUFFIX')
    verbose(tag, f"raw compressed suffix = {rawsuffix + compressedsuffix}")
    verbose(tag, f"Trying raw directory: {rawdir}")

    if not exists(rawdir):
        # The most sensible thing to do is to quit succesfully after a warning
        # warning (tag, 'rawdir set to . because ' + rawdir + ' does not exists!')
        # rawdir = os.getcwd()
        error(tag, f"Raw directory {rawdir} does not exist", 2)
    else:
        # Check that it contains at least one raw or compressed-raw file and set compression flag
        from glob import glob
        list = glob(join(rawdir, '*' + rawsuffix))
        listz = glob(join(rawdir, '*' + rawsuffix + compressedsuffix))
        if (len(list) + len(listz)) == 0:
            error(tag, f"Empty raw directory {rawdir}", 5)
        elif len(list) != 0 and len(listz) != 0:
            warning(tag, f"Both, compressed and not compressed filex co-existing in {rawdir}")
        elif len(listz) != 0:
            options.compressed = True
            verbose(tag, "Compressed option flag set")
    verbose(tag, f"Raw directory: {rawdir}")
    return rawdir


def getrawdir():
    tag = gettag()
    from os.path import join
    from osa.configs import config
    from osa.utils import utils
    rawdir = None
    nightdir = utils.lstdate_to_dir(options.date)
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        rawdir = join(config.cfg.get(options.tel_id, 'RAWDIR'), nightdir)
    return rawdir


def getreportdir():
    tag = gettag()
    from os.path import exists, join
    reportdir = join(config.cfg.get(options.tel_id, 'REPORTDIR'), options.date)
    reportsuffix = config.cfg.get('LSTOSA', 'REPORTSUFFIX')
    if not exists(reportdir):
        # The most sensible thing to do is to quit succesfully after a warning
        # warning (tag, 'rawdir set to . because ' + rawdir + ' does not exists!')
        # rawdir = os.getcwd()
        error(tag, f"Report directory {reportdir} does not exist", 2)
    else:
        # Check that it contains at least one raw or compressed-raw file and set compression flag
        from glob import glob
        list = glob(join(reportdir, '*' + reportsuffix))
      
        if len(list) == 0:
            error(tag, f"Empty report directory {reportdir}", 5)
    verbose(tag, f"Report directory: {reportdir}")
    return reportdir
