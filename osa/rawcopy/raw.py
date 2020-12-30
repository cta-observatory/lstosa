import logging
from glob import glob
from os.path import exists, join

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import lstdate_to_dir

log = logging.getLogger(__name__)


def are_rawfiles_transferred_for_tel(tel_id):
    nightdir = lstdate_to_dir(options.date)
    dir = join(cfg.get(tel_id, "ENDOFRAWTRANSFERDIR"), nightdir)
    flagfile = join(dir, cfg.get("LSTOSA", "ENDOFACTIVITYPREFIX"))

    # FIXME: How can we check that all files are there?
    if exists(flagfile):
        log.info(f"Files for {options.date} {tel_id} are completely transferred to raid")
        return True
    else:
        log.warning(f"File {flagfile} not found!")
        log.info(
            f"Files for {options.date} {tel_id} are not yet transferred to raid. Expecting more raw data"
        )
        return False


def are_rawfiles_transferred():
    if options.tel_id != "ST":
        return are_rawfiles_transferred_for_tel(options.tel_id)
    return are_rawfiles_transferred_for_tel("LST1")


def get_check_rawdir():
    rawdir = getrawdir()
    rawsuffix = cfg.get("LSTOSA", "RAWSUFFIX")
    log.debug(f"raw suffix = {rawsuffix}")
    compressedsuffix = cfg.get("LSTOSA", "COMPRESSEDSUFFIX")
    log.debug(f"raw compressed suffix = {rawsuffix + compressedsuffix}")
    log.debug(f"Trying raw directory: {rawdir}")

    if not exists(rawdir):
        # the most sensible thing to do is to quit succesfully after a warning
        # log.warning(f"rawdir set to . because {rawdir} does not exists!")
        # rawdir = os.getcwd()
        log.error(f"Raw directory {rawdir} does not exist")
    else:
        # check that it contains at least one raw or compressed-raw file and set compression flag
        list = glob(join(rawdir, "*" + rawsuffix))
        listz = glob(join(rawdir, "*" + rawsuffix + compressedsuffix))
        if (len(list) + len(listz)) == 0:
            log.error(f"Empty raw directory {rawdir}")
        elif len(list) != 0 and len(listz) != 0:
            log.warning(f"Both, compressed and not compressed filex co-existing in {rawdir}")
        elif len(listz) != 0:
            options.compressed = True
            log.debug("Compressed option flag set")
    log.debug(f"Raw directory: {rawdir}")
    return rawdir


def getrawdir():
    rawdir = None
    nightdir = lstdate_to_dir(options.date)
    if options.tel_id in ["LST1", "LST2"]:
        rawdir = join(cfg.get(options.tel_id, "RAWDIR"), nightdir)
    return rawdir


def getreportdir():
    reportdir = join(cfg.get(options.tel_id, "REPORTDIR"), options.date)
    reportsuffix = cfg.get("LSTOSA", "REPORTSUFFIX")
    if not exists(reportdir):
        # the most sensible thing to do is to quit succesfully after a warning
        # log.warning(f"rawdir set to . because {rawdir} does not exists!")
        # rawdir = os.getcwd()
        log.error(f"Report directory {reportdir} does not exist")
    else:
        # check that it contains at least one raw or compressed-raw file and set compression flag
        list = glob(join(reportdir, "*" + reportsuffix))

        if len(list) == 0:
            log.error(f"Empty report directory {reportdir}")
    log.debug(f"Report directory: {reportdir}")
    return reportdir
