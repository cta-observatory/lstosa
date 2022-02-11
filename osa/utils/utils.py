"""Functions to deal with dates and prod IDs."""

import inspect
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from socket import gethostname

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import write_to_file
from osa.utils.logging import myLogger

__all__ = [
    "getcurrentdate",
    "night_directory",
    "get_lstchain_version",
    "lstdate_to_dir",
    "lstdate_to_iso",
    "is_day_closed",
    "get_prod_id",
    "date_in_yymmdd",
    "get_lock_file",
    "is_defined",
    "create_lock",
    "stringify",
    "gettag",
    "get_calib_prod_id",
    "get_dl1_prod_id",
    "get_dl2_prod_id",
    "get_night_limit_timestamp",
    "time_to_seconds",
    "DATACHECK_FILE_PATTERNS"
]

log = myLogger(logging.getLogger(__name__))

DATACHECK_PRODUCTS = ["drs4", "enf_calibration", "dl1"]

DATACHECK_FILE_PATTERNS = {
    "PEDESTAL": "drs4*.pdf",
    "CALIB": "calibration*.pdf",
    "DL1": "datacheck_dl1*.pdf",
    "LONGTERM": "DL1_datacheck_*.*"
}
# Sets the amount of hours after midnight for the default OSA date
# to be the current date, eg 4: 04:00:00 UTC, -4: 20:00:00 UTC day before
LIMIT_NIGHT = 12


def getcurrentdate(sep="_"):
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
    string_date: string
        Date in string format using the given separator
    """
    now = datetime.utcnow()
    if (now.hour >= LIMIT_NIGHT >= 0) or (
            now.hour < LIMIT_NIGHT + 24 and LIMIT_NIGHT < 0
    ):
        # today, nothing to do
        pass
    elif LIMIT_NIGHT >= 0:
        # yesterday
        gap = timedelta(hours=24)
        now = now - gap
    else:
        # tomorrow
        gap = timedelta(hours=24)
        now = now + gap
    string_date = now.strftime(f"%Y{sep}%m{sep}%d")
    log.debug(f"Date string by default {string_date}")
    return string_date


def night_directory():
    """
    Path of the running_analysis directory for a certain night

    Returns
    -------
    directory
        Path of the running_analysis directory for a certain night
    """
    log.debug(f"Getting analysis path for tel_id {options.tel_id}")
    date = lstdate_to_dir(options.date)
    options.prod_id = get_prod_id()
    directory = Path(cfg.get(options.tel_id, "ANALYSIS_DIR")) / date / options.prod_id

    if not directory.exists() and not options.simulate:
        directory.mkdir(parents=True, exist_ok=True)
    else:
        log.debug("SIMULATE the creation of the analysis directory.")

    log.debug(f"Analysis directory: {directory}")
    return directory


def get_lstchain_version():
    """
    Get the lstchain version.

    Returns
    -------
    lstchain_version: string
    """
    from lstchain import __version__
    return "v" + __version__


def get_prod_id():
    """
    Get production ID from the configuration file if it is defined.
    Otherwise, it takes the lstchain version used.

    Returns
    -------
    prod_id: string
    """
    if not options.prod_id:
        if cfg.get("LST1", "PROD_ID") is not None:
            options.prod_id = cfg.get("LST1", "PROD_ID")
        else:
            options.prod_id = get_lstchain_version()

    log.debug(f"Getting prod ID for the running analysis directory: {options.prod_id}")

    return options.prod_id


def get_calib_prod_id() -> str:
    """Build calibration production ID."""
    if not options.calib_prod_id:
        if cfg.get("LST1", "CALIB_PROD_ID") is not None:
            options.calib_prod_id = cfg.get("LST1", "CALIB_PROD_ID")
        else:
            options.calib_prod_id = get_lstchain_version()

    log.debug(f"Getting prod ID for calibration products: {options.calib_prod_id}")

    return options.calib_prod_id


def get_dl1_prod_id():
    """
    Get the prod ID for the dl1 products provided
    it is defined in the configuration file.

    Returns
    -------
    dl1_prod_id: string
    """
    if not options.dl1_prod_id:
        if cfg.get("LST1", "DL1_PROD_ID") is not None:
            options.dl1_prod_id = cfg.get("LST1", "DL1_PROD_ID")
        else:
            options.dl1_prod_id = get_lstchain_version()

    log.debug(f"Getting prod ID for DL1 products: {options.dl1_prod_id}")

    return options.dl1_prod_id


def get_dl2_prod_id():
    """

    Returns
    -------

    """
    if not options.dl2_prod_id:
        if cfg.get("LST1", "DL2_PROD_ID") is not None:
            options.dl2_prod_id = cfg.get("LST1", "DL2_PROD_ID")
        else:
            options.dl2_prod_id = get_lstchain_version()

    log.debug(f"Getting prod ID for DL2 products: {options.dl2_prod_id}")

    return options.dl2_prod_id


def create_lock(lockfile) -> bool:
    """
    Create a lock file to prevent multiple instances of the same analysis.

    Parameters
    ----------
    lockfile: pathlib.Path

    Returns
    -------
    bool
    """
    directory_lock = lockfile.parent
    if options.simulate:
        log.debug(f"Simulate the creation of lock file {lockfile}")
    elif lockfile.exists() and lockfile.is_file():
        with open(lockfile, "r") as f:
            hostpid = f.readline()
        log.error(f"Lock by a previous process {hostpid}, exiting!\n")
        return True
    else:
        if not directory_lock.exists():
            directory_lock.mkdir(exist_ok=True, parents=True)
            log.debug(f"Creating parent directory {directory_lock} for lock file")
            pid = str(os.getpid())
            hostname = gethostname()
            content = f"{hostname}:{pid}"
            write_to_file(lockfile, content)
            log.debug(f"Lock file {lockfile} created")
            return True
    return False


def get_lock_file() -> Path:
    """
    Create night-is-finished lock file.

    Returns
    -------
    lockfile: pathlib.Path
        Path of the lock file
    """
    basename = cfg.get("LSTOSA", "end_of_activity")
    date = lstdate_to_dir(options.date)
    close_directory = Path(cfg.get(options.tel_id, "CLOSER_DIR"))
    lock_file = close_directory / date / options.prod_id / basename
    log.debug(f"Looking for lock file {lock_file}")
    return lock_file.resolve()


def lstdate_to_iso(date_string):
    """Function to change from YYYY_MM_DD to YYYY-MM-DD."""
    date_format = "%Y_%m_%d"
    datetime.strptime(date_string, date_format)
    return date_string.replace("_", "-")


def lstdate_to_dir(date_string):
    """Function to change from YYYY_MM_DD to YYYYMMDD."""
    date_format = "%Y_%m_%d"
    datetime.strptime(date_string, date_format)
    return date_string.replace("_", "")


def is_defined(variable):
    """Check if a variable is already defined."""
    try:
        variable
    except NameError:
        variable = None
    return variable is not None


def get_night_limit_timestamp():
    """Night limit timestamp for DB."""
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


def is_day_closed() -> bool:
    """Get the name and Check for the existence of the Closer flag file."""
    flag_file = get_lock_file()
    return flag_file.exists()


def date_in_yymmdd(date_string):
    """
    Convert date string YYYYMMDD into YY_MM_DD format to be used for
    drive log file names.

    Parameters
    ----------
    date_string: in format YYYYMMDD

    Returns
    -------
    yy_mm_dd: date_string in format YY_MM_DD

    """
    date = list(date_string)
    year = "".join(date[2:4])
    month = "".join(date[4:6])
    day = "".join(date[6:8])
    return f"{year}_{month}_{day}"


def stringify(args):
    """Join a list of arguments in a string."""
    return " ".join(map(str, args))


def gettag():
    """Get the name of the script currently being used."""
    parent_file = os.path.basename(inspect.stack()[1][1])
    parent_module = inspect.stack()[1][3]
    return f"{parent_file}({parent_module})"


def time_to_seconds(timestring):
    """
    Transform (D-)HH:MM:SS time format to seconds.

    Parameters
    ----------
    timestring: str
        Time in format (D-)HH:MM:SS

    Returns
    -------
    Seconds that correspond to (D-)HH:MM:SS
    """
    if timestring is None:
        timestring = "00:00:00"
    if "-" in timestring:
        # Day is also specified (D-)HH:MM:SS
        days, hhmmss = timestring.split("-")
        hours, minutes, seconds = hhmmss.split(":")
        return (
            int(days) * 24 * 3600 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        )

    split_time = timestring.split(":")
    if len(split_time) == 2:
        # MM:SS
        minutes, seconds = split_time
        hours = 0
    elif len(split_time) == 3:
        # HH:MM:SS
        hours, minutes, seconds = split_time
    else:
        raise ValueError("Time format not recognized.")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
