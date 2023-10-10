"""Functions to deal with dates and prod IDs."""


import inspect
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from socket import gethostname

import osa.paths
from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import write_to_file
from osa.utils.logging import myLogger

__all__ = [
    "get_lstchain_version",
    "date_to_dir",
    "date_to_iso",
    "is_day_closed",
    "get_prod_id",
    "night_finished_flag",
    "is_defined",
    "create_lock",
    "stringify",
    "gettag",
    "get_dl1_prod_id",
    "get_dl2_prod_id",
    "time_to_seconds",
    "DATACHECK_FILE_PATTERNS",
    "YESTERDAY",
    "set_prod_ids",
    "is_night_time",
    "cron_lock",
    "example_seq",
    "wait_for_daytime",
]

log = myLogger(logging.getLogger(__name__))

DATACHECK_PRODUCTS = ["drs4", "enf_calibration", "dl1"]

DATACHECK_FILE_PATTERNS = {
    "PEDESTAL": "drs4*.pdf",
    "CALIB": "calibration*.pdf",
    "DL1AB": "datacheck_dl1*.pdf",
    "LONGTERM": "DL1_datacheck_*.*",
}

YESTERDAY = datetime.now() - timedelta(days=1)


def get_lstchain_version():
    """
    Get the lstchain version.

    Returns
    -------
    lstchain_version: string
    """
    from lstchain import __version__

    return f"v{__version__}"


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


def night_finished_flag() -> Path:
    """
    Create night-is-finished lock file.

    Returns
    -------
    lockfile: pathlib.Path
        Path of the lock file
    """
    basename = cfg.get("LSTOSA", "end_of_activity")
    date = date_to_dir(options.date)
    close_directory = Path(cfg.get(options.tel_id, "CLOSER_DIR"))
    lock_file = close_directory / date / options.prod_id / basename
    log.debug(f"Looking for lock file {lock_file}")
    return lock_file.resolve()


def date_to_iso(date: datetime) -> str:
    """Function to change from YYYY-MM-DD to YYYY-MM-DD."""
    return date.strftime("%Y-%m-%d")


def date_to_dir(date: datetime) -> str:
    """Function to change from YYYY-MM-DD to YYYYMMDD format (used for directories)."""
    return date.strftime("%Y%m%d")


def is_defined(variable):
    """Check if a variable is already defined."""
    try:
        variable
    except NameError:
        variable = None
    return variable is not None


def is_day_closed() -> bool:
    """Get the name and Check for the existence of the Closer flag file."""
    flag_file = night_finished_flag()
    return flag_file.exists()


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
    timestring: str or None
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
        return int(days) * 24 * 3600 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)

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


def set_prod_ids():
    """Set the product IDs."""
    options.prod_id = get_prod_id()

    if cfg.get("LST1", "DL1_PROD_ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id

    if cfg.get("LST1", "DL2_PROD_ID") is not None:
        options.dl2_prod_id = get_dl2_prod_id()
    else:
        options.dl2_prod_id = options.prod_id


def is_night_time(hour):
    """Check if it is nighttime."""
    if 8 <= hour <= 18:
        return False
    log.error("It is dark outside...")
    return True


def example_seq():
    """Example sequence table output for testing."""
    return "./extra/example_sequencer.txt"


def cron_lock(tel) -> Path:
    """Create a lock file for the cron jobs."""
    return osa.paths.analysis_path(tel) / "cron.lock"


def wait_for_daytime(start=8, end=18):
    """
    Check every hour if it is still nighttime
    to not running jobs while it is still night.
    """
    while time.localtime().tm_hour <= start or time.localtime().tm_hour >= end:
        log.info("Waiting for sunrise to not interfere with the data-taking. Sleeping.")
        time.sleep(3600)
