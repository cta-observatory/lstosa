"""Functions to deal with dates, directories and prod IDs."""

import hashlib
import inspect
import logging
import os
import subprocess
from datetime import datetime, timedelta
from os import getpid, readlink, symlink
from os.path import dirname, exists, islink, split
from pathlib import Path
from socket import gethostname

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import write_to_file

__all__ = [
    "getcurrentdate",
    "night_directory",
    "get_lstchain_version",
    "create_directories_datacheck_web",
    "set_no_observations_flag",
    "copy_files_datacheck_web",
    "lstdate_to_dir",
    "lstdate_to_iso",
    "lstdate_to_number",
    "is_day_closed",
    "get_prod_id",
    "date_in_yymmdd",
    "destination_dir",
    "time_to_seconds",
    "date_in_yymmdd",
    "time_to_seconds",
    "get_lock_file",
    "is_defined",
    "destination_dir",
    "create_lock",
    "get_input_file",
    "stringify",
    "gettag",
    "get_calib_prod_id",
    "get_md5sum_and_copy",
    "get_dl1_prod_id",
    "get_dl2_prod_id",
    "get_night_limit_timestamp",
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
    string_date: string
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
    string_date = now.strftime("%Y" + sep + "%m" + sep + "%d")
    log.debug(f"stringdate by default {string_date}")
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
    directory = Path(cfg.get(options.tel_id, "ANALYSISDIR")) / date / options.prod_id

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

    options.lstchain_version = "v" + __version__
    return options.lstchain_version


def get_prod_id():
    """
    Get production ID from the configuration file if it is defined.
    Otherwise, it takes the lstchain version used.

    Returns
    -------
    prod_id: string
    """
    if not options.prod_id:
        if cfg.get("LST1", "PROD-ID") is not None:
            options.prod_id = cfg.get("LST1", "PROD-ID")
        else:
            options.prod_id = get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")

    log.debug(f"Getting prod ID for the running analysis directory: {options.prod_id}")

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
            options.calib_prod_id = (
                get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")
            )

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
        if cfg.get("LST1", "DL1-PROD-ID") is not None:
            options.dl1_prod_id = cfg.get("LST1", "DL1-PROD-ID")
        else:
            options.dl1_prod_id = (
                get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")
            )

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
            options.dl2_prod_id = (
                get_lstchain_version() + "_" + cfg.get("LST1", "VERSION")
            )

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
        log.debug(f"SIMULATE Creation of lock file {lockfile}")
    else:
        if lockfile.exists() and lockfile.is_file():
            with open(lockfile, "r") as f:
                hostpid = f.readline()
            log.error(f"Lock by a previous process {hostpid}, exiting!\n")
            return True
        else:
            if not directory_lock.exists():
                directory_lock.mkdir(exist_ok=True, parents=True)
                log.debug(f"Creating parent directory {directory_lock} for lock file")
                pid = str(getpid())
                hostname = gethostname()
                content = f"{hostname}:{pid}"
                write_to_file(lockfile, content)
                log.debug(f"Lock file {lockfile} created")
                return True
    return False


def get_lock_file():
    """
    Create night-is-finished lock file.

    Returns
    -------
    lockfile: pathlib.Path
        Path of the lock file
    """
    basename = cfg.get("LSTOSA", "end_of_activity")
    date = lstdate_to_dir(options.date)
    close_directory = Path(cfg.get(options.tel_id, "CLOSERDIR"))
    lock_file = close_directory / date / options.prod_id / basename
    log.debug(f"Looking for lock file {lock_file}")
    return lock_file


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


def lstdate_to_dir(date):
    """
    Function to change from YYYY_MM_DD to YYYYMMDD.

    Parameters
    ----------
    date: string
        String with the date in YYYY_MM_DD format

    Returns
    -------
    String with the date in YYYYMMDD format
    """
    nightdir = date.split(cfg.get("LST", "DATESEPARATOR"))
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
    flag_file = get_lock_file()
    return bool(exists(flag_file))


def time_to_seconds(timestring):
    """Transform (D-)HH:MM:SS time format to seconds.

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
        days, hhmmss = timestring.split(
            "-",
        )
        hours, minutes, seconds = hhmmss.split(":")
        return (
            int(days) * 24 * 3600 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        )

    hours, minutes, seconds = timestring.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


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
            cfg.get(options.tel_id, concept + "DIR"),
            nightdir,
            options.prod_id,
            options.dl2_prod_id,
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


def copy_files_datacheck_web(host, datedir, file_list) -> None:
    """
    Copy files to the data-check webserver via scp.

    Parameters
    ----------
    host: str
        Hostname of the webserver
    datedir: str
        Date directory in the webserver
    file_list: list
        List of files to be copied
    """
    # FIXME: Check if files exists already at webserver CHECK HASH
    # Copy files to server
    for file_to_transfer in file_list:
        if "drs4" in str(file_to_transfer):
            destination_dir = DATACHECK_BASEDIR / "drs4" / options.prod_id / datedir
            cmd = ["scp", str(file_to_transfer), f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "calibration" in str(file_to_transfer):
            destination_dir = (
                DATACHECK_BASEDIR / "enf_calibration" / options.prod_id / datedir
            )
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)

        elif "datacheck" in str(file_to_transfer):
            destination_dir = DATACHECK_BASEDIR / "dl1" / options.prod_id / datedir
            cmd = ["scp", file_to_transfer, f"{host}:{destination_dir}/."]
            subprocess.run(cmd)


def get_input_file(run_number):
    """
    Get the input file for the given run number.

    Parameters
    ----------
    run_number: str
        String with the run number

    Returns
    -------
    input_file: str

    Raises
    ------
    IOError
        If the input file cannot be found.
    """
    r0_path = Path(cfg.get("LST1", "RAWDIR")).absolute()

    # Get raw data file.
    file_list = list(
        r0_path.rglob(f"*/{cfg.get('LSTOSA', 'R0PREFIX')}.Run{run_number}.0000*")
    )

    if file_list:
        return str(file_list[0])
    raise IOError(f"Files corresponding to run {run_number} not found in {r0_path}.")


def stringify(args):
    """Join a list of arguments in a string."""
    return " ".join(map(str, args))


def gettag():
    """Get the name of the script currently being used."""
    parent_file = os.path.basename(inspect.stack()[1][1])
    parent_module = inspect.stack()[1][3]
    return f"{parent_file}({parent_module})"
