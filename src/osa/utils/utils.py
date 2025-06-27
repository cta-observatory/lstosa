"""Functions to deal with dates and prod IDs."""


import inspect
import logging
import os
import re
import json
import time
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from socket import gethostname
import subprocess as sp
from gammapy.data import observatory_locations
from astropy import units as u
from astropy.table import Table
from lstchain.onsite import find_filter_wheels

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
    "time_to_seconds",
    "DATACHECK_FILE_PATTERNS",
    "YESTERDAY",
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


def get_calib_filters(run_id):
    """Get the filters used for the calibration."""
    if options.test:  # Run tests avoiding the access to the database
        return 52

    else:
        mongodb = cfg.get("database", "caco_db")
        try:
            # Cast run_id to int to avoid problems with numpy int64 encoding in MongoDB
            return find_filter_wheels(int(run_id), mongodb)
        except IOError:
            log.warning("No filter information found in database. Assuming positions 52.")
            return 52


def culmination_angle(dec: u.Quantity) -> u.Quantity:  
    """
    Calculate culmination angle for a given declination.

    Parameters
    ----------
    dec: Quantity
        declination coordinate in degrees

    Returns
    -------
    Culmination angle in degrees
    """
    location = observatory_locations["cta_north"]
    Lat = location.lat  # latitude of the LST1 site    
    return abs(Lat - dec)


def convert_dec_string(dec_str: str) -> u.Quantity:
    """Return the declination angle in degrees corresponding to a 
    given string of the form "dec_XXXX" or "dec_min_XXXX"."""
    
    # Check if dec_str has a valid format
    pattern = r'^dec_(\d{3,4})$|^dec_min_(\d{3,4})$'
    if re.match(pattern, dec_str):
        
        # Split the string into parts
        parts = dec_str.split('_')

        # Extract the sign, degrees, and minutes
        sign = 1 if 'min' not in parts else -1
        degrees = int(parts[-1])

        # Calculate the numerical value
        dec_value = sign * (degrees / 100)

        return dec_value*u.deg


def get_declinations_dict(list1: list, list2: list) -> dict:
    """Return a dictionary created from two given lists."""
    corresponding_dict = {}
    for index, element in enumerate(list2):
        corresponding_dict[element] = list1[index]
    return corresponding_dict
        

def get_nsb_dict(rf_models_dir: Path, rf_models_prefix: str) -> dict:
    """Return a dictionary with the NSB level of the RF models and the path to each model."""
    rf_models = sorted(rf_models_dir.glob(f"{rf_models_prefix}*"))
    pattern = r"nsb_tuning_([\d.]+)"
    nsb_dict = {
        float(re.search(pattern, str(rf_model)).group(1)): rf_model
        for rf_model in rf_models if re.search(pattern, str(rf_model))
    }
    return nsb_dict


def get_mc_nsb_dir(run_id: int, rf_models_dir: Path) -> Path:
    """
    Return the path of the RF models directory with the NSB level 
    closest to that of the data for a given run.
    """
    additional_nsb = get_nsb_level(run_id)
    rf_models_prefix = cfg.get("lstchain", "mc_prod")
    nsb_dict = get_nsb_dict(rf_models_dir, rf_models_prefix)
    closest_nsb_value = min(nsb_dict.keys(), key=lambda x: abs(float(x) - additional_nsb))

    return nsb_dict[closest_nsb_value]


def get_nsb_level(run_id):
    """Choose the closest NSB among those that are processed with the same cleaning level."""
    tailcuts_finder_dir = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR"))
    log_file = tailcuts_finder_dir / f"log_find_tailcuts_Run{run_id:05d}.log"
    with open(log_file, "r") as file:
        log_content = file.read()
    match = re.search(r"Additional NSB rate \(over dark MC\): ([\d.]+)", log_content)
    nsb = float(match.group(1))
    
    dl1b_config_filename = tailcuts_finder_dir / f"dl1ab_Run{run_id:05d}.json"
    with open(dl1b_config_filename) as json_file:
        dl1b_config = json.load(json_file)
    picture_th = dl1b_config["tailcuts_clean_with_pedestal_threshold"]["picture_thresh"]

    nsb_levels = np.array([0.00, 0.07, 0.14, 0.22, 0.38, 0.50, 0.81, 1.25, 1.76, 2.34])
    pth = np.array([8, 8, 8, 8, 10, 10, 12, 14, 16, 18])
    candidate_nsbs = nsb_levels[pth==picture_th]

    diff = abs(candidate_nsbs - nsb)
    return candidate_nsbs[np.argsort(diff)][0]


def get_RF_model(run_id: int) -> Path:
    """Get the path of the RF models to be used in the DL2 production for a given run.
    
    The choice of the models is based on the adequate additional NSB level
    and the proper declination line of the MC used for the training.
    """
    run_catalog_dir = Path(cfg.get(options.tel_id, "RUN_CATALOG"))
    run_catalog_file = run_catalog_dir / f"RunCatalog_{date_to_dir(options.date)}.ecsv"
    run_catalog = Table.read(run_catalog_file)
    pointing_dec = run_catalog[run_catalog["run_id"]==run_id]["source_dec"]*u.deg  
    # the "source_dec" given in the run catalogs is not actually the source declination, but the pointing declination
    pointing_culmination = culmination_angle(pointing_dec)

    rf_models_base_dir = Path(cfg.get(options.tel_id, "RF_MODELS"))
    rf_models_dir = get_mc_nsb_dir(run_id, rf_models_base_dir)
    dec_list = os.listdir(rf_models_dir)
    dec_list = [i for i in dec_list if i.startswith("dec")]

    # Convert each string in the list to numerical values
    dec_values = [convert_dec_string(dec) for dec in dec_list]
    dec_values = [dec for dec in dec_values if dec is not None]
    
    closest_declination = min(dec_values, key=lambda x: abs(x - pointing_dec))
    closest_dec_culmination = culmination_angle(closest_declination)
    
    lst_location = observatory_locations["cta_north"]
    lst_latitude = lst_location.lat  # latitude of the LST1 site    
    closest_lines = sorted(sorted(dec_values, key=lambda x: abs(x - lst_latitude))[:2])

    if pointing_dec < closest_lines[0] or pointing_dec > closest_lines[1]:
        # If the pointing declination is between the two MC lines closest to the latitude of 
        # the LST1 site, this check is not necessary.
        log.debug(
            f"The declination closest to {pointing_dec} is: {closest_declination}."
            "Checking if the culmination angle is larger than the one of the pointing."
        )
        while closest_dec_culmination > pointing_culmination:
            # If the culmination angle of the closest declination line is larger than for
            # the pointing declination, remove it from the declination lines list and
            # look for the second closest declination line.
            declinations_dict = get_declinations_dict(dec_list, dec_values)
            declination_str = declinations_dict[closest_declination]
            dec_values.remove(closest_declination)
            dec_list.remove(declination_str)
            closest_declination = min(dec_values, key=lambda x: abs(x - pointing_dec))
            closest_dec_culmination = culmination_angle(closest_declination)
    
    log.debug(f"The declination line to use for the DL2 production is: {closest_declination}")
    
    declinations_dict = get_declinations_dict(dec_list, dec_values)
    declination_str = declinations_dict[closest_declination]

    rf_model_path = rf_models_dir / declination_str

    return rf_model_path.resolve()


def get_lstcam_calib_version(env_path: Path) -> str:
    """Get the version of the lstcam_calib package installed in the given environment."""
    if options.test or options.simulate:
        return "0.1.1"
    python_exe = f"{str(env_path)}/bin/python"
    cmd = [python_exe, "-m", "pip", "show", "lstcam_calib"]
    result = sp.run(cmd, capture_output=True, text=True, check=True)
    for line in result.stdout.split('\n'):
        if line.startswith('Version:'):
            return line.split(':', 1)[1].strip()