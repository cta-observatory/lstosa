"""Handle the paths of the analysis products."""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List
import subprocess
import time

import lstchain
from astropy.table import Table
from lstchain.onsite import (find_systematics_correction_file,
                             find_time_calibration_file,
                             find_filter_wheels)

from osa.configs import options
from osa.configs.config import DEFAULT_CFG, cfg
from osa.configs.datamodel import Sequence
from osa.utils import utils
from osa.utils.logging import myLogger


log = myLogger(logging.getLogger(__name__))

__all__ = [
    "get_calibration_filename",
    "get_drs4_pedestal_filename",
    "pedestal_ids_file_exists",
    "get_run_date",
    "drs4_pedestal_exists",
    "calibration_file_exists",
    "sequence_calibration_files",
    "destination_dir",
    "datacheck_directory",
    "get_datacheck_files",
    "get_drive_file",
    "get_summary_file",
    "get_pedestal_ids_file",
    "DATACHECK_WEB_BASEDIR",
    "DEFAULT_CFG",
    "create_source_directories",
    "analysis_path",
]


DATACHECK_WEB_BASEDIR = Path(cfg.get("WEBSERVER", "DATACHECK"))
CALIB_BASEDIR = Path(cfg.get("LST1", "CALIB_DIR"))
DRS4_PEDESTAL_BASEDIR = Path(cfg.get("LST1", "PEDESTAL_DIR"))


def analysis_path(tel) -> Path:
    """
    Path of the running_analysis directory for a certain date

    Returns
    -------
    directory : Path
        Path of the running_analysis directory for a certain date
    """
    log.debug(f"Getting analysis path for telescope {tel}")
    flat_date = utils.date_to_dir(options.date)
    options.prod_id = utils.get_prod_id()
    directory = Path(cfg.get(tel, "ANALYSIS_DIR")) / flat_date / options.prod_id

    if not options.simulate:
        directory.mkdir(parents=True, exist_ok=True)
    else:
        log.debug("SIMULATE the creation of the analysis directory.")

    log.debug(f"Analysis directory: {directory}")
    return directory


def get_run_date(run_id: int) -> datetime:
    """
    Return the date (YYYYMMDD) when the given run was taken. The search for this date
    is done by looking at the date corresponding to each run in the merged run summaries
    file.
    """
    merged_run_summaries_file = cfg.get("LST1", "MERGED_SUMMARY")
    summary_table = Table.read(merged_run_summaries_file)

    try:
        date_string = summary_table[summary_table["run_id"] == run_id]["date"][0]
    except IndexError:
        log.warning(
            f"Run {run_id} is not in the summary table. "
            f"Assuming the date of the run is {options.date}."
        )
        date_string = utils.date_to_iso(options.date)

    return datetime.strptime(date_string, "%Y-%m-%d")


def get_drs4_pedestal_filename(run_id: int, prod_id: str) -> Path:
    """
    Return the drs4 pedestal file corresponding to a given run id
    regardless of the date when the run was taken.
    """
    if drs4_pedestal_exists(run_id, prod_id):
        files = search_drs4_files(run_id, prod_id)
        return files[-1]  # Get the latest production among the major lstchain version

    date = utils.date_to_dir(get_run_date(run_id))
    return (
        DRS4_PEDESTAL_BASEDIR
        / date
        / f"v{lstchain.__version__}/drs4_pedestal.Run{run_id:05d}.0000.h5"
    ).resolve()


def get_calibration_filename(run_id: int, prod_id: str) -> Path:
    # sourcery skip: remove-unnecessary-cast
    """
    Return the calibration file corresponding to a given run_id.

    Parameters
    ----------
    run_id : int
        Run id of the calibration file to be built.

    Notes
    -----
    The file path will be built regardless of the date when the run was taken.
    We follow the naming convention of the calibration files produced by the
    lstchain script which depends on the filter wheels position. Therefore, we
    need to try to fetch the filter position from the CaCo database. If the
    filter position is not found, we assume the default filter position 5-2.
    Filter information is not available in the database for runs taken before
    mid 2021 approx.
    """

    if calibration_file_exists(run_id, prod_id):
        files = search_calibration_files(run_id, prod_id)
        return files[-1]  # Get the latest production among the major lstchain version

    date = utils.date_to_dir(get_run_date(run_id))

    if options.test:  # Run tests avoiding the access to the database
        options.filters = 52

    else:
        mongodb = cfg.get("database", "caco_db")
        try:
            # Cast run_id to int to avoid problems with numpy int64 encoding in MongoDB
            options.filters = find_filter_wheels(int(run_id), mongodb)
        except IOError:
            log.warning("No filter information found in database. Assuming positions 52.")
            options.filters = 52

    return (
        CALIB_BASEDIR
        / date
        / f"v{lstchain.__version__}/calibration_filters_{options.filters}.Run{run_id:05d}.0000.h5"
    ).resolve()


def pedestal_ids_file_exists(run_id: int) -> bool:
    """Look for the files with pedestal interleaved event identification."""
    pedestal_ids_dir = Path(cfg.get("LST1", "PEDESTAL_FINDER_DIR"))
    file_list = sorted(pedestal_ids_dir.rglob(f"pedestal_ids_Run{run_id:05d}.*.h5"))
    return bool(file_list)


def drs4_pedestal_exists(run_id: int, prod_id: str) -> bool:
    """Return true if drs4 pedestal file was already produced."""
    files = search_drs4_files(run_id, prod_id)

    return len(files) != 0


def calibration_file_exists(run_id: int, prod_id: str) -> bool:
    """Return true if calibration file was already produced."""
    files = search_calibration_files(run_id, prod_id)

    return len(files) != 0


def search_drs4_files(run_id: int, prod_id: str) -> list:
    """
    Find DRS4 baseline correction files corresponding to a run ID
    and major lstchain production version
    """
    date = utils.date_to_dir(get_run_date(run_id))
    version = get_major_version(prod_id)
    drs4_dir = DRS4_PEDESTAL_BASEDIR / date
    return sorted(
        drs4_dir.glob(f"{version}*/drs4_pedestal.Run{run_id:05d}.0000.h5")
    )


def get_major_version(prod_id):
    """Given a version as vX.Y.Z return vX.Y"""
    # First check that the given version is in the correct format
    if prod_id.startswith("v") and len(prod_id.split(".")) >= 2:
        return re.search(r"\D\d+\.\d+", prod_id)[0]

    raise ValueError("Format of the version is not in the form vW.X.Y.Z")


def search_calibration_files(run_id: int, prod_id: str) -> list:
    """
    Search charge calibration files corresponding to a run ID and major lstchain production version
    """
    date = utils.date_to_dir(get_run_date(run_id))
    version = get_major_version(prod_id)
    return sorted(
        (CALIB_BASEDIR / date).glob(f"{version}*/calibration_filters_*.Run{run_id:05d}.0000.h5")
    )


def get_drive_file(date: str) -> Path:
    """Return the drive file corresponding to a given date in YYYYMMDD format."""
    drive_dir = Path(cfg.get("LST1", "DRIVE_DIR"))
    return (drive_dir / f"DrivePosition_log_{date}.txt").resolve()


def get_summary_file(date) -> Path:
    """Return the run summary file corresponding to a given date in YYYYMMDD format."""
    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    return (run_summary_dir / f"RunSummary_{date}.ecsv").resolve()


def get_pedestal_ids_file(run_id: int, date: str) -> Path:
    """
    Return the pedestal ids file path corresponding to a given run
    from a date in format YYYYMMDD.
    """
    pedestal_ids_dir = Path(cfg.get("LST1", "PEDESTAL_FINDER_DIR")) / date
    file = pedestal_ids_dir / f"pedestal_ids_Run{run_id:05d}.{{subruns:04d}}.h5"
    return file.resolve()


def sequence_calibration_files(sequence_list: List[Sequence]) -> None:
    """Build names of the calibration files for each sequence in the list."""
    flat_date = utils.date_to_dir(options.date)
    base_dir = Path(cfg.get("LST1", "BASE"))
    prod_id = options.prod_id

    for sequence in sequence_list:
        # Assign the calibration files to the sequence object
        sequence.drs4_file = get_drs4_pedestal_filename(
            sequence.drs4_run, prod_id
        )
        sequence.calibration_file = get_calibration_filename(
            sequence.pedcal_run, prod_id
        )
        sequence.time_calibration_file = find_time_calibration_file(
            "pro", sequence.pedcal_run, base_dir=base_dir
        )
        sequence.systematic_correction_file = find_systematics_correction_file(
            "pro", flat_date, base_dir=base_dir
        )


def get_datacheck_files(pattern: str, directory: Path) -> list:
    """Return a list of files matching the pattern."""
    return sorted(directory.glob(pattern))


def datacheck_directory(data_type: str, date: str) -> Path:
    """Returns the path to the datacheck directory given the data type."""
    if data_type in {"PEDESTAL", "CALIB"}:
        directory = Path(cfg.get("LST1", f"{data_type}_DIR")) / date / "pro/log"
    elif data_type == "DL1AB":
        directory = destination_dir("DATACHECK", create_dir=False)
    elif data_type == "LONGTERM":
        directory = Path(cfg.get("LST1", f"{data_type}_DIR")) / options.prod_id / date
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    return directory


def destination_dir(concept: str, create_dir: bool = True) -> Path:
    """
    Create final destination directory for each data level.
    See Also osa.utils.register_run_concept_files

    Parameters
    ----------
    concept : str
        Expected: MUON, DL1AB, DATACHECK, INTERLEAVED, DL2, PEDESTAL, CALIB, TIMECALIB
    create_dir : bool
        Set it to True (default) if you want to create the directory.
        Otherwise, it just returns the path

    Returns
    -------
    path : pathlib.Path
        Path to the directory
    """
    nightdir = utils.date_to_dir(options.date)

    if concept == "MUON":
        directory = Path(cfg.get(options.tel_id, "DL1_DIR")) / nightdir / options.prod_id / "muons"
    elif concept == "INTERLEAVED":
        directory = (
            Path(cfg.get(options.tel_id, "DL1_DIR")) / nightdir / options.prod_id / "interleaved"
        )
    elif concept == "DATACHECK":
        directory = (
            Path(cfg.get(options.tel_id, "DL1_DIR"))
            / nightdir
            / options.prod_id
            / options.dl1_prod_id
            / "datacheck"
        )
    elif concept == "DL1AB":
        directory = (
            Path(cfg.get(options.tel_id, "DL1_DIR"))
            / nightdir
            / options.prod_id
            / options.dl1_prod_id
        )
    elif concept in {"DL2", "DL3"}:
        directory = (
            (Path(cfg.get(options.tel_id, f"{concept}_DIR")) / nightdir)
            / options.prod_id
        ) / options.dl2_prod_id
    elif concept in {"PEDESTAL", "CALIB", "TIMECALIB"}:
        directory = (
            Path(cfg.get(options.tel_id, f"{concept}_DIR"))
            / nightdir
            / options.prod_id
        )
    elif concept == "HIGH_LEVEL":
        directory = (
            Path(cfg.get(options.tel_id, f"{concept}_DIR"))
            / nightdir
            / options.prod_id
        )
    else:
        log.warning(f"Concept {concept} not known")
        directory = None

    if not options.simulate and create_dir:
        log.debug(f"Destination directory created for {concept}: {directory}")
        directory.mkdir(parents=True, exist_ok=True)
    else:
        log.debug(f"SIMULATING creation of final directory for {concept}")

    return directory


def create_source_directories(source_list: list, cuts_dir: Path):
    """Create a subdirectory for each source."""
    for source in source_list:
        if source is not None:
            source_dir = cuts_dir / source
            source_dir.mkdir(parents=True, exist_ok=True)


def get_latest_version_file(longterm_files: List[str]) -> Path:
    """Get the latest version path of the produced longterm DL1 datacheck files for a given date."""
    return max(
        longterm_files,
        key=lambda path: int(path.parents[1].name.split(".")[1])
        if path.parents[1].name.startswith("v")
        else "",
    )


def is_job_completed(job_id: str):
    """
    Check whether SLURM job `job_id` has finished.
    
    It keeps checking every 10 minutes for one our.
    """
    n_max = 10
    n = 0
    while n < n_max:
        # Check if the status of the SLURM job is "COMPLETED"
        status = subprocess.run(["sacct", "--format=state", "--jobs", job_id], capture_output=True, text=True)
        if "COMPLETED" in status.stdout:
            log.debug(f"Job {job_id} finished successfully!")
            return True
        n += 1
        log.debug(f"Job {job_id} is not completed yet, checking again in 10 minutes...")
        time.sleep(600)  # wait 10 minutes to check again
    log.info(f"The maximum number of checks of job {job_id} was reached, job {job_id} did not finish succesfully yet.")
    return False


def create_longterm_symlink(cherenkov_job_id: str = None):
    """If the created longterm DL1 datacheck file corresponds to the latest 
    version available, make symlink to it in the "all" common directory."""
    if not cherenkov_job_id or is_job_completed(cherenkov_job_id):
        nightdir = utils.date_to_dir(options.date)
        longterm_dir = Path(cfg.get("LST1", "LONGTERM_DIR"))
        linked_longterm_file = longterm_dir / f"night_wise/all/DL1_datacheck_{nightdir}.h5"
        all_longterm_files = longterm_dir.rglob(f"v*/{nightdir}/DL1_datacheck_{nightdir}.h5")
        latest_version_file = get_latest_version_file(all_longterm_files)
        log.info("Symlink the latest version longterm DL1 datacheck file in the common directory.")
        linked_longterm_file.unlink(missing_ok=True)
        linked_longterm_file.symlink_to(latest_version_file)
    else:
        log.warning(f"Job {cherenkov_job_id} (lstchain_cherenkov_transparency) did not finish successfully.")

def dl1_datacheck_longterm_file_exits() -> bool:
    """Return true if the longterm DL1 datacheck file was already produced."""
    nightdir = utils.date_to_dir(options.date)
    longterm_dir = Path(cfg.get("LST1", "LONGTERM_DIR"))
    longterm_file = longterm_dir / options.prod_id / nightdir / f"DL1_datacheck_{nightdir}.h5"
    return longterm_file.exists()

