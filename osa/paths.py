"""Handle the paths of the analysis products."""

import logging
from pathlib import Path
from typing import List

from astropy.table import Table
from lstchain.onsite import find_systematics_correction_file, find_time_calibration_file
from lstchain.scripts.onsite.onsite_create_calibration_file import search_filter

from osa.configs import options
from osa.configs.config import DEFAULT_CFG
from osa.configs.config import cfg
from osa.configs.datamodel import Sequence
from osa.utils import utils
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "get_calibration_file",
    "get_drs4_pedestal_file",
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


def get_run_date(run_id: int) -> str:
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
        date_string = utils.date_to_dir(options.date)

    return date_string.replace("-", "")


def get_drs4_pedestal_file(run_id: int) -> Path:
    """
    Return the drs4 pedestal file corresponding to a given run id
    regardless of the date when the run was taken.
    """
    drs4_pedestal_dir = Path(cfg.get("LST1", "PEDESTAL_DIR"))
    date = get_run_date(run_id)
    file = drs4_pedestal_dir / date / f"pro/drs4_pedestal.Run{run_id:05d}.0000.h5"
    return file.resolve()


def get_calibration_file(run_id: int) -> Path:
    """
    Return the calibration file corresponding to a given run_id.

    Parameters
    ----------
    run_id : int
        Run id of the calibration file to be built.

    Notes
    -----
    The file path will be built regardless of the date when the run was taken.
    We follow the naming convention of the calibration files produced by the lstchain script
    which depends on the filter wheels position. Therefore, we need to try to fetch the filter
    position from the CaCo database. If the filter position is not found, we assume the default
    filter position 5-2. Filter information is not available in the database for runs taken before
    mid 2021 approx.
    """

    calib_dir = Path(cfg.get("LST1", "CALIB_DIR"))
    date = get_run_date(run_id)

    if options.test:  # Run tests avoiding the access to the database
        filters = 52

    else:
        mongodb = cfg.get("database", "CaCo_db")
        try:
            # Cast run_id to int to avoid problems with numpy int64 encoding in MongoDB
            filters = search_filter(int(run_id), mongodb)
        except IOError:
            log.warning("No filter information found in database. Assuming positions 52.")
            filters = 52

    file = calib_dir / date / f"pro/calibration_filters_{filters}.Run{run_id:05d}.0000.h5"

    return file.resolve()


def pedestal_ids_file_exists(run_id: int) -> bool:
    """Look for the files with pedestal interleaved event identification."""
    pedestal_ids_dir = Path(cfg.get("LST1", "PEDESTAL_FINDER_DIR"))
    file_list = sorted(pedestal_ids_dir.rglob(f"pedestal_ids_Run{run_id:05d}.*.h5"))
    return bool(file_list)


def drs4_pedestal_exists(run_id: int) -> bool:
    """Return true if drs4 pedestal file was already produced."""
    file = get_drs4_pedestal_file(run_id)
    return file.exists()


def calibration_file_exists(run_id: int) -> bool:
    """Return true if calibration file was already produced."""
    file = get_calibration_file(run_id)
    return file.exists()


def get_drive_file(date: str) -> Path:
    """Return the drive file corresponding to a given date in YYYYMMDD format."""
    yy_mm_dd = utils.date_in_yymmdd(date)
    drive_dir = Path(cfg.get("LST1", "DRIVE_DIR"))
    return (drive_dir / f"drive_log_{yy_mm_dd}.txt").resolve()


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

    for sequence in sequence_list:
        # Assign the calibration files to the sequence object
        sequence.drs4_file = get_drs4_pedestal_file(sequence.drs4_run)
        sequence.calibration_file = get_calibration_file(sequence.pedcal_run)
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
        directory = destination_dir("DL1AB", create_dir=False)
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
        Expected: MUON, DL1AB, DATACHECK, DL2, PEDESTAL, CALIB, TIMECALIB
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
        directory = Path(cfg.get(options.tel_id, concept + "_DIR")) / nightdir / options.prod_id
    elif concept in {"DL1AB", "DATACHECK"}:
        directory = (
            Path(cfg.get(options.tel_id, concept + "_DIR"))
            / nightdir
            / options.prod_id
            / options.dl1_prod_id
        )
    elif concept in {"DL2", "DL3"}:
        directory = (
            Path(cfg.get(options.tel_id, concept + "_DIR"))
            / nightdir
            / options.prod_id
            / options.dl2_prod_id
        )
    elif concept in {"PEDESTAL", "CALIB", "TIMECALIB"}:
        directory = (
            Path(cfg.get(options.tel_id, concept + "_DIR")) / nightdir / options.calib_prod_id
        )
    elif concept == "HIGH_LEVEL":
        directory = Path(cfg.get(options.tel_id, concept + "_DIR")) / nightdir / options.prod_id
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
