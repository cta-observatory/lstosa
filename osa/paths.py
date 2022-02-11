"""Handle the paths of the analysis products."""

import logging
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_dir, date_in_yymmdd

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "get_time_calibration_file",
    "get_calibration_file",
    "get_drs4_pedestal_file",
    "get_systematic_correction_file",
    "pedestal_ids_file_exists",
    "get_run_date",
    "drs4_pedestal_exists",
    "calibration_file_exists",
    "sequence_calibration_files",
]


def get_run_date(run_id: int) -> str:
    """Return the date of the run corresponding to the run id given."""
    r0_dir = Path(cfg.get("LST1", "R0_DIR"))
    r0_file = sorted(r0_dir.rglob(f"LST-1.1.Run{run_id:05d}.0000.fits.fz"))
    return r0_file[0].parent.name


def get_time_calibration_file(run_id: int) -> Path:
    """
    Return the time calibration file corresponding to a calibration run taken before
    the run id given. If run_id is smaller than the first run id from the time
    calibration files, return the first time calibration file available, which
    corresponds to 1625.
    """
    time_calibration_dir = Path(cfg.get("LST1", "TIMECALIB_DIR"))
    file_list = sorted(time_calibration_dir.rglob("pro/time_calibration.Run*.h5"))

    if not file_list:
        raise IOError("No time calibration file found")

    for file in file_list:
        run_in_list = int(file.name.split(".")[1].strip("Run"))
        if run_id < 1625:
            time_calibration_file = file_list[0]
        elif run_in_list <= run_id:
            time_calibration_file = file
        else:
            break

    return time_calibration_file.resolve()


def get_systematic_correction_file(date: str) -> Path:
    """
    Return the systematic correction file for a given date.

    Parameters
    ----------
    date : str
        Date in the format YYYYMMDD.

    Notes
    -----
    The search for the proper systematic correction file is based on
    lstchain/scripts/onsite/onsite_create_calibration_file.py
    """
    sys_dir = Path(cfg.get("LST1", "SYSTEMATIC_DIR"))

    # Search for the first sys correction file before the run, if nothing before,
    # use the first found
    dir_list = sorted(sys_dir.rglob('*/pro/ffactor_systematics*'))
    if not dir_list:
        raise IOError(
            f"No systematic correction file found for production pro in {sys_dir}\n"
        )
    sys_date_list = sorted([file.parts[-3] for file in dir_list], reverse=True)
    selected_date = next(
        (day for day in sys_date_list if day <= date), sys_date_list[-1]
    )

    return Path(
        f"{sys_dir}/{selected_date}/pro/ffactor_systematics_{selected_date}.h5"
    ).resolve()


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
    Return the drs4 pedestal file corresponding to a given run id
    regardless of the date when the run was taken.
    """
    calib_dir = Path(cfg.get("LST1", "CALIB_DIR"))
    date = get_run_date(run_id)
    file = calib_dir / date / f"pro/calibration_filters_52.Run{run_id:05d}.0000.h5"
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
    yy_mm_dd = date_in_yymmdd(date)
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


def sequence_calibration_files(sequence_list):
    """Build names of the calibration files for each sequence in the list."""
    flat_date = lstdate_to_dir(options.date)

    for sequence in sequence_list:

        if not sequence.parent_list:
            drs4_pedestal_run_id = sequence.previousrun
            pedcal_run_id = sequence.run
        else:
            drs4_pedestal_run_id = sequence.parent_list[0].previousrun
            pedcal_run_id = sequence.parent_list[0].run

        # Assign the calibration files to the sequence object
        sequence.pedestal = get_drs4_pedestal_file(drs4_pedestal_run_id)
        sequence.calibration = get_calibration_file(pedcal_run_id)
        sequence.time_calibration = get_time_calibration_file(pedcal_run_id)
        sequence.systematic_correction = get_systematic_correction_file(flat_date)
