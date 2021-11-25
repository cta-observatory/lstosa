import logging
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import lstdate_to_dir

log = logging.getLogger(__name__)

__all__ = [
    "are_raw_files_transferred",
    "are_raw_files_transferred_for_tel",
    "get_check_raw_dir",
    "get_raw_dir"
]


def are_raw_files_transferred_for_tel(tel_id):
    """Check that there is a flag indicating the end of transferring raw data."""
    # TODO: This function is to be removed. The raw files are not being transferred
    #  to RAID. We should check for the end of observation instead (around 8:00 UTC)
    night_dir = lstdate_to_dir(options.date)
    end_of_transfer = Path(cfg.get(tel_id, "ENDOFRAWTRANSFERDIR")) / night_dir
    flag_file = end_of_transfer / cfg.get("LSTOSA", "end_of_activity")

    if flag_file.exists():
        log.info(f"Files for {options.date} {tel_id} are completely transferred to raid")
        return True
    else:
        log.warning(f"File {flag_file} not found!")
        log.info(
            f"Files for {options.date} {tel_id} are not yet "
            f"transferred to raid. Expecting more raw data"
        )
        return False


def are_raw_files_transferred() -> bool:
    if options.tel_id != "ST":
        return are_raw_files_transferred_for_tel(options.tel_id)

    return False


def get_check_raw_dir() -> Path:
    raw_dir = get_raw_dir()
    log.debug(f"Raw directory: {raw_dir}")

    if not raw_dir.exists():
        log.error(f"Raw directory {raw_dir} does not exist")
        sys.exit(1)
    else:
        # check that it contains raw files
        files = raw_dir.glob("*.fits.fz")
        if not files:
            log.error(f"Empty raw directory {raw_dir}")
    return raw_dir


def get_raw_dir() -> Path:
    night_dir = lstdate_to_dir(options.date)
    r0_dir = Path(cfg.get(options.tel_id, "R0_DIR")) / night_dir
    return r0_dir if options.tel_id in ["LST1", "LST2"] else None
