import logging
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_dir

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "get_check_raw_dir",
    "get_raw_dir",
    "is_raw_data_available"
]


def get_check_raw_dir() -> Path:
    """Get the raw directory and check if it contains raw files."""
    raw_dir = get_raw_dir()
    log.debug(f"Raw directory: {raw_dir}")

    if not raw_dir.exists():
        raise IOError(f"Raw directory {raw_dir} does not exist")

    # check that it contains raw files
    files = raw_dir.glob("*.fits.fz")
    if not files:
        raise IOError(f"Empty raw directory {raw_dir}")

    return raw_dir


def get_raw_dir() -> Path:
    night_dir = lstdate_to_dir(options.date)
    r0_dir = Path(cfg.get(options.tel_id, "R0_DIR")) / night_dir
    return r0_dir if options.tel_id in ["LST1", "LST2"] else None


def is_raw_data_available() -> bool:
    """Get the raw directory and check its existence."""
    answer = False
    if options.tel_id != "ST":
        raw_dir = get_check_raw_dir()
        if raw_dir.exists():
            answer = True
    else:
        answer = True
    return answer
