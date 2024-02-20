from datetime import datetime
from pathlib import Path

import pytest

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import date_to_dir


def test_get_raw_dir():
    options.date = datetime.fromisoformat("2020-01-17")
    night_dir = date_to_dir(options.date)
    r0_dir = Path(cfg.get("LST1", "R0_DIR")) / night_dir
    from osa.raw import get_raw_dir

    assert get_raw_dir(options.date) == r0_dir


def test_get_check_raw_dir(r0_dir):
    options.date = datetime.fromisoformat("2020-01-18")
    from osa.raw import get_check_raw_dir

    with pytest.raises(OSError):
        assert get_check_raw_dir(options.date)

    options.date = datetime.fromisoformat("2020-01-17")
    raw_dir = get_check_raw_dir(options.date)
    assert raw_dir.resolve() == r0_dir


def test_is_raw_data_available(r0_data):
    from osa.raw import is_raw_data_available

    for file in r0_data:
        assert file.exists()

    options.date = datetime.fromisoformat("2020-01-17")
    assert is_raw_data_available(options.date) is True
