import datetime

import pytest

from osa.configs import options
from osa.configs.config import cfg

options.date = datetime.datetime.fromisoformat("2020-01-17")
options.tel_id = "LST1"
options.prod_id = "v0.1.0"


def test_analysis_path(running_analysis_dir):
    from osa.paths import analysis_path

    assert analysis_path("LST1").resolve() == running_analysis_dir


def test_get_lstchain_version():
    from osa.utils.utils import get_lstchain_version
    from lstchain import __version__

    assert get_lstchain_version() == f"v{__version__}"


def test_get_prod_id():
    from osa.utils.utils import get_prod_id

    prod_id = cfg.get(options.tel_id, "PROD_ID")
    assert get_prod_id() == prod_id


def test_date_to_dir():
    from osa.utils.utils import date_to_dir

    assert date_to_dir(options.date) == "20200117"


def test_time_to_seconds():
    from osa.utils.utils import time_to_seconds

    seconds_with_day = time_to_seconds("2-02:27:15")
    assert seconds_with_day == 2 * 24 * 3600 + 2 * 3600 + 27 * 60 + 15
    seconds = time_to_seconds("02:27:15")
    assert seconds == 2 * 3600 + 27 * 60 + 15
    seconds = time_to_seconds("27:15")
    assert seconds == 27 * 60 + 15
    assert time_to_seconds(None) == 0

    with pytest.raises(ValueError):
        time_to_seconds("12.11.11")


def test_stringify():
    from osa.utils.utils import stringify

    assert stringify(["command", "foo", "--bar"]) == "command foo --bar"


def test_gettag():
    from osa.utils.utils import gettag

    assert gettag() == "test_utils.py(test_gettag)"


def test_night_finished_flag(base_test_dir):
    from osa.utils.utils import night_finished_flag

    assert night_finished_flag() == base_test_dir / "OSA/Closer/20200117/v0.1.0/NightFinished.txt"


def test_create_lock(base_test_dir):
    from osa.utils.utils import create_lock

    lock_path = base_test_dir / "test_lock.closed"
    is_closed = create_lock(lock_path)
    assert is_closed is False
