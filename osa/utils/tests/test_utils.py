import datetime
import os

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import lstdate_to_dir

options.date = "2020_01_17"
options.tel_id = "LST1"
options.prod_id = "v0.1.0_v01"


def test_getcurrentdate():
    from osa.utils.utils import getcurrentdate

    # Both having the same separator
    now = datetime.datetime.utcnow()
    if now.hour < 12:
        # In our convention, date changes at 12:00 pm
        yesterday_lst_date = now - datetime.timedelta(hours=12)
        assert getcurrentdate("_") == yesterday_lst_date.strftime("%Y_%m_%d")
    else:
        assert getcurrentdate("_") == now.strftime("%Y_%m_%d")


def test_night_directory(running_analysis_dir):
    from osa.utils.utils import night_directory

    assert night_directory().resolve() == running_analysis_dir


def test_lstdate_to_number():
    from osa.utils.utils import lstdate_to_number

    assert lstdate_to_number("2020_01_01") == "20200101"


def test_get_lstchain_version():
    from osa.utils.utils import get_lstchain_version
    from lstchain import __version__

    assert get_lstchain_version() == "v" + __version__


def test_get_prod_id():
    from osa.utils.utils import get_prod_id

    prod_id = cfg.get(options.tel_id, "PROD-ID")
    assert get_prod_id() == prod_id


def test_date_in_yymmdd():
    from osa.utils.utils import date_in_yymmdd

    assert date_in_yymmdd("20200113") == "20_01_13"


def test_destination_dir():
    from osa.utils.utils import destination_dir

    options.date = "2020_01_17"
    datedir = lstdate_to_dir(options.date)
    options.dl1_prod_id = cfg.get("LST1", "DL1-PROD-ID")
    options.dl2_prod_id = cfg.get("LST1", "DL2-PROD-ID")
    options.prod_id = cfg.get("LST1", "PROD-ID")
    basedir = cfg.get("LST1", "DIR")

    data_types = {
        "DL1AB": "DL1",
        "DATACHECK": "DL1",
        "PEDESTAL": "calibration",
        "CALIB": "calibration",
        "TIMECALIB": "calibration",
        "MUON": "DL1",
        "DL2": "DL2",
    }

    for concept, dst in data_types.items():
        directory = destination_dir(concept, create_dir=False)
        if concept in ["DL1AB", "DATACHECK"]:
            expected_directory = os.path.join(
                basedir, dst, datedir, options.prod_id, options.dl1_prod_id
            )
        elif concept == "DL2":
            expected_directory = os.path.join(
                basedir, dst, datedir, options.prod_id, options.dl2_prod_id
            )
        else:
            expected_directory = os.path.join(basedir, dst, datedir, options.prod_id)
        assert directory == expected_directory


def test_time_to_seconds():
    from osa.utils.utils import time_to_seconds

    seconds_with_day = time_to_seconds("2-02:27:15")
    assert seconds_with_day == 2 * 24 * 3600 + 2 * 3600 + 27 * 60 + 15
    seconds = time_to_seconds("02:27:15")
    assert seconds == 2 * 3600 + 27 * 60 + 15
