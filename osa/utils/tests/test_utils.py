import datetime
from osa.configs import options
from osa.configs.config import cfg
import os


options.date = "2020_01_02"
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


def test_getnightdirectory():
    from osa.utils.utils import getnightdirectory, lstdate_to_dir
    analysis_dir = cfg.get(options.tel_id, "ANALYSISDIR")
    nightdir = lstdate_to_dir(options.date)
    assert getnightdirectory() == os.path.join(analysis_dir, nightdir, options.prod_id)


def test_lstdate_to_number():
    from osa.utils.utils import lstdate_to_number
    assert lstdate_to_number("2020_01_01") == "20200101"


def test_get_lstchain_version():
    from osa.utils.utils import get_lstchain_version
    assert get_lstchain_version().startswith("v")
    # Last line is version specific, needs to be changed
    assert get_lstchain_version() == "v0.6.3"


def test_get_prod_id():
    from osa.utils.utils import get_prod_id
    prod_id = cfg.get(options.tel_id, "PROD-ID")
    assert get_prod_id() == prod_id
