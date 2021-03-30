import os
from pathlib import Path

from osa.configs import options

extra_files = Path(os.getenv("OSA_TEST_DATA", "extra"))
datasequence_history_file = extra_files / "history_files/sequence_LST1_04185.0010.history"
calibration_history_file = extra_files / "history_files/sequence_LST1_04183.history"
options.date = "2020_01_17"
options.tel_id = "LST1"


def test_historylevel():
    from osa.jobs.job import historylevel

    options.dl1_prod_id = "tailcut84"
    options.dl2_prod_id = "model1"

    level, rc = historylevel(datasequence_history_file, "DATA")
    assert level == 0
    assert rc == 0

    level, rc = historylevel(calibration_history_file, "PEDCALIB")
    assert level == 0
    assert rc == 0

    options.dl1_prod_id = "tailcut84"
    options.dl2_prod_id = "model2"

    level, rc = historylevel(datasequence_history_file, "DATA")
    assert level == 1
    assert rc == 0


def test_preparejobs(test_data, sequence_list):
    from osa.jobs.job import preparejobs

    options.simulate = False
    options.directory = test_data[3]
    preparejobs(sequence_list)
    expected_calib_script = os.path.join(test_data[3], "sequence_LST1_01805.py")
    expected_data_script = os.path.join(test_data[3], "sequence_LST1_01807.py")
    assert os.path.isfile(os.path.abspath(expected_calib_script))
    assert os.path.isfile(os.path.abspath(expected_data_script))


def test_setsequencefilenames(test_data, sequence_list):
    from osa.jobs.job import setsequencefilenames

    for sequence in sequence_list:
        setsequencefilenames(sequence)
        assert sequence.script == os.path.join(test_data[3], f"sequence_LST1_{sequence.run:05d}.py")
