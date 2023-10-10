from datetime import datetime
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import date_to_dir

options.date = datetime.fromisoformat("2020-01-17")
options.prod_id = "v0.1.1"


def test_get_calibration_file(r0_data, merged_run_summary):
    from osa.paths import get_calibration_filename

    for file in r0_data:
        assert file.exists()
    file = get_calibration_filename(1809, options.prod_id)
    file.exists()


def test_get_drs4_pedestal_file(r0_data, merged_run_summary):
    from osa.paths import get_drs4_pedestal_filename

    for file in r0_data:
        assert file.exists()
    file = get_drs4_pedestal_filename(1804, options.prod_id)
    file.exists()


def test_pedestal_ids_file_exists(pedestal_ids_file):
    from osa.paths import pedestal_ids_file_exists

    pedestal_ids_file.exists()
    assert pedestal_ids_file_exists(1808) is True


def test_get_datacheck_file(datacheck_dl1_files):
    from osa.paths import get_datacheck_files

    for file in datacheck_dl1_files:
        assert file.exists()
    dl1_path = Path("test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84")
    files = get_datacheck_files(pattern="datacheck*.pdf", directory=dl1_path)
    expected_files = [
        dl1_path / "datacheck_dl1_LST-1.Run01808.pdf",
        dl1_path / "datacheck_dl1_LST-1.Run01807.pdf",
    ]
    assert set(files) == set(expected_files)


def test_destination_dir():
    from osa.paths import destination_dir

    datedir = date_to_dir(options.date)
    options.dl1_prod_id = cfg.get("LST1", "DL1_PROD_ID")
    options.dl2_prod_id = cfg.get("LST1", "DL2_PROD_ID")
    options.prod_id = cfg.get("LST1", "PROD_ID")
    base_directory = cfg.get("LST1", "BASE")
    base_path = Path(base_directory)

    data_types = {
        "INTERLEAVED": "DL1",
        "DL1AB": "DL1",
        "DATACHECK": "DL1",
        "MUON": "DL1",
        "DL2": "DL2",
    }

    for concept, dst_dir in data_types.items():
        directory = destination_dir(concept, create_dir=False)
        if concept == "DL1AB":
            expected_directory = (
                base_path / dst_dir / datedir / options.prod_id / options.dl1_prod_id
            )
        elif concept == "DATACHECK":
            expected_directory = (
                base_path / dst_dir / datedir / options.prod_id / options.dl1_prod_id / "datacheck"
            )
        elif concept == "MUON":
            expected_directory = (
                base_path / dst_dir / datedir / options.prod_id / "muons"
            )
        elif concept == "INTERLEAVED":
            expected_directory = (
                base_path / dst_dir / datedir / options.prod_id / "interleaved"
            )
        elif concept == "DL2":
            expected_directory = (
                base_path / dst_dir / datedir / options.prod_id / options.dl2_prod_id
            )

        assert directory == expected_directory


def test_get_run_date(merged_run_summary):
    from osa.paths import get_run_date

    assert merged_run_summary.exists()

    assert get_run_date(1808) == datetime(2020,1,17)

    assert get_run_date(1200) == datetime(2020,1,17)
