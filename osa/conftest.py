import pytest

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import extractruns, extractsubruns, extractsequences
from osa.nightsummary.nightsummary import run_summary_table
from osa.scripts.tests.test_osa_scripts import run_program
from osa.utils.utils import lstdate_to_dir

date = "2020_01_17"
nightdir = lstdate_to_dir(date)
prod_id = "v0.1.0"
dl1_prod_id = cfg.get("LST1", "DL1_PROD_ID")
dl2_prod_id = cfg.get("LST1", "DL2_PROD_ID")


@pytest.fixture(scope="session")
def base_test_dir(tmp_path_factory):
    """Creates a temporary directory for the tests."""
    return tmp_path_factory.mktemp("test_files")


@pytest.fixture(scope="session")
def running_analysis_dir(base_test_dir):
    analysis_dir = base_test_dir / "running_analysis" / nightdir / prod_id
    analysis_dir.mkdir(parents=True, exist_ok=True)
    return analysis_dir


@pytest.fixture(scope="session")
def r0_dir(base_test_dir):
    r0_directory = base_test_dir / "R0" / nightdir
    r0_directory.mkdir(parents=True, exist_ok=True)
    return r0_directory


@pytest.fixture(scope="session")
def r0_data(r0_dir):
    r0_file_1 = r0_dir / "LST-1.1.Run01805.0000.fits.fz"
    r0_file_2 = r0_dir / "LST-1.1.Run01806.0000.fits.fz"
    r0_file_1.touch()
    r0_file_2.touch()
    return r0_file_1, r0_file_2


@pytest.fixture(scope="session")
def dl1b_subdir(running_analysis_dir):
    dl1ab_directory = running_analysis_dir / dl1_prod_id
    dl1ab_directory.mkdir(parents=True, exist_ok=True)
    return dl1ab_directory


@pytest.fixture(scope="session")
def dl2_subdir(running_analysis_dir):
    dl2_directory = running_analysis_dir / dl2_prod_id
    dl2_directory.mkdir(parents=True, exist_ok=True)
    return dl2_directory


@pytest.fixture(scope="session")
def test_calibration_data(running_analysis_dir):
    """Mock calibration files for testing."""
    calib_file = running_analysis_dir / "calibration.Run01805.0000.h5"
    drs4_file = running_analysis_dir / "drs4_pedestal.Run01804.0000.fits"
    time_file = running_analysis_dir / "time_calibration.Run01805.0000.h5"
    calib_file.touch()
    drs4_file.touch()
    time_file.touch()
    return calib_file, drs4_file, time_file


@pytest.fixture(scope="session")
def test_observed_data(running_analysis_dir, dl1b_subdir, dl2_subdir):
    """Mock observed data files for testing."""
    dl1_file = running_analysis_dir / "dl1_LST-1.Run01808.0011.h5"
    muons_file = running_analysis_dir / "muons_LST-1.Run01808.0011.fits"
    dl1ab_file = dl1b_subdir / "dl1_LST-1.Run01808.0011.h5"
    datacheck_file = dl1b_subdir / "datacheck_dl1_LST-1.Run01808.0011.h5"
    dl2_file = dl2_subdir / "dl2_LST-1.Run01808.0011.h5"
    dl1_file.touch()
    muons_file.touch()
    dl1ab_file.touch()
    datacheck_file.touch()
    dl2_file.touch()
    return dl1_file, dl1ab_file, dl2_file, muons_file, datacheck_file


@pytest.fixture(scope="session")
def run_summary():
    """Creates a sequence list from a run summary file."""
    return run_summary_table(date)


@pytest.fixture(scope="session")
def sequence_list(running_analysis_dir, run_summary):
    """Creates a sequence list from a run summary file."""
    options.directory = running_analysis_dir
    options.simulate = True

    subrun_list = extractsubruns(run_summary)
    run_list = extractruns(subrun_list)
    return extractsequences(run_list)


@pytest.fixture(scope="session")
def sequence_file_list(running_analysis_dir):
    run_program("sequencer", "-d", "2020_01_17", "--no-submit", "-t", "LST1")
    return [
        running_analysis_dir / "sequence_LST1_01805.py",
        running_analysis_dir / "sequence_LST1_01807.py",
        running_analysis_dir / "sequence_LST1_01808.py",
    ]


@pytest.fixture(scope="session")
def txt_file_test(running_analysis_dir):
    from osa.utils.iofile import write_to_file
    file_name = running_analysis_dir / "test.txt"
    write_to_file(file_name, 'This is a test')
    return file_name
