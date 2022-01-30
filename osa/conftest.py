"""
Mock test data set for testing OSA.

R0 files are in BASE_DIR/R0/YYYYMMDD

Calibration files follow the structure:
BASE_DIR/monitoring/PixelCalibration/LevelA/<calibration_product>/YYYYMMDD/<version>
Where <calibration_product> can be: drs4_baseline, calibration,
drs4_time_sampling_from_FF, ffactor_systematics
<version> will usually be "pro" (for production)

RunSummary file: BASE_DIR/monitoring/RunSummary/RunSummary_YYYMMDD.ecsv

Analysis products (dl1, dl2, muons and datacheck) are produced in the analysis directory:
BASE_DIR/running_analysis/YYYYMMDD/<prod_id>
"""

from textwrap import dedent

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
def monitoring_dir(base_test_dir):
    monitoring_dir = base_test_dir / "monitoring"
    monitoring_dir.mkdir(parents=True, exist_ok=True)
    return monitoring_dir


@pytest.fixture(scope="session")
def run_summary_dir(monitoring_dir):
    summary_dir = monitoring_dir / "RunSummary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    return summary_dir


@pytest.fixture(scope="session")
def run_catalog_dir(monitoring_dir):
    catalog_dir = monitoring_dir / "RunCatalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    return catalog_dir


@pytest.fixture(scope="session")
def calibration_base_dir(monitoring_dir):
    base_dir = monitoring_dir / "PixelCalibration" / "LevelA"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


@pytest.fixture(scope="session")
def calibration_dir(calibration_base_dir):
    directory = calibration_base_dir / "calibration" / nightdir / "pro"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def drs4_baseline_dir(calibration_base_dir):
    directory = calibration_base_dir / "drs4_baseline" / nightdir / "pro"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def drs4_time_calibration_files(calibration_base_dir):
    directory = calibration_base_dir / "drs4_time_sampling_from_FF"
    directory1 = directory / "20191124" / "pro"
    directory2 = directory / "20210321" / "pro"
    directory3 = directory / "20210902" / "pro"
    directory1.mkdir(parents=True, exist_ok=True)
    directory2.mkdir(parents=True, exist_ok=True)
    directory3.mkdir(parents=True, exist_ok=True)
    file1 = directory1 / "time_calibration.Run01625.0000.h5"
    file2 = directory2 / "time_calibration.Run04211.0000.h5"
    file3 = directory3 / "time_calibration.Run05979.0000.h5"
    time_file_list = [file1, file2, file3]
    for file in time_file_list:
        file.touch()
    return time_file_list


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
    r0_files = []
    for i in range(4, 8):
        r0_file = r0_dir / f"LST-1.1.Run0180{i}.0000.fits.fz"
        r0_file.touch()
        r0_files.append(r0_file)
    return r0_files


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
def calibration_file(calibration_dir):
    """Mock calibration files for testing."""
    calib_file = calibration_dir / "calibration_filters_52.Run01805.0000.h5"
    calib_file.touch()
    return calib_file


@pytest.fixture(scope="session")
def drs4_baseline_file(drs4_baseline_dir):
    """Mock calibration files for testing."""
    drs4_file = drs4_baseline_dir / "drs4_pedestal.Run01804.0000.h5"
    drs4_file.touch()
    return drs4_file


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
def run_summary_file(run_summary_dir):

    summary_content = dedent("""\
    # %ECSV 0.9
    # ---
    # datatype:
    # - {name: run_id, datatype: int64}
    # - {name: n_subruns, datatype: int64}
    # - {name: run_type, datatype: string}
    # - {name: ucts_timestamp, datatype: int64}
    # - {name: run_start, datatype: int64}
    # - {name: dragon_reference_time, datatype: int64}
    # - {name: dragon_reference_module_id, datatype: int16}
    # - {name: dragon_reference_module_index, datatype: int16}
    # - {name: dragon_reference_counter, datatype: uint64}
    # - {name: dragon_reference_source, datatype: string}
    # delimiter: ','
    # meta: !!omap
    # - {date: '2020-01-17'}
    # - {lstchain_version: 0.7.0}
    # schema: astropy-2.0
    run_id,n_subruns,run_type,ucts_timestamp,run_start,dragon_reference_time,dragon_reference_module_id,dragon_reference_module_index,dragon_reference_counter,dragon_reference_source
    1804,6,DRS4,1579289727863850890,1579289712000000000,1579289727863850890,90,0,5863850400,ucts
    1805,5,PEDCALIB,1579291426030146503,1579291413000000000,1579291426030146503,90,0,2030146000,ucts
    1806,5,PEDCALIB,1579291932080485703,1579291917000000000,1579291932080485703,90,0,5080485200,ucts
    1807,11,DATA,1579292477145904430,1579292461000000000,1579292477145904430,90,0,6145904000,ucts
    1808,9,DATA,1579292985532016507,1579292975000000000,1579292985532016507,90,0,2532016000,ucts""")

    summary_file = run_summary_dir / 'RunSummary_20200117.ecsv'
    summary_file.touch()
    summary_file.write_text(summary_content)
    return summary_file


@pytest.fixture(scope="session")
def run_summary(run_summary_file):
    """Creates a sequence list from a run summary file."""
    assert run_summary_file.exists()
    return run_summary_table(date)


@pytest.fixture(scope="session")
def sequence_list(running_analysis_dir, run_summary, drs4_time_calibration_files):
    """Creates a sequence list from a run summary file."""
    options.directory = running_analysis_dir
    options.simulate = True
    options.test = True
    for file in drs4_time_calibration_files:
        assert file.exists()

    subrun_list = extractsubruns(run_summary)
    run_list = extractruns(subrun_list)
    options.test = False
    return extractsequences(run_list)


@pytest.fixture(scope="session")
def sequence_file_list(
        running_analysis_dir,
        run_summary_file,
        run_catalog,
        drs4_time_calibration_files,
        r0_data
):
    for r0_file in r0_data:
        assert r0_file.exists()
    for file in drs4_time_calibration_files:
        assert file.exists()
    assert run_summary_file.exists()
    assert run_catalog.exists()

    run_program("sequencer", "-d", "2020_01_17", "--no-submit", "-t", "LST1")
    return [
        running_analysis_dir / "sequence_LST1_01805.py",
        running_analysis_dir / "sequence_LST1_01807.py",
        running_analysis_dir / "sequence_LST1_01808.py",
    ]


@pytest.fixture(scope="session")
def txt_file_test(running_analysis_dir):
    from osa.utils.iofile import write_to_file
    options.simulate = False
    file = running_analysis_dir / "test.txt"
    write_to_file(file, 'This is a test')
    options.simulate = True
    return file


@pytest.fixture(scope="session")
def datacheck_dl1_files(base_test_dir):
    dl1b_dir = base_test_dir / "DL1" / "20200117" / "v0.1.0" / "tailcut84"
    dl1b_dir.mkdir(parents=True, exist_ok=True)
    pdf_file_1 = dl1b_dir / "datacheck_dl1_LST-1.Run01807.pdf"
    pdf_file_2 = dl1b_dir / "datacheck_dl1_LST-1.Run01808.pdf"
    pdf_file_1.touch()
    pdf_file_2.touch()
    return pdf_file_1, pdf_file_2


@pytest.fixture(scope="session")
def longterm_dir(base_test_dir):
    directory = base_test_dir / "OSA" / "DL1DataCheck_LongTerm" / "v0.1.0" / "20200117"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def daily_datacheck_dl1_files(longterm_dir):
    html_file = longterm_dir / "DL1_datacheck_20200117.html"
    h5_file = longterm_dir / "DL1_datacheck_20200117.h5"
    log_file = longterm_dir / "DL1_datacheck_20200117.log"
    html_file.touch()
    h5_file.touch()
    log_file.touch()
    return html_file, h5_file, log_file


@pytest.fixture(scope="session")
def calibration_check_plot(calibration_dir):
    calibration_dir_log = calibration_dir / "log"
    calibration_dir_log.mkdir(parents=True, exist_ok=True)
    file = calibration_dir_log / "calibration_filters_52.Run01805.0000.pdf"
    file.touch()
    return file


@pytest.fixture(scope="session")
def drs4_check_plot(drs4_baseline_dir):
    drs4_baseline_dir_log = drs4_baseline_dir / "log"
    drs4_baseline_dir_log.mkdir(parents=True, exist_ok=True)
    file = drs4_baseline_dir_log / "drs4_pedestal.Run01804.0000.pdf"
    file.touch()
    return file


@pytest.fixture(scope="session")
def run_catalog(run_catalog_dir):
    source_information = dedent("""\
    # %ECSV 1.0
    # ---
    # datatype:
    # - {name: run_id, datatype: int32}
    # - {name: source_name, datatype: string}
    # - {name: source_ra, datatype: float64}
    # - {name: source_dec, datatype: float64}
    # delimiter: ','
    # schema: astropy-2.0
    run_id,source_name,source_ra,source_dec
    1807,Source1,35.543,11.04
    1808,Source2,115.441,43.98""")

    catalog_file = run_catalog_dir / 'RunCatalog_20200117.ecsv'
    catalog_file.touch()
    catalog_file.write_text(source_information)
    return catalog_file

