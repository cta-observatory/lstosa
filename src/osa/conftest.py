"""
Mock test data set for testing OSA.

R0 files are in BASE_DIR/R0/YYYYMMDD

Calibration files follow the structure:
BASE_DIR/monitoring/PixelCalibration/Cat-A/<calibration_product>/YYYYMMDD/<version>
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
from osa.nightsummary.extract import extract_runs, extract_sequences
from osa.nightsummary.nightsummary import run_summary_table
from osa.scripts.tests.test_osa_scripts import run_program
from osa.utils.utils import date_to_dir
from datetime import datetime
import lstchain

date = datetime.fromisoformat("2020-01-17")
nightdir = date_to_dir(date)
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
    base_dir = monitoring_dir / "PixelCalibration" / "Cat-A"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


@pytest.fixture(scope="session")
def drive_log(monitoring_dir):
    drive_dir = monitoring_dir / "DrivePositioning"
    drive_file = drive_dir / "DrivePosition_log_20200117.txt"
    drive_dir.mkdir(parents=True, exist_ok=True)
    drive_file.touch()
    return drive_file


@pytest.fixture(scope="session")
def calibration_dir(calibration_base_dir):
    directory = calibration_base_dir / "calibration" / nightdir / f"v{lstchain.__version__}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def drs4_baseline_dir(calibration_base_dir):
    directory = calibration_base_dir / "drs4_baseline" / nightdir / f"v{lstchain.__version__}"
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
def systematic_correction_files(calibration_base_dir):
    directory = calibration_base_dir / "ffactor_systematics"
    directory1 = directory / "20200725" / "pro"
    directory2 = directory / "20201110" / "pro"
    directory1.mkdir(parents=True, exist_ok=True)
    directory2.mkdir(parents=True, exist_ok=True)
    file1 = directory1 / "ffactor_systematics_20200725.h5"
    file2 = directory2 / "ffactor_systematics_20201110.h5"
    sys_corr_file_list = [file1, file2]
    for file in sys_corr_file_list:
        file.touch()
    return sys_corr_file_list


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
def dl2_final_dir(base_test_dir):
    directory = base_test_dir / "DL2" / "20200117" / prod_id / dl2_prod_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def calibration_file(calibration_dir):
    """Mock calibration files for testing."""
    calib_file = calibration_dir / "calibration_filters_52.Run01809.0000.h5"
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
    interleaved_file = running_analysis_dir / "interleaved_LST-1.Run01808.0011.h5"
    dl1ab_file = dl1b_subdir / "dl1_LST-1.Run01808.0011.h5"
    datacheck_file = dl1b_subdir / "datacheck_dl1_LST-1.Run01808.0011.h5"
    dl2_file = dl2_subdir / "dl2_LST-1.Run01808.0011.h5"
    dl1_file.touch()
    muons_file.touch()
    interleaved_file.touch()
    dl1ab_file.touch()
    datacheck_file.touch()
    dl2_file.touch()
    return dl1_file, dl1ab_file, dl2_file, muons_file, datacheck_file, interleaved_file


@pytest.fixture(scope="session")
def dl2_merged(dl2_final_dir):
    file_1 = dl2_final_dir / "dl2_LST-1.Run01807.h5"
    file_2 = dl2_final_dir / "dl2_LST-1.Run01808.h5"
    file_1.touch()
    file_2.touch()

    return file_1, file_2


@pytest.fixture(scope="session")
def run_summary_file(run_summary_dir):

    summary_content = dedent(
        """\
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
    1803,6,DRS4,1579289727863850890,1579289712000000000,1579289727863850890,90,0,5863850400,ucts
    1804,6,DRS4,1579289727863850890,1579289712000000000,1579289727863850890,90,0,5863850400,ucts
    1805,5,PEDCALIB,1579291426030146503,1579291413000000000,1579291426030146503,90,0,2030146000,ucts
    1806,5,PEDCALIB,1579291932080485703,1579291917000000000,1579291932080485703,90,0,5080485200,ucts
    1807,11,DATA,1579292477145904430,1579292461000000000,1579292477145904430,90,0,6145904000,ucts
    1808,9,DATA,1579292985532016507,1579292975000000000,1579292985532016507,90,0,2532016000,ucts
    1809,5,PEDCALIB,1579291932080485703,1579291917000000000,1579291932080485703,90,0,5080485200,ucts"""
    )

    summary_file = run_summary_dir / "RunSummary_20200117.ecsv"
    summary_file.touch()
    summary_file.write_text(summary_content)
    return summary_file


@pytest.fixture(scope="session")
def run_summary_file_no_calib(run_summary_dir):

    summary_content = dedent(
        """\
    # %ECSV 1.0
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
    # - {date: '2022-09-23'}
    # - {lstchain_version: 0.9.6}
    # schema: astropy-2.0
    run_id,n_subruns,run_type,ucts_timestamp,run_start,dragon_reference_time,dragon_reference_module_id,dragon_reference_module_index,dragon_reference_counter,dragon_reference_source
    9379,21,DATA,-1,1663951379000000000,1663951379000000000,0,0,6463544800,run_start
    9380,44,DRS4,1663951929600198168,1663951917000000000,1663951929600198168,0,0,5600197900,ucts
    9381,41,DRS4,1663952467501824402,1663952457000000000,1663952467501824402,0,0,3501824100,ucts"""
    )

    summary_file = run_summary_dir / "RunSummary_20220923.ecsv"
    summary_file.touch()
    summary_file.write_text(summary_content)
    return summary_file


@pytest.fixture(scope="session")
def run_summary_file_no_calib2(run_summary_dir):

    summary_content = dedent(
        """\
    # %ECSV 1.0
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
    # - {date: '2022-09-22'}
    # - {lstchain_version: 0.9.6}
    # schema: astropy-2.0
    run_id,n_subruns,run_type,ucts_timestamp,run_start,dragon_reference_time,dragon_reference_module_id,dragon_reference_module_index,dragon_reference_counter,dragon_reference_source
    9326,4,DRS4,1663877740307617642,1663877725000000000,1663877740307617642,0,0,6307617400,ucts
    9327,3,ERRPEDCALIB,1663878027873304499,1663878015000000000,1663878027873304499,0,0,3873304200,ucts
    9258,5,PEDCALIB,1663796417227478675,1663796405000000000,1663796417227478675,90,0,5227478400,ucts"""
    )

    summary_file = run_summary_dir / "RunSummary_20220922.ecsv"
    summary_file.touch()
    summary_file.write_text(summary_content)
    return summary_file


@pytest.fixture(scope="session")
def merged_run_summary(base_test_dir):
    """Mock merged run summary file for testing."""
    summary_content = dedent(
        """\
    # %ECSV 1.0
    # ---
    # datatype:
    # - {name: date, datatype: string}
    # - {name: run_id, datatype: int64}
    # - {name: run_type, datatype: string}
    # - {name: n_subruns, datatype: int64}
    # - {name: run_start, datatype: string}
    # - {name: ra, unit: deg, datatype: float64}
    # - {name: dec, unit: deg, datatype: float64}
    # - {name: alt, unit: rad, datatype: float64}
    # - {name: az, unit: rad, datatype: float64}
    # meta: !!omap
    # - __serialized_columns__:
    #     run_start:
    #       __class__: astropy.time.core.Time
    #       format: isot
    #       in_subfmt: '*'
    #       out_subfmt: '*'
    #       precision: 3
    #       scale: utc
    #       value: !astropy.table.SerializedColumn {name: run_start}
    # schema: astropy-2.0
    date run_id run_type n_subruns run_start ra dec alt az
    2019-11-23 1611 DRS4 5 2019-11-23T22:14:09.000 22.0 3.1 0.96 2.3
    2019-11-23 1614 PEDCALIB 10 2019-11-23T23:33:59.000 4.6 2.1 1.1 4.5
    2019-11-23 1615 DATA 61 2019-11-23T23:41:13.000 8.1 45.1 1.5 4.6
    2019-11-23 1616 DATA 62 2019-11-24T00:11:52.000 3.2 4.2 0.9 1.6
    2020-01-17 1804 DRS4 35 2020-01-18T00:44:06.000 2.1 42.9 11.3 4.7
    2020-01-17 1805 PEDCALIB 62 2020-01-18T00:11:52.000 13.9 21.9 17.9 1.6
    2020-01-17 1806 PEDCALIB 35 2020-01-18T00:44:06.000 8.6 29.1 45.5 6.9
    2020-01-17 1807 DATA 35 2020-01-18T00:44:06.000 6.6 2.8 70.4 10.1
    2020-01-17 1808 DATA 35 2020-01-18T00:44:06.000 8.6 9.2 60.8 3.2
    2020-01-17 1809 PEDCALIB 4 2020-01-18T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-22 9326 DRS4 4 2021-09-22T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-22 9327 ERRPEDCALIB 3 2021-09-22T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-22 9258 PEDCALIB 5 2021-09-22T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-23 9379 DATA 21 2021-09-23T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-23 9380 DRS4 44 2021-09-23T00:44:06.000 6.9 4.2 16.8 11.2
    2022-09-23 9381 DRS4 41 2021-09-23T00:44:06.000 6.9 4.2 16.8 11.2"""
    )

    merged_summary_dir = base_test_dir / "OSA/Catalog"
    merged_summary_dir.mkdir(parents=True, exist_ok=True)

    file = merged_summary_dir / "merged_RunSummary.ecsv"
    file.touch()
    file.write_text(summary_content)
    return file


@pytest.fixture(scope="session")
def run_summary(run_summary_file):
    """Creates a sequence list from a run summary file."""
    assert run_summary_file.exists()
    return run_summary_table(date)


@pytest.fixture(scope="session")
def pedestal_ids_file(base_test_dir):
    """Mock pedestal ids file for testing."""
    pedestal_ids_dir = base_test_dir / "auxiliary/PedestalFinder/20200117"
    pedestal_ids_dir.mkdir(parents=True, exist_ok=True)
    file = pedestal_ids_dir / "pedestal_ids_Run01808.0000.h5"
    file.touch()
    return file


@pytest.fixture(scope="session")
def sequence_list(
    running_analysis_dir,
    run_summary,
    drs4_time_calibration_files,
    systematic_correction_files,
    r0_data,
    pedestal_ids_file,
    merged_run_summary,
):
    """Creates a sequence list from a run summary file."""
    options.directory = running_analysis_dir
    options.simulate = True
    options.test = True

    for file in drs4_time_calibration_files:
        assert file.exists()

    for file in systematic_correction_files:
        assert file.exists()

    for file in r0_data:
        assert file.exists()

    assert pedestal_ids_file.exists()
    assert merged_run_summary.exists()

    run_list = extract_runs(run_summary)
    options.test = False
    return extract_sequences(options.date, run_list)


@pytest.fixture(scope="session")
def sequence_file_list(
    running_analysis_dir,
    run_summary_file,
    run_catalog,
    drs4_time_calibration_files,
    systematic_correction_files,
    r0_data,
):
    for r0_file in r0_data:
        assert r0_file.exists()

    for file in drs4_time_calibration_files:
        assert file.exists()

    for file in systematic_correction_files:
        assert file.exists()

    assert run_summary_file.exists()
    assert run_catalog.exists()

    run_program("sequencer", "-d", "2020-01-17", "--no-submit", "-t", "LST1")
    # First sequence in the list corresponds to the calibration run 1809
    return [
        running_analysis_dir / "sequence_LST1_01809.py",
        running_analysis_dir / "sequence_LST1_01807.py",
        running_analysis_dir / "sequence_LST1_01808.py",
    ]


@pytest.fixture(scope="session")
def txt_file_test(running_analysis_dir):
    from osa.utils.iofile import write_to_file

    options.simulate = False
    file = running_analysis_dir / "test.txt"
    write_to_file(file, "This is a test")
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
    directory = base_test_dir / "OSA" / "DL1DataCheck_LongTerm" / prod_id / date_to_dir(date)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def longterm_link_latest_dir(base_test_dir):
    directory = base_test_dir / "OSA" / "DL1DataCheck_LongTerm" / "night_wise" / "all"
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
    file = calibration_dir_log / "calibration_filters_52.Run01809.0000.pdf"
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
    source_information = dedent(
        """\
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
    1807,Crab,83.543,22.08
    1808,MadeUpSource,115.441,43.98"""
    )

    catalog_file = run_catalog_dir / "RunCatalog_20200117.ecsv"
    catalog_file.touch()
    catalog_file.write_text(source_information)
    return catalog_file


@pytest.fixture(scope="session")
def database(base_test_dir):
    import sqlite3

    osa_dir = base_test_dir / "OSA"
    osa_dir.mkdir(parents=True, exist_ok=True)
    db_file = osa_dir / "osa.db"
    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS processing
            (telescope, date, prod_id, start, end, is_finished)"""
        )
        cursor.connection.commit()
        yield cursor
