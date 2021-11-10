import os
import tempfile
from pathlib import Path

import pytest

from osa.configs import options
from osa.nightsummary.extract import extractruns, extractsubruns, extractsequences
from osa.nightsummary.nightsummary import run_summary_table

options.date = "2020_01_17"


@pytest.fixture(scope="session")
def temp_dir():
    """Shared temporal directory for the tests."""
    with tempfile.TemporaryDirectory(prefix="test_osa") as d:
        yield Path(d)


@pytest.fixture(scope="session")
def test_data():
    """
    Create dummy test directory and files to test closer
    """
    test_dir = "testfiles"
    date = "20200117"
    prod_id = "v0.1.0_v01"
    dl1_prod_id = "tailcut84"

    raw_dir = os.path.join(test_dir, "R0", date)
    running_analysis = os.path.join(test_dir, "running_analysis", date, prod_id)
    dl1ab_directory = os.path.join(running_analysis, dl1_prod_id)

    os.makedirs(test_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(running_analysis, exist_ok=True)
    os.makedirs(dl1ab_directory, exist_ok=True)

    # Dummy files in running_analysis directory
    fd, dl1_file = tempfile.mkstemp(prefix="dl1_", suffix=".h5", dir=running_analysis)
    fd, dl1ab_file = tempfile.mkstemp(prefix="dl1_", suffix=".h5", dir=dl1ab_directory)
    fd, dl2_file = tempfile.mkstemp(prefix="dl2_", suffix=".h5", dir=running_analysis)
    fd, muons_file = tempfile.mkstemp(prefix="muons_", suffix=".fits", dir=running_analysis)
    fd, dcheck_file = tempfile.mkstemp(prefix="datacheck_dl1_", suffix=".h5", dir=dl1ab_directory)
    fd, calib_file = tempfile.mkstemp(prefix="calibration_", suffix=".hdf5", dir=running_analysis)
    fd, drs4_file = tempfile.mkstemp(prefix="drs4_", suffix=".fits", dir=running_analysis)
    fd, time_file = tempfile.mkstemp(
        prefix="time_calibration_", suffix=".hdf5", dir=running_analysis
    )

    return test_dir, dl1_file, raw_dir, running_analysis, dl1ab_directory, dl1ab_file, \
           dl2_file, muons_file, dcheck_file, calib_file, drs4_file, time_file


@pytest.fixture(scope="session")
def run_summary():
    """
    Creates a sequence list from a run summary file
    """
    # building the sequences
    return run_summary_table(options.date)


@pytest.fixture(scope="session")
def sequence_list(test_data, run_summary):
    """
    Creates a sequence list from a run summary file
    """
    # building the sequences
    options.directory = test_data[3]
    options.simulate = True

    subrun_list = extractsubruns(run_summary)
    run_list = extractruns(subrun_list)
    return extractsequences(run_list)
