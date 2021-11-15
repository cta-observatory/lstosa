import os
import shutil

import pytest


@pytest.mark.tryfirst
def test_dirs_available(base_test_dir, running_analysis_dir, r0_dir):
    """Assure that the test directories are available."""
    assert base_test_dir.is_dir()
    assert running_analysis_dir.is_dir()
    assert r0_dir.is_dir()


def test_files_available(test_observed_data):
    """Function to clean the test files created by the previous test."""
    assert os.path.exists(test_observed_data[0])  # DL1 file
    assert os.path.exists(test_observed_data[1])  # DL1ab file
    assert os.path.exists(test_observed_data[2])  # DL2 file
    assert os.path.exists(test_observed_data[3])  # Muons file
    assert os.path.exists(test_observed_data[4])  # Datacheck DL1 file


@pytest.mark.trylast
def test_clean_test_files(base_test_dir):
    """Function to clean the test files created by the previous test."""
    shutil.rmtree(base_test_dir)
    # FIXME: produce this prov.log file inside
    if os.path.exists("prov.log"):
        os.remove("prov.log")
