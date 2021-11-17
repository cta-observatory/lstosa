import os

import pytest


@pytest.mark.tryfirst
def test_dirs_available(base_test_dir, running_analysis_dir, r0_dir):
    """Assure that the test directories are available."""
    assert base_test_dir.is_dir()
    assert running_analysis_dir.is_dir()
    assert r0_dir.is_dir()


def test_files_available(test_observed_data, r0_data):
    assert os.path.exists(r0_data[0])  # R0 first file
    assert os.path.exists(r0_data[1])  # R0 second file
    assert os.path.exists(test_observed_data[0])  # DL1 file
    assert os.path.exists(test_observed_data[1])  # DL1ab file
    assert os.path.exists(test_observed_data[2])  # DL2 file
    assert os.path.exists(test_observed_data[3])  # Muons file
    assert os.path.exists(test_observed_data[4])  # Datacheck DL1 file
