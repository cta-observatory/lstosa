import pytest


@pytest.mark.tryfirst
def test_basedir_available(
    base_test_dir, r0_data, drs4_time_calibration_files, calibration_file, drs4_baseline_file
):

    assert base_test_dir.is_dir()

    for data_file in r0_data:
        assert data_file.exists()

    for file in drs4_time_calibration_files:
        assert file.exists()

    assert drs4_baseline_file.exists()
    assert calibration_file.exists()
