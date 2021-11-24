import pytest


@pytest.mark.tryfirst
def test_files_available(
        base_test_dir,
        test_observed_data,
        test_calibration_data,
        r0_data
):
    """Assure that the test directories and files are available."""
    assert base_test_dir.is_dir()
    assert r0_data[0].exists()  # R0 first file
    assert r0_data[1].exists()  # R0 second file
    assert test_calibration_data[0].exists()  # calib file
    assert test_calibration_data[1].exists()  # drs4 file
    assert test_calibration_data[2].exists()  # time_calib file
    assert test_observed_data[0].exists()  # DL1 file
    assert test_observed_data[1].exists()  # DL1ab file
    assert test_observed_data[2].exists()  # DL2 file
    assert test_observed_data[3].exists()  # Muons file
    assert test_observed_data[4].exists()  # Datacheck DL1 file
