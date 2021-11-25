import pytest


@pytest.mark.tryfirst
def test_basedir_available(base_test_dir, r0_data):

    assert base_test_dir.is_dir()

    for data_file in r0_data:
        assert data_file.exists()
