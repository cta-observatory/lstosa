def test_get_calibration_file(r0_data):
    from osa.paths import get_calibration_file
    for file in r0_data:
        assert file.exists()
    file = get_calibration_file(1805)
    file.exists()


def test_get_drs4_pedestal_file(r0_data):
    from osa.paths import get_drs4_pedestal_file
    for file in r0_data:
        assert file.exists()
    file = get_drs4_pedestal_file(1804)
    file.exists()


def test_get_time_calibration_file(drs4_time_calibration_files):
    from osa.paths import get_time_calibration_file
    for file in drs4_time_calibration_files:
        assert file.exists()

    run = 1616
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 1625
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 1900
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 4211
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[1]

    run = 5000
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[1]

    run = 5979
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[2]

    run = 6000
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[2]


def test_pedestal_ids_file_exists(pedestal_ids_file):
    from osa.paths import pedestal_ids_file_exists
    pedestal_ids_file.exists()
    assert pedestal_ids_file_exists(1808) is True