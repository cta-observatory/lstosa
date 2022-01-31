from osa.scripts.tests.test_osa_scripts import run_program


def test_dl3_stage(r0_data, drs4_time_calibration_files, run_summary_file, run_catalog):
    for file in r0_data:
        assert file.exists()
    assert run_catalog.exists()
    assert run_summary_file.exists()
    for files in drs4_time_calibration_files:
        assert files.exists()

    output = run_program(
        'dl3_stage',
        '-d',
        '2020_01_17',
        '-s',
        'LST1'
    )

    assert output.returncode == 0
    assert output.stdout.splitlines()[-1] == 'Simulate launching scripts'
