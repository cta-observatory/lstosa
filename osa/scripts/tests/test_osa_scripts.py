import subprocess as sp

import pytest

ALL_SCRIPTS = [
    "sequencer",
    "closer",
    # "autocloser",
    # "calibrationsequence",
    "copy_datacheck",
    # "datasequence",
    # "provprocess",
    # "simulate_processing",
]


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_all_help(script):
    """Test for all scripts if at least the help works."""
    run_program(script, "--help")


def run_program(*args):
    result = sp.run(args, stdout=sp.PIPE, stderr=sp.STDOUT, encoding='utf-8')

    if result.returncode != 0:
        raise ValueError(
            f"Running {args[0]} failed with return code {result.returncode}"
            f", output: \n {result.stdout}"
        )


def test_sequencer():
    run_program(
        "sequencer", "-c", "cfg/sequencer_test.cfg", "-d", "2020_01_17", "-t", "-s", "LST1"
    )


def test_closer():
    run_program(
        "closer", "-c", "cfg/sequencer_test.cfg", "-y", "-s", "-d", "2020_01_17", "LST1"
    )


def test_datasequence(temp_dir):

    drs4_file = "drs4_pedestal.Run00001.0000.fits"
    calib_file = "calibration.Run00002.0000.hdf5"
    timecalib_file = "time_calibration.Run00002.0000.hdf5"
    drive_file = "drive_log_20200117.txt"
    runsummary_file = "RunSummary_20200117.ecsv"
    prod_id = "v0.1.0"
    run_number = "00003.0000"
    analysis_dir = temp_dir / "running_analysis/20200117/v0.1.0"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    run_program(
        "python", "osa/scripts/datasequence.py",
        "-c", "cfg/sequencer_test.cfg",
        "-d", "2020_01_17", "-s",
        "--prod-id", prod_id,
        drs4_file,
        calib_file,
        timecalib_file,
        drive_file,
        runsummary_file,
        run_number,
        "LST1"
    )
