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
        "sequencer", "-c", "cfg/sequencer_test.cfg", "-d", "2020_01_17", "-s", "LST1"
    )


def test_closer():
    run_program(
        "closer", "-c", "cfg/sequencer_test.cfg", "-y", "-s", "-d", "2020_01_17", "LST1"
    )
