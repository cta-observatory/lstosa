import datetime
import os
import subprocess as sp
from textwrap import dedent

import pytest

from osa.configs import options

ALL_SCRIPTS = [
    "sequencer",
    "closer",
    "calibrationsequence",
    "copy_datacheck",
    "datasequence",
    "show_run_summary",
]


def run_program(*args):
    result = sp.run(args, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8")

    if result.returncode != 0:
        raise ValueError(
            f"Running {args[0]} failed with return code {result.returncode}"
            f", output: \n {result.stdout}"
        )
    else:
        return result


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_all_help(script):
    """Test for all scripts if at least the help works."""
    run_program(script, "--help")


def test_simulated_sequencer():
    rc = run_program(
        "sequencer", "-c", "cfg/sequencer.cfg", "-d", "2020_01_17", "-s", "-t", "LST1"
    )
    assert rc.returncode == 0
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assert dedent(
        f"""\
    =========================== Starting sequencer.py at {now} UTC for LST, Telescope: LST1, Night: 2020_01_17 ===========================
    Tel   Seq  Parent  Type      Run   Subruns  Source  Wobble  Action  Tries  JobID  State  Host  CPU_time  Walltime  Exit  DL1%  MUONS%  DL1AB%  DATACHECK%  DL2%  
    LST1    0  None    PEDCALIB  1805  5        None    None    None    None   None   None   None  None      None      None  None  None    None    None        None  
    LST1    1       0  DATA      1807  19       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    2       0  DATA      1808  35       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    3       0  DATA      1809  18       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    4       0  DATA      1810  5        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    5       0  DATA      1812  2        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    6       0  DATA      1813  1        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    7       0  DATA      1814  48       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    8       0  DATA      1815  82       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1    9       0  DATA      1816  83       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   10       0  DATA      1817  71       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   11       0  DATA      1818  72       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   12       0  DATA      1819  96       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   13       0  DATA      1820  77       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   14       0  DATA      1821  47       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   15       0  DATA      1822  102      None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   16       0  DATA      1823  68       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   17       0  DATA      1824  79       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   18       0  DATA      1825  69       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   19       0  DATA      1826  6        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
    LST1   20       0  DATA      1827  1        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0 
    """
    )


def test_sequencer(sequence_file):
    assert sequence_file[0].exists()
    assert sequence_file[1].exists()


def test_autocloser(running_analysis_dir):
    result = run_program(
        "python",
        "osa/scripts/autocloser.py",
        "-c",
        "cfg/sequencer.cfg",
        "-d",
        "2020_01_17",
        "-t",
        "LST1",
    )
    assert os.path.exists(
        running_analysis_dir
    )  # Check that the analysis directory exists
    assert result.stdout.split()[-1] == "Exit"
    assert os.path.exists(
        "./test_osa/test_files0/running_analysis/20200117/v0.1.0_v01/AutoCloser_Incidences_tmp.txt"
    )


def test_closer(r0_dir, running_analysis_dir):
    run_program(
        "closer", "-c", "cfg/sequencer.cfg", "-y", "-v", "-t", "-d", "2020_01_17", "LST1"
    )
    assert os.path.exists(r0_dir)
    assert running_analysis_dir.exists()
    # Check that files have been moved to their final destinations
    assert os.path.exists("./test_osa/test_files0/DL1/20200117/v0.1.0_v01/tailcut84")
    assert os.path.exists("./test_osa/test_files0/DL2/20200117/v0.1.0_v01")
    assert os.path.exists("./test_osa/test_files0/calibration/20200117/v01")


def test_datasequence(running_analysis_dir):
    drs4_file = "drs4_pedestal.Run00001.0000.fits"
    calib_file = "calibration.Run00002.0000.hdf5"
    timecalib_file = "time_calibration.Run00002.0000.hdf5"
    drive_file = "drive_log_20200117.txt"
    runsummary_file = "RunSummary_20200117.ecsv"
    prod_id = "v0.1.0_v01"
    run_number = "00003.0000"
    options.directory = running_analysis_dir

    output = run_program(
        "datasequence",
        "-c",
        "cfg/sequencer.cfg",
        "-d",
        "2020_01_17",
        "-s",
        "--prod-id",
        prod_id,
        drs4_file,
        calib_file,
        timecalib_file,
        drive_file,
        runsummary_file,
        run_number,
        "LST1",
    )
    assert output.returncode == 0


def test_calibrationsequence(running_analysis_dir):
    drs4_file = "drs4_pedestal.Run02005.0000.fits"
    calib_file = "calibration.Run02006.0000.hdf5"
    runsummary_file = "RunSummary_20200117.ecsv"
    prod_id = "v0.1.0_v01"
    drs4_run_number = "02005"
    pedcal_run_number = "02006"
    options.directory = running_analysis_dir

    output = run_program(
        "calibrationsequence",
        "-c",
        "cfg/sequencer.cfg",
        "-d",
        "2020_01_17",
        "-s",
        "--prod-id",
        prod_id,
        drs4_file,
        calib_file,
        drs4_run_number,
        pedcal_run_number,
        runsummary_file,
        "LST1",
    )
    assert output.returncode == 0


def test_is_sequencer_successful(run_summary):
    from osa.scripts.closer import is_sequencer_successful, is_finished_check

    seq_tuple = is_finished_check(run_summary)
    assert is_sequencer_successful(seq_tuple) is True
