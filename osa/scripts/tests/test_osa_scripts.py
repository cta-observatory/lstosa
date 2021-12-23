import datetime
import os
import subprocess as sp
from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from osa.configs import options
from osa.scripts.closer import is_sequencer_successful, is_finished_check

ALL_SCRIPTS = [
    "sequencer",
    "closer",
    "copy_datacheck",
    "datasequence",
    "calibration_pipeline",
    "show_run_summary",
    "provprocess",
    "simulate_processing",
]

options.date = "2020_01_17"


def remove_provlog():
    log_file = Path("prov.log")
    if log_file.is_file():
        log_file.unlink()


def run_program(*args):
    result = sp.run(args, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8", check=True)

    if result.returncode != 0:
        raise ValueError(
            f"Running {args[0]} failed with return code {result.returncode}"
            f", output: \n {result.stdout}"
        )

    return result


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_all_help(script):
    """Test for all scripts if at least the help works."""
    run_program(script, "--help")


def test_simulate_processing(drs4_time_calibration_files, run_summary_file):

    for file in drs4_time_calibration_files:
        assert file.exists()

    assert run_summary_file.exists()

    remove_provlog()
    rc = run_program("simulate_processing", "-p", "--force")
    assert rc.returncode == 0

    prov_dl1_path = Path("./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/log")
    prov_dl2_path = Path("./test_osa/test_files0/DL2/20200117/v0.1.0/tailcut84_model1/log")
    prov_file_dl1 = prov_dl1_path / "r0_to_dl1_01807_prov.log"
    prov_file_dl2 = prov_dl2_path / "r0_to_dl2_01807_prov.log"
    json_file_dl1 = prov_dl1_path / "r0_to_dl1_01807_prov.json"
    json_file_dl2 = prov_dl2_path / "r0_to_dl2_01807_prov.json"
    pdf_file_dl1 = prov_dl1_path / "r0_to_dl1_01807_prov.pdf"
    pdf_file_dl2 = prov_dl2_path / "r0_to_dl2_01807_prov.pdf"

    assert prov_file_dl1.exists()
    assert prov_file_dl2.exists()
    assert pdf_file_dl1.exists()
    assert pdf_file_dl2.exists()

    with open(json_file_dl1) as file:
        dl1 = yaml.safe_load(file)
    assert len(dl1["entity"]) == 11
    assert len(dl1["activity"]) == 2
    assert len(dl1["used"]) == 9
    assert len(dl1["wasGeneratedBy"]) == 3

    with open(json_file_dl2) as file:
        dl2 = yaml.safe_load(file)
    assert len(dl2["entity"]) == 20
    assert len(dl2["activity"]) == 4
    assert len(dl2["used"]) == 17
    assert len(dl2["wasGeneratedBy"]) == 8

    rc = run_program("simulate_processing", "-p")
    assert rc.returncode == 0

    remove_provlog()
    rc = run_program("simulate_processing", "-p")
    assert rc.returncode == 0


def test_simulated_sequencer(drs4_time_calibration_files, run_summary_file):
    assert run_summary_file.exists()
    for file in drs4_time_calibration_files:
        assert file.exists()
    rc = run_program("sequencer", "-c", "cfg/sequencer.cfg", "-d", "2020_01_17", "-s", "-t", "LST1")
    assert rc.returncode == 0
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    assert rc.stdout == dedent(
        f"""\
        ================================== Starting sequencer.py at {now} UTC for LST, Telescope: LST1, Night: 2020_01_17 ==================================
        Tel   Seq  Parent  Type      Run   Subruns  Source  Wobble  Action  Tries  JobID  State  Host  CPU_time  Walltime  Exit  DL1%  MUONS%  DL1AB%  DATACHECK%  DL2%  
        LST1    0  None    PEDCALIB  1805  5        None    None    None    None   None   None   None  None      None      None  None  None    None    None        None  
        LST1    1       0  DATA      1807  11       None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
        LST1    2       0  DATA      1808  9        None    None    None    None   None   None   None  None      None      None     0       0       0           0     0  
        """)


def test_sequencer(sequence_file_list):
    for sequence_file in sequence_file_list:
        assert sequence_file.exists()


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
    assert os.path.exists(running_analysis_dir)
    assert result.stdout.split()[-1] == "Exit"
    assert os.path.exists(
        "./test_osa/test_files0/running_analysis/20200117/v0.1.0/"
        "AutoCloser_Incidences_tmp.txt"
    )


def test_closer(r0_dir, running_analysis_dir, test_observed_data):
    # First assure that the end of night flag is not set and remove it otherwise
    night_finished_flag = Path(
        "./test_osa/test_files0/OSA/Closer/20200117/v0.1.0/NightFinished.txt"
    )
    if night_finished_flag.exists():
        night_finished_flag.unlink()

    assert r0_dir.exists()
    assert running_analysis_dir.exists()
    for obs_file in test_observed_data:
        assert obs_file.exists()

    run_program(
        "closer", "-c", "cfg/sequencer.cfg", "-y", "-v", "-t", "-d", "2020_01_17", "LST1"
    )
    conda_env_export = running_analysis_dir / "log" / "conda_env.yml"
    closed_seq_file = running_analysis_dir / "sequence_LST1_01805.closed"

    # Check that files have been moved to their final destinations
    assert os.path.exists(
        "./test_osa/test_files0/DL1/20200117/v0.1.0/muons_LST-1.Run01808.0011.fits"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/dl1_LST-1.Run01808.0011.h5"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/"
        "datacheck_dl1_LST-1.Run01808.0011.h5"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL2/20200117/v0.1.0/tailcut84_model1/"
        "dl2_LST-1.Run01808.0011.h5"
    )
    # Assert that the link to dl1 and muons files have been created
    assert os.path.islink(
        "./test_osa/test_files0/running_analysis/20200117/"
        "v0.1.0/muons_LST-1.Run01808.0011.fits"
    )
    assert os.path.islink(
        "./test_osa/test_files0/running_analysis/20200117/"
        "v0.1.0/dl1_LST-1.Run01808.0011.h5"
    )

    assert night_finished_flag.exists()
    assert conda_env_export.exists()
    assert closed_seq_file.exists()


def test_datasequence(running_analysis_dir):
    drs4_file = "drs4_pedestal.Run00001.0000.fits"
    calib_file = "calibration.Run00002.0000.hdf5"
    timecalib_file = "time_calibration.Run00002.0000.hdf5"
    drive_file = "drive_log_20200117.txt"
    runsummary_file = "RunSummary_20200117.ecsv"
    prod_id = "v0.1.0"
    run_number = "00003.0000"
    options.directory = running_analysis_dir

    output = run_program(
        "datasequence",
        "--config",
        "cfg/sequencer.cfg",
        "--date=2020_01_17",
        "--simulate",
        f"--prod-id={prod_id}",
        f"--drs4-pedestal-file={drs4_file}",
        f"--pedcal-file={calib_file}",
        f"--time-calib-file={timecalib_file}",
        f"--drive-file={drive_file}",
        f"--run-summary={runsummary_file}",
        run_number,
        "LST1",
    )
    assert output.returncode == 0


def test_calibration_pipeline(running_analysis_dir):
    prod_id = "v0.1.0"
    drs4_run_number = "01805"
    pedcal_run_number = "01806"
    options.directory = running_analysis_dir

    output = run_program(
        "calibration_pipeline",
        "-c",
        "cfg/sequencer.cfg",
        "-d",
        "2020_01_17",
        "-s",
        "--prod-id",
        prod_id,
        "--drs4-pedestal-run",
        drs4_run_number,
        "--pedcal-run",
        pedcal_run_number,
        "LST1",
    )
    assert output.returncode == 0


def test_is_sequencer_successful(run_summary, running_analysis_dir):
    options.directory = running_analysis_dir
    seq_tuple = is_finished_check(run_summary)
    assert is_sequencer_successful(seq_tuple) is True


def test_drs4_pedestal_cmd(base_test_dir):
    from osa.scripts.calibration_pipeline import drs4_pedestal_command
    cmd = drs4_pedestal_command(drs4_pedestal_run_id="01804")
    expected_command = [
        'onsite_create_drs4_pedestal_file',
        '--run_number=01804',
        f'--base_dir={base_test_dir}',
        '--no-progress',
        '--yes'
    ]
    assert cmd == expected_command


def test_calibration_file_cmd(base_test_dir):
    from osa.scripts.calibration_pipeline import calibration_file_command
    cmd = calibration_file_command(pedcal_run_id="01805")
    expected_command = [
        'onsite_create_calibration_file',
        '--run_number=01805',
        f'--base_dir={base_test_dir}',
        '--yes',
        '--filters=52'
    ]
    assert cmd == expected_command


def test_drs4_pedestal(running_analysis_dir):
    from osa.scripts.calibration_pipeline import drs4_pedestal
    history_file = running_analysis_dir / "calibration_sequence.history"
    with pytest.raises(SystemExit):
        rc = drs4_pedestal(
            drs4_pedestal_run_id="01804",
            history_file=history_file
        )
        assert rc != 0


def test_calibrate_charge(running_analysis_dir):
    from osa.scripts.calibration_pipeline import calibrate_charge
    history_file = running_analysis_dir / "calibration_sequence.history"
    with pytest.raises(SystemExit):
        rc = calibrate_charge(
            calibration_run="01805",
            history_file=history_file
        )
        assert rc != 0
