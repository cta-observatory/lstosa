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
    "dl3_stage",
    "theta2_significance",
    "source_coordinates",
    "sequencer_webmaker",
]

options.date = datetime.datetime.fromisoformat("2020-01-17")
options.tel_id = "LST1"
options.prod_id = "v0.1.0"
options.dl1_prod_id = "tailcut84"
options.directory = "test_osa/test_files0/running_analysis/20200117/v0.1.0/"


def remove_provlog():
    log_file = Path("prov.log")
    if log_file.is_file():
        log_file.unlink()


def run_program(*args):
    result = sp.run(args, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8", check=True)

    if result.returncode != 0:
        new_line = "\n"
        raise ValueError(
            f"Running {args[0]} failed with return code {result.returncode}, output: "
            f"{new_line.join(result.stdout)}"
        )

    return result


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_all_help(script):
    """Test for all scripts if at least the help works."""
    run_program(script, "--help")


def test_simulate_processing(
    drs4_time_calibration_files,
    systematic_correction_files,
    run_summary_file,
    r0_data,
    merged_run_summary,
    drive_log
):

    for file in drs4_time_calibration_files:
        assert file.exists()

    for file in systematic_correction_files:
        assert file.exists()

    for r0_file in r0_data:
        assert r0_file.exists()

    assert run_summary_file.exists()
    assert merged_run_summary.exists()
    assert drive_log.exists()

    remove_provlog()
    rc = run_program("simulate_processing", "-p", "--force")
    assert rc.returncode == 0

    prov_dl1_path = Path("./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/log")
    prov_dl2_path = Path("./test_osa/test_files0/DL2/20200117/v0.1.0/model2/log")
    prov_file_dl1 = prov_dl1_path / "calibration_to_dl1_01807_prov.log"
    prov_file_dl2 = prov_dl2_path / "calibration_to_dl2_01807_prov.log"
    json_file_dl1 = prov_dl1_path / "calibration_to_dl1_01807_prov.json"
    json_file_dl2 = prov_dl2_path / "calibration_to_dl2_01807_prov.json"
    pdf_file_dl1 = prov_dl1_path / "calibration_to_dl1_01807_prov.pdf"
    pdf_file_dl2 = prov_dl2_path / "calibration_to_dl2_01807_prov.pdf"

    assert prov_file_dl1.exists()
    assert prov_file_dl2.exists()
    assert pdf_file_dl1.exists()
    assert pdf_file_dl2.exists()

    with open(json_file_dl1) as file:
        dl1 = yaml.safe_load(file)
    assert len(dl1["entity"]) == 19
    assert len(dl1["activity"]) == 5
    assert len(dl1["used"]) == 15
    assert len(dl1["wasGeneratedBy"]) == 10

    with open(json_file_dl2) as file:
        dl2 = yaml.safe_load(file)
    assert len(dl2["entity"]) == 25
    assert len(dl2["activity"]) == 6
    assert len(dl2["used"]) == 21
    assert len(dl2["wasGeneratedBy"]) == 12

    rc = run_program("simulate_processing", "-p")
    assert rc.returncode == 0

    remove_provlog()
    rc = run_program("simulate_processing", "-p")
    assert rc.returncode == 0


def test_simulated_sequencer(
    drs4_time_calibration_files,
    systematic_correction_files,
    run_summary_file,
    run_catalog,
    r0_data,
    merged_run_summary,
):
    assert run_summary_file.exists()
    assert run_catalog.exists()

    for r0_file in r0_data:
        assert r0_file.exists()

    for file in drs4_time_calibration_files:
        assert file.exists()

    for file in systematic_correction_files:
        assert file.exists()

    rc = run_program("sequencer", "-d", "2020-01-17", "-s", "-t", "LST1")

    assert rc.returncode == 0
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    assert rc.stdout == dedent(
        f"""\
        =================================== Starting sequencer.py at {now} UTC for LST, Telescope: LST1, Date: 2020-01-17 ===================================
        Tel   Seq  Parent  Type      Run   Subruns  Source        Action  Tries  JobID  State  CPU_time  Exit  DL1%  MUONS%  DL1AB%  DATACHECK%  DL2%  
        LST1    1  None    PEDCALIB  1809  5        None          None    None   None   None   None      None  None  None    None    None        None  
        LST1    2       1  DATA      1807  11       Crab          None    None   None   None   None      None     0       0       0           0     0  
        LST1    3       1  DATA      1808  9        MadeUpSource  None    None   None   None   None      None     0       0       0           0     0  
        """  # noqa: E501
    )


def test_sequencer(sequence_file_list):
    for sequence_file in sequence_file_list:
        assert sequence_file.exists()


def test_autocloser(running_analysis_dir):
    result = run_program(
        "autocloser",
        "--date",
        "2020-01-17",
        "--test",
        "LST1",
    )
    assert os.path.exists(running_analysis_dir)
    assert result.stdout.split()[-1] == "Exit"


def test_closer(
    r0_data,
    running_analysis_dir,
    test_observed_data,
    run_summary_file,
    drs4_time_calibration_files,
    systematic_correction_files,
    merged_run_summary,
    longterm_dir,
    longterm_link_latest_dir,
    daily_datacheck_dl1_files,
):
    # First assure that the end of night flag is not set and remove it otherwise
    night_finished_flag = Path(
        "./test_osa/test_files0/OSA/Closer/20200117/v0.1.0/NightFinished.txt"
    )
    if night_finished_flag.exists():
        night_finished_flag.unlink()

    for r0_file in r0_data:
        assert r0_file.exists()
    for file in drs4_time_calibration_files:
        assert file.exists()
    for file in systematic_correction_files:
        assert file.exists()
    assert running_analysis_dir.exists()
    assert run_summary_file.exists()
    for obs_file in test_observed_data:
        assert obs_file.exists()
    assert merged_run_summary.exists()
    assert longterm_dir.exists()
    assert longterm_link_latest_dir.exists()
    for check_file in daily_datacheck_dl1_files:
        assert check_file.exists()

    run_program("closer", "-y", "-v", "-t", "-d", "2020-01-17", "LST1")
    closed_seq_file = running_analysis_dir / "sequence_LST1_01809.closed"

    # Check that files have been moved to their final destinations
    assert os.path.exists(
       "./test_osa/test_files0/DL1/20200117/v0.1.0/muons/muons_LST-1.Run01808.0011.fits"
    )
    assert os.path.exists(
       "./test_osa/test_files0/DL1/20200117/v0.1.0/interleaved/interleaved_LST-1.Run01808.0011.h5"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/dl1_LST-1.Run01808.0011.h5"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/datacheck/"
        "datacheck_dl1_LST-1.Run01808.0011.h5"
    )
    assert os.path.exists(
        "./test_osa/test_files0/DL2/20200117/v0.1.0/model2/dl2_LST-1.Run01808.0011.h5"
    )
    # Assert that the link to dl1 and muons files have been created
    assert os.path.islink(
        "./test_osa/test_files0/running_analysis/20200117/v0.1.0/muons_LST-1.Run01808.0011.fits"
    )
    assert os.path.islink(
        "./test_osa/test_files0/running_analysis/20200117/v0.1.0/dl1_LST-1.Run01808.0011.h5"
    )

    assert night_finished_flag.exists()
    assert closed_seq_file.exists()


def test_datasequence(running_analysis_dir):
    drs4_file = "drs4_pedestal.Run00001.0000.fits"
    calib_file = "calibration.Run00002.0000.hdf5"
    timecalib_file = "time_calibration.Run00002.0000.hdf5"
    systematic_correction_file = "no_sys_corrected_calibration_scan_fit_20210514.0000.h5"
    drive_file = "DrivePosition_20200117.txt"
    runsummary_file = "RunSummary_20200117.ecsv"
    prod_id = "v0.1.0"
    run_number = "00003.0000"
    options.directory = running_analysis_dir

    output = run_program(
        "datasequence",
        "--date=2020-01-17",
        "--simulate",
        f"--prod-id={prod_id}",
        f"--drs4-pedestal-file={drs4_file}",
        f"--pedcal-file={calib_file}",
        f"--time-calib-file={timecalib_file}",
        f"--systematic-correction-file={systematic_correction_file}",
        f"--drive-file={drive_file}",
        f"--run-summary={runsummary_file}",
        run_number,
        "LST1",
    )
    assert output.returncode == 0


def test_calibration_pipeline(running_analysis_dir):
    options.prod_id = "v0.1.0"
    drs4_run_number = "01804"
    pedcal_run_number = "01805"
    options.directory = running_analysis_dir

    output = run_program(
        "calibration_pipeline",
        "--date=2020-01-17",
        "--simulate",
        f"--prod-id={options.prod_id}",
        f"--drs4-pedestal-run={drs4_run_number}",
        f"--pedcal-run={pedcal_run_number}",
        "LST1",
    )
    assert output.returncode == 0


def test_is_sequencer_successful(run_summary, running_analysis_dir):
    options.directory = running_analysis_dir
    options.test = True
    seq_tuple = is_finished_check(run_summary)
    options.test = False
    assert is_sequencer_successful(seq_tuple) is True


def test_drs4_pedestal_cmd(base_test_dir):
    from osa.scripts.calibration_pipeline import drs4_pedestal_command

    cmd = drs4_pedestal_command(drs4_pedestal_run_id="01804")
    expected_command = [
        "onsite_create_drs4_pedestal_file",
        "--run_number=01804",
        f"--base_dir={base_test_dir}",
        "--no-progress",
    ]
    assert cmd == expected_command


def test_calibration_file_cmd(base_test_dir):
    from osa.scripts.calibration_pipeline import calibration_file_command

    cmd = calibration_file_command(drs4_pedestal_run_id="01804", pedcal_run_id="01809")
    expected_command = [
        "onsite_create_calibration_file",
        "--pedestal_run=01804",
        "--run_number=01809",
        f"--base_dir={base_test_dir}",
    ]
    assert cmd == expected_command


def test_daily_longterm_cmd():
    from osa.scripts.closer import daily_longterm_cmd

    job_ids = ["12345", "54321"]
    cmd = daily_longterm_cmd(parent_job_ids=job_ids)

    expected_cmd = [
        "sbatch",
        "--parsable",
        "-D",
        options.directory,
        "-o",
        "log/longterm_daily_%j.log",
        "--dependency=afterok:12345,54321",
        "lstchain_longterm_dl1_check",
        "--input-dir=test_osa/test_files0/DL1/20200117/v0.1.0/tailcut84/datacheck",
        "--output-file=test_osa/test_files0/OSA/DL1DataCheck_LongTerm/v0.1.0/20200117/DL1_datacheck_20200117.h5",
        "--muons-dir=test_osa/test_files0/DL1/20200117/v0.1.0/muons",
        "--batch",
    ]

    assert cmd == expected_cmd


def test_observation_finished():
    """Check if observation is finished for `options.date=2020-01-17`."""
    from osa.scripts.closer import observation_finished

    date1 = datetime.datetime(2020, 1, 21, 12, 0, 0)
    assert observation_finished(date=date1) is True
    date2 = datetime.datetime(2020, 1, 17, 5, 0, 0)
    assert observation_finished(date=date2) is False


def test_no_runs_found():
    output = sp.run(
        ["sequencer", "-s", "-d", "2015-01-01", "LST1"], text=True, stdout=sp.PIPE, stderr=sp.PIPE
    )
    assert output.returncode == 0
    assert "No runs found for this date. Nothing to do. Exiting." in output.stderr.splitlines()[-1]


@pytest.mark.skip(reason="Currently not working with all combinations")
def test_sequencer_webmaker(
    run_summary,
    merged_run_summary,
    drs4_time_calibration_files,
    systematic_correction_files,
    base_test_dir,
):
    # Check if night finished flag is set
    night_finished = base_test_dir / "OSA/Closer/20200117/v0.1.0/NightFinished.txt"

    if night_finished.exists():
        output = sp.run(
            ["sequencer_webmaker", "--test", "-d", "2020-01-17"],
            text=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
        )
        assert output.returncode != 0
        assert output.stderr.splitlines()[-1] == "Date 2020-01-17 is already closed for LST1"
        night_finished.unlink()

    output = sp.run(["sequencer_webmaker", "--test", "-d", "2020-01-17"])
    assert output.returncode == 0
    directory = base_test_dir / "OSA" / "SequencerWeb"
    directory.mkdir(parents=True, exist_ok=True)
    expected_file = directory / "osa_status_20200117.html"
    assert expected_file.exists()

    output = sp.run(["sequencer_webmaker", "--test"])
    assert output.returncode != 0

    # Running without test option will make the script fail
    output = sp.run(["sequencer_webmaker", "-d", "2020-01-17"])
    assert output.returncode != 0
