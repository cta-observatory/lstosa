import pytest
import tenacity

from osa.configs import options


def test_analysis_stage(running_analysis_dir):
    from osa.workflow.stages import AnalysisStage

    options.simulate = False
    options.directory = running_analysis_dir

    # First step of the analysis
    r0_file = "input_r0.fits"
    pedestal_file = "drs4_file.h5"
    calibration_file = "calib_file.h5"
    drive_file = "drive_log.txt"

    cmd = [
        "lstchain_data_r0_to_dl1",
        f"--input-file={r0_file}",
        f"--pedestal-file={pedestal_file}",
        f"--calibration-file={calibration_file}",
        f"--drive-file={drive_file}",
    ]
    stage = AnalysisStage(run="01000.0001", command_args=cmd)
    assert stage.rc is None
    assert stage.show_command() == " ".join(cmd)
    with pytest.raises(tenacity.RetryError):
        stage.execute()
    assert stage.rc == 2
    # Check that the stage is marked as failed in the history file
    with open(stage.history_file, "r") as f:
        lines = f.readlines()
        assert len(lines) >= 3
    # Check that the last element in the last line is the rc 2
    assert lines[-1].split(" ")[0] == stage.run
    assert lines[-1].split(" ")[1] == cmd[0]
    assert lines[-1].split(" ")[-1] == "2\n"

    # Second step
    cmd = [
        "lstchain_dl1ab",
        "--input-file=dl1a_file.h5",
        "--output-file=dl1b_file.h5",
    ]
    stage = AnalysisStage(run="01000.0001", command_args=cmd)
    assert stage.rc is None
    assert stage.show_command() == " ".join(cmd)
    with pytest.raises(tenacity.RetryError):
        stage.execute()
    assert stage.rc == 1
    # Check that the stage is marked as failed in the history file
    with open(stage.history_file, "r") as f:
        lines = f.readlines()
        assert len(lines) >= 6
    # Check that the last element in the last line is the step rc
    assert lines[-1].split(" ")[0] == stage.run
    assert lines[-1].split(" ")[1] == cmd[0]
    assert lines[-1].split(" ")[-1] == "1\n"

    # Third step
    cmd = ["lstchain_check_dl1", "--input-file=dl1_file.h5", "--batch"]
    stage = AnalysisStage(run="01000.0001", command_args=cmd)
    assert stage.rc is None
    assert stage.show_command() == " ".join(cmd)
    with pytest.raises(tenacity.RetryError):
        stage.execute()
    assert stage.rc == 255
    # Check that the stage is marked as failed in the history file
    with open(stage.history_file, "r") as f:
        lines = f.readlines()
        assert len(lines) >= 9
    # Check that the last element in the last line is the step rc
    assert lines[-1].split(" ")[0] == stage.run
    assert lines[-1].split(" ")[1] == cmd[0]
    assert lines[-1].split(" ")[-1] == "255\n"


def test_calibration_steps(running_analysis_dir):
    from osa.workflow.stages import DRS4PedestalStage, ChargeCalibrationStage
    from osa.scripts.calibration_pipeline import drs4_pedestal_command, calibration_file_command

    options.simulate = False
    options.directory = running_analysis_dir

    cmd1 = drs4_pedestal_command(998)
    cmd2 = calibration_file_command(drs4_pedestal_run_id=998, pedcal_run_id=999)
    step1 = DRS4PedestalStage(run="00998", run_pedcal="00999", command_args=cmd1)
    step2 = ChargeCalibrationStage(run="00999", command_args=cmd2)

    with pytest.raises(tenacity.RetryError):
        step1.execute()

    with pytest.raises(tenacity.RetryError):
        step2.execute()

    assert step1.history_file == step2.history_file
    # Check that the error is in the history file
    with open(step1.history_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 6
    # Check that the last element in the last line is the rc
    assert lines[0].split(" ")[0] == step1.run
    assert lines[-1].split(" ")[0] == step2.run
    assert lines[0].split(" ")[1] == cmd1[0]
    assert lines[-1].split(" ")[1] == cmd2[0]
    assert lines[0].split(" ")[-1] == "1\n"
    assert lines[-1].split(" ")[-1] == "1\n"
