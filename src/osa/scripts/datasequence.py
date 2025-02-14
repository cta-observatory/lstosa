"""Script called from the batch scheduler to process a run."""

import logging
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.job import historylevel
from osa.workflow.stages import AnalysisStage
from osa.provenance.capture import trace
from osa.paths import get_catB_calibration_filename
from osa.utils.cliopts import data_sequence_cli_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir
from osa.paths import catB_closed_file_exists, get_dl1_prod_id, get_dl2_nsb_prod_id


__all__ = ["data_sequence", "r0_to_dl1", "dl1_to_dl2", "dl1ab", "dl1_datacheck"]

log = myLogger(logging.getLogger())


def data_sequence(
    calibration_file: Path,
    pedestal_file: Path,
    time_calibration_file: Path,
    systematic_correction_file: Path,
    drive_file: Path,
    run_summary: Path,
    pedestal_ids_file: Path,
    run_str: str,
    rf_model_path: Path=None,
):
    """
    Performs all the steps to process a whole run.

    Parameters
    ----------
    calibration_file: pathlib.Path
    pedestal_file: pathlib.Path
    time_calibration_file: pathlib.Path
    systematic_correction_file: pathlib.Path
    drive_file: pathlib.Path
    run_summary: pathlib.Path
    pedestal_ids_file: pathlib.Path
    run_str: str

    Returns
    -------
    rc: int
        Return code of the last executed command.
    """
    history_file = Path(options.directory) / f"sequence_{options.tel_id}_{run_str}.history"
    # Set the starting level and corresponding return code from last analysis step
    # registered in the history file.
    level, rc = (4, 0) if options.simulate else historylevel(history_file, "DATA")
    log.info(f"Going to level {level}")

    if level == 4:
        rc = r0_to_dl1(
            calibration_file,
            pedestal_file,
            time_calibration_file,
            systematic_correction_file,
            drive_file,
            run_summary,
            pedestal_ids_file,
            run_str,
        )
        level -= 1
        log.info(f"Going to level {level}")

    if level == 3:
        if options.no_dl1ab:
            level = 0
            log.info(f"No DL1B are going to be produced. Going to level {level}")
        else:
            rc = dl1ab(run_str)
            if cfg.getboolean("lstchain", "store_image_dl1ab"):
                level -= 1
                log.info(f"Going to level {level}")
            else:
                level -= 2
                log.info(f"No images stored in dl1ab. Producing DL2. Going to level {level}")

    if level == 2:
        rc = dl1_datacheck(run_str)
        if options.no_dl2:
            level = 0
            log.info(f"No DL2 are going to be produced. Going to level {level}")
        else:
            level -= 1
            log.info(f"Going to level {level}")

    if level == 1:
        if options.no_dl2:
            level = 0
            log.info(f"No DL2 are going to be produced. Going to level {level}")
        else:
            rc = dl1_to_dl2(run_str, rf_model_path)
            level -= 1
            log.info(f"Going to level {level}")

    if level == 0:
        log.info(f"Job for sequence {run_str} finished without fatal errors")

    return rc


@trace
def r0_to_dl1(
    calibration_file: Path,
    pedestal_file: Path,
    time_calibration_file: Path,
    systematic_correction_file: Path,
    drive_file: Path,
    run_summary: Path,
    pedestal_ids_file: Path,
    run_str: str,
) -> int:
    """
    Prepare and launch the actual lstchain script that is performing
    the low and high-level calibration to raw camera images.
    It also applies the image cleaning and obtains shower parameters.

    Parameters
    ----------
    calibration_file: pathlib.Path
    pedestal_file: pathlib.Path
    time_calibration_file: pathlib.Path
    systematic_correction_file: pathlib.Path
    drive_file: pathlib.Path
    run_summary: : pathlib.Path
        Path to the run summary file
    pedestal_ids_file: pathlib.Path
        Path to file containing the interleaved pedestal event ids
    run_str: str
        XXXXX.XXXX (run_number.subrun_number)

    Returns
    -------
    rc: int
        Return code of the executed command.
    """
    command = cfg.get("lstchain", "r0_to_dl1")
    night_dir = date_to_dir(options.date)
    r0_dir = Path(cfg.get("LST1", "R0_DIR")) / night_dir
    r0_file = r0_dir / f"LST-1.1.Run{run_str}.fits.fz"
    dl1a_config = Path(cfg.get("lstchain", "dl1a_config"))

    cmd = [
        command,
        f"--input-file={r0_file}",
        f"--output-dir={options.directory}",
        f"--pedestal-file={pedestal_file}",
        f"--calibration-file={calibration_file}",
        f"--time-calibration-file={time_calibration_file}",
        f"--systematic-correction-file={systematic_correction_file}",
        f"--config={dl1a_config}",
        f"--pointing-file={drive_file}",
        f"--run-summary-path={run_summary}",
    ]

    if pedestal_ids_file is not None:
        cmd.append(f"--pedestal-ids-path={pedestal_ids_file}")

    if options.simulate:
        return 0

    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=dl1a_config.name)
    analysis_step.execute()
    return analysis_step.rc


@trace
def dl1ab(run_str: str) -> int:
    """
    Prepare and launch the actual lstchain script that is performing
    the image cleaning considering the interleaved pedestal information
    and obtains shower parameters. It keeps the shower images.

    Parameters
    ----------
    run_str: str

    Returns
    -------
    rc: int
        Return code of the executed command.
    """
    
    # Prepare and launch the actual lstchain script
    command = cfg.get("lstchain", "dl1ab")
    if not cfg.getboolean("lstchain", "apply_standard_dl1b_config"):
        config_file = Path(options.directory) / f"dl1ab_Run{run_str[:5]}.json"
        if not config_file.exists():
            log.info(
                f"The dl1b config file was not created yet for run {run_str[:5]}. "
                "Please try again later."
            )
            sys.exit(1)
        else: 
            options.dl1_prod_id = get_dl1_prod_id(config_file)
    else:
        config_file = Path(cfg.get("lstchain", "dl1b_config"))
    
    # Create a new subdirectory for the dl1ab output
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    dl1ab_subdirectory.mkdir(parents=True, exist_ok=True)
    # DL1a input file from base running_analysis directory
    input_dl1_datafile = Path(options.directory) / f"dl1_LST-1.Run{run_str}.h5"
    # DL1b output file to be stored in the dl1ab subdirectory
    output_dl1_datafile = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"

    cmd = [
        command,
        f"--input-file={input_dl1_datafile}",
        f"--output-file={output_dl1_datafile}",
        f"--config={config_file}",
    ]
    
    if not cfg.getboolean("lstchain", "store_image_dl1ab"):
        cmd.append("--no-image=True")

    if cfg.getboolean("lstchain", "apply_catB_calibration"):
        if catB_closed_file_exists(int(run_str[:5])):
            catB_calibration_file = get_catB_calibration_filename(int(run_str[:5]))
            cmd.append(f"--catB-calibration-file={catB_calibration_file}")
        else:
            log.info(
                f"Cat-B calibration did not finish yet for run {run_str[:5]}. "
                "Please try again later."
            )
            sys.exit(1)

    if options.simulate:
        return 0

    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=config_file.name)
    analysis_step.execute()
    return analysis_step.rc


@trace
def dl1_datacheck(run_str: str) -> int:
    """
    Run datacheck script

    Parameters
    ----------
    run_str: str

    Returns
    -------
    rc: int
    """
    # Create a new subdirectory for the dl1ab output
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    input_dl1_datafile = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"
    output_directory = Path(options.directory) / options.dl1_prod_id
    output_directory.mkdir(parents=True, exist_ok=True)

    # Prepare and launch the actual lstchain script
    command = cfg.get("lstchain", "check_dl1")
    cmd = [
        command,
        f"--input-file={input_dl1_datafile}",
        f"--output-dir={output_directory}",
        f"--muons-dir={options.directory}",
        "--omit-pdf",
        "--batch",
    ]

    if options.simulate:
        return 0

    analysis_step = AnalysisStage(run=run_str, command_args=cmd)
    analysis_step.execute()
    return analysis_step.rc


@trace
def dl1_to_dl2(run_str: str, rf_model_path: Path) -> int:
    """
    It prepares and execute the dl1 to dl2 lstchain scripts that applies
    the already trained RFs models to DL1 files. It identifies the
    primary particle, reconstructs its energy and direction.

    Parameters
    ----------
    run_str: str

    Returns
    -------
    rc: int
    """
    nsb_prod_id = get_dl2_nsb_prod_id(rf_model_path)
    options.dl2_prod_id = options.dl1_prod_id / nsb_prod_id
    dl2_subdirectory = Path(options.directory) / options.dl2_prod_id
    dl2_config = Path(cfg.get("lstchain", "dl2_config"))
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    dl1_file = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"

    command = cfg.get("lstchain", "dl1_to_dl2")
    cmd = [
        command,
        f"--input-file={dl1_file}",
        f"--output-dir={dl2_subdirectory}",
        f"--path-models={rf_model_path}",
        f"--config={dl2_config}",
    ]

    if options.simulate:
        return 0

    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=dl2_config.name)
    analysis_step.execute()
    return analysis_step.rc


def main():
    """Performs the analysis steps to convert raw data into DL2 files."""
    (
        calibration_file,
        drs4_ped_file,
        time_calibration_file,
        systematic_correction_file,
        drive_log_file,
        run_summary_file,
        pedestal_ids_file,
        run_number,
        rf_model_path,
    ) = data_sequence_cli_parsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # Run the routine piping all the analysis steps
    rc = data_sequence(
        calibration_file,
        drs4_ped_file,
        time_calibration_file,
        systematic_correction_file,
        drive_log_file,
        run_summary_file,
        pedestal_ids_file,
        run_number,
        rf_model_path,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
