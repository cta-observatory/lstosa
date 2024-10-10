"""Script called from the batch scheduler to process a run."""

import logging
import sys
from pathlib import Path
import time

from osa.configs import options
from osa.configs.config import cfg
from osa.workflow.stages import AnalysisStage
from osa.provenance.capture import trace
from osa.paths import (
    get_catB_calibration_filename,
    get_drive_file,
    get_summary_file,
    pedestal_ids_file_exists,
    get_last_pedcalib,
)
from osa.utils.cliopts import data_sequence_cli_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir, get_calib_filters, stringify


__all__ = ["data_sequence", "r0_to_dl1", "dl1_to_dl2", "dl1ab", "dl1_datacheck"]

log = myLogger(logging.getLogger(__name__))


def historylevel(history_file: Path, data_type: str):
    """
    Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file.

    Notes
    -----
    Workflow for PEDCALIB sequences:
     - Creation of DRS4 pedestal file, level 2->1
     - Creation of charge calibration file, level 1->0
     - Sequence completed when reaching level 0

    Workflow for DATA sequences:
     - R0->DL1, level 4->3
     - DL1->DL1AB, level 3->2
     - DATACHECK, level 2->1
     - DL1->DL2, level 1->0
     - Sequence completed when reaching level 0

    Parameters
    ----------
    history_file: pathlib.Path
    data_type: str
        Type of the sequence, either 'DATA' or 'PEDCALIB'

    Returns
    -------
    level : int
    exit_status : int
    """

    # TODO: Create a dict with the program exit status and prod id to take
    #  into account not only the last history line but also the others.

    if data_type == "DATA":
        level = 5
    elif data_type == "PEDCALIB":
        level = 2
    else:
        raise ValueError(f"Type {data_type} not expected")

    exit_status = 0

    if history_file.exists():
        for line in history_file.read_text().splitlines():
            words = line.split()
            try:
                program = words[1]
                prod_id = words[2]
                exit_status = int(words[-1])
                log.debug(f"{program}, finished with error {exit_status} and prod ID {prod_id}")
            except (IndexError, ValueError) as err:
                log.exception(f"Malformed history file {history_file}, {err}")
            else:
                # Calibration sequence
                if program == cfg.get("lstchain", "drs4_baseline"):
                    level = 1 if exit_status == 0 else 2
                elif program == cfg.get("lstchain", "charge_calibration"):
                    level = 0 if exit_status == 0 else 1
                # Data sequence
                elif program == cfg.get("lstchain", "r0_to_dl1"):
                    level = 4 if exit_status == 0 else 5
                elif program == cfg.get("lstchain", "catB_calibration"):
                    level = 3 if exit_status == 0 else 4
                elif program == cfg.get("lstchain", "dl1ab"):
                    if (exit_status == 0) and (prod_id == options.dl1_prod_id):
                        log.debug(f"DL1ab prod ID: {options.dl1_prod_id} already produced")
                        level = 2
                    else:
                        level = 3
                        log.debug(f"DL1ab prod ID: {options.dl1_prod_id} not produced yet")
                        break
                elif program == cfg.get("lstchain", "check_dl1"):
                    level = 1 if exit_status == 0 else 2
                elif program == cfg.get("lstchain", "dl1_to_dl2"):
                    if (exit_status == 0) and (prod_id == options.dl2_prod_id):
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} already produced")
                        level = 0
                    else:
                        level = 1
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} not produced yet")

                else:
                    log.warning(f"Program name not identified: {program}")

    return level, exit_status


def sbatch_command(sequence, parent_jobid, scheduler="slurm"):
    if scheduler != "slurm":
        log.warning("No other schedulers are currently supported")
        return None

    sbatch_cmd = [
        "sbatch",
        "--parsable",
        f"--job-name={sequence.jobname}",
        f"--time={cfg.get('SLURM', 'WALLTIME')}",
        f"--chdir={options.directory}",
        f"--output=log/Run{sequence.run:05d}.{sequence.subrun:04d}_{parent_jobid}.out",
        f"--error=log/Run{sequence.run:05d}.{sequence.subrun:04d}_{parent_jobid}.err",
    ]

    # Get the number of subruns counting from 0.
    subruns = sequence.subruns - 1

    # Depending on the type of sequence, we need to set
    # different sbatch environment variables
    if sequence.type == "DATA":
        sbatch_cmd.append(f"--array=0-{subruns}")

    sbatch_cmd.append(f"--partition={cfg.get('SLURM', f'PARTITION_{sequence.type}')}")
    sbatch_cmd.append(f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE_{sequence.type}')}")
    sbatch_cmd.append(f"--account={cfg.get('SLURM', 'ACCOUNT')}")

    if parent_jobid:
        sbatch_cmd.append(f"--dependency=afterok:{parent_jobid}")

    return sbatch_cmd

def data_sequence(sequence, parent_jobid=None):
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
    #run_str = f"{sequence.run:05d}.{{subruns:04d}}"
    run_str = sequence.run_str
    history_file = Path(options.directory) / f"sequence_{options.tel_id}_{run_str}.history"
    # Set the starting level and corresponding return code from last analysis step
    # registered in the history file.
    level, rc = (5, 0) if options.simulate else historylevel(history_file, "DATA")
    log.debug(f"Going to level {level}")

    if level == 5:
        drive_file = get_drive_file(date_to_dir(options.date))
        run_summary = get_summary_file(date_to_dir(options.date))
        sbatch_cmd = sbatch_command(sequence, parent_jobid)

        if pedestal_ids_file_exists(int(run_str[:5])):
            pedestal_ids_file = get_pedestal_ids_file(int(run_str[:5]), date_to_dir(options.date))
        else:
            pedestal_ids_file = None
        
        rc, jobid_dl1 = r0_to_dl1(
            sequence.calibration_file,
            sequence.drs4_file,
            sequence.time_calibration_file,
            sequence.systematic_correction_file,
            drive_file,
            run_summary,
            pedestal_ids_file,
            run_str,
            sbatch_cmd,
        )
        level -= 1
        log.debug(f"Going to level {level}")

    if level == 4:
        sbatch_cmd = sbatch_command(sequence, jobid_dl1)
        rc, jobid_catB = catB_calibration(run_str, sbatch_cmd)
        level -= 1
        log.debug(f"Going to level {level}")

    if level == 3:
        sbatch_cmd = sbatch_command(sequence, jobid_catB)
        rc, jobid_dl1ab = dl1ab(run_str, sbatch_cmd)
        if cfg.getboolean("lstchain", "store_image_dl1ab"):
            level -= 1
            log.debug(f"Going to level {level}")
        else:
            level -= 2
            log.info(f"No images stored in dl1ab. Producing DL2. Going to level {level}")

    if level == 2:
        sbatch_cmd = sbatch_command(sequence, jobid_dl1ab)
        rc, jobid_datacheck = dl1_datacheck(run_str, sbatch_cmd)
        if options.no_dl2:
            level = 0
            log.info(f"No DL2 are going to be produced. Going to level {level}")
        else:
            level -= 1
            log.debug(f"Going to level {level}")

    if level == 1:    
        if options.no_dl2:
            level = 0
            log.info(f"No DL2 are going to be produced.")
        else:
            sbatch_cmd = sbatch_command(sequence, jobid_datacheck)
            rc = dl1_to_dl2(run_str, sbatch_cmd)
            level -= 1

    if level == 0:
        log.debug(f"Job for sequence {run_str} finished without fatal errors")

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
    sbatch_cmd,
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
    #sbatch_cmd = sbatch_command(sequence, jobid)
    command = cfg.get("lstchain", "r0_to_dl1")
    night_dir = date_to_dir(options.date)
    r0_dir = Path(cfg.get("LST1", "R0_DIR")) / night_dir
    r0_file = r0_dir / f"LST-1.1.Run{run_str}.fits.fz"
    dl1a_config = Path(cfg.get("lstchain", "dl1a_config"))

    lstchain_cmd = [
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
        lstchain_cmd.append(f"--pedestal-ids-path={pedestal_ids_file}")

    if options.simulate:
        return 0, "1"

    cmd = sbatch_cmd + lstchain_cmd
    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=dl1a_config.name)
    analysis_step.execute()
    return analysis_step.rc, analysis_step.jobid


@trace
def catB_calibration(run_str: str, sbatch_cmd) -> int:
    """
    Prepare and launch the lstchain script that creates the 
    Category B calibration files. It should be executed runwise,
    so it is only launched for the first subrun of each run.

    Parameters
    ----------
    run_str: str

    Returns
    -------
    rc: int
        Return code of the executed command.
    """
    if run_str[-4:] != "0000":
        log.debug(f"{run_str} is not the first subrun of the run, so the script "
            "onsite_create_cat_B_calibration_file will not be launched for this subrun.")
        return 0, None

    #sbatch_cmd = sbatch_command(sequence, jobid)
    command = cfg.get("lstchain", "catB_calibration")
    options.filters = get_calib_filters(int(run_str[:5])) 
    base_dir = Path(cfg.get("LST1", "BASE")).resolve()
    r0_dir = Path(cfg.get("LST1", "R0_DIR")).resolve()
    catA_calib_run = get_last_pedcalib(options.date)
    catA_calib_run = "01806"
    lstchain_cmd = [
        command,
        f"--run_number={run_str[:5]}",
        f"--catA_calibration_run={catA_calib_run}",
        f"--base_dir={base_dir}",
        f"--r0-dir={r0_dir}",
        f"--filters={options.filters}",
    ]
    if options.simulate:
        return 0, "1"

    cmd = sbatch_cmd + lstchain_cmd
    analysis_step = AnalysisStage(run=run_str, command_args=cmd)
    analysis_step.execute()
    return analysis_step.rc, analysis_step.jobid


@trace
def dl1ab(run_str: str, sbatch_cmd) -> int:
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
    # Create a new subdirectory for the dl1ab output
    #sbatch_cmd = sbatch_command(sequence, jobid)
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    dl1ab_subdirectory.mkdir(parents=True, exist_ok=True)
    dl1b_config = Path(cfg.get("lstchain", "dl1b_config"))
    # DL1a input file from base running_analysis directory
    input_dl1_datafile = Path(options.directory) / f"dl1_LST-1.Run{run_str}.h5"
    # DL1b output file to be stored in the dl1ab subdirectory
    output_dl1_datafile = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"
    
    # Prepare and launch the actual lstchain script
    command = cfg.get("lstchain", "dl1ab")
    lstchain_cmd = [
        command,
        f"--input-file={input_dl1_datafile}",
        f"--output-file={output_dl1_datafile}",
        f"--config={dl1b_config}",
    ]
    
    if not cfg.getboolean("lstchain", "store_image_dl1ab"):
        lstchain_cmd.append("--no-image=True")

    if cfg.getboolean("lstchain", "apply_catB_calibration"):
        catB_calibration_file = get_catB_calibration_filename(int(run_str[:5]))
        lstchain_cmd.append(f"--catB-calibration-file={catB_calibration_file}")

    if options.simulate:
        return 0, "1"

    cmd = sbatch_cmd + lstchain_cmd
    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=dl1b_config.name)
    analysis_step.execute()
    return analysis_step.rc, analysis_step.jobid


@trace
def dl1_datacheck(run_str: str, sbatch_cmd) -> int:
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
    #sbatch_cmd = sbatch_command(sequence, jobid)
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    input_dl1_datafile = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"
    output_directory = Path(options.directory) / options.dl1_prod_id
    output_directory.mkdir(parents=True, exist_ok=True)

    # Prepare and launch the actual lstchain script
    command = cfg.get("lstchain", "check_dl1")
    lstchain_cmd = [
        command,
        f"--input-file={input_dl1_datafile}",
        f"--output-dir={output_directory}",
        f"--muons-dir={options.directory}",
        "--omit-pdf",
        "--batch",
    ]

    if options.simulate:
        return 0, "1"

    cmd = sbatch_cmd + lstchain_cmd
    analysis_step = AnalysisStage(run=run_str, command_args=cmd)
    analysis_step.execute()
    return analysis_step.rc, analysis_step.jobid


@trace
def dl1_to_dl2(run_str: str, sbatch_cmd) -> int:
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
    #sbatch_cmd = sbatch_command(sequence, jobid)
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    dl2_subdirectory = Path(options.directory) / options.dl2_prod_id
    dl2_config = Path(cfg.get("lstchain", "dl2_config"))
    rf_models_directory = Path(cfg.get("lstchain", "RF_MODELS"))
    dl1_file = dl1ab_subdirectory / f"dl1_LST-1.Run{run_str}.h5"

    command = cfg.get("lstchain", "dl1_to_dl2")
    lstchain_cmd = [
        command,
        f"--input-file={dl1_file}",
        f"--output-dir={dl2_subdirectory}",
        f"--path-models={rf_models_directory}",
        f"--config={dl2_config}",
    ]

    if options.simulate:
        return 0

    cmd = sbatch_cmd + lstchain_cmd
    analysis_step = AnalysisStage(run=run_str, command_args=cmd, config_file=dl2_config.name)
    analysis_step.execute()
    return analysis_step.rc


def main():
    """Performs the analysis steps to convert raw data into DL2 files."""
    (sequence, parent_jobid) = data_sequence_cli_parsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # Run the routine piping all the analysis steps
    rc = data_sequence(sequence, parent_jobid)
    sys.exit(rc)


if __name__ == "__main__":
    main()
    
