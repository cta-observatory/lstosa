"""Functions to handle the interaction with the job scheduler."""

import datetime
import logging
import shutil
import subprocess as sp
import time
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import (
    pedestal_ids_file_exists,
    get_drive_file,
    get_summary_file,
    get_pedestal_ids_file,
)
from osa.utils.iofile import write_to_file
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir, time_to_seconds, stringify, date_to_iso

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "are_all_jobs_correctly_finished",
    "historylevel",
    "prepare_jobs",
    "sequence_filenames",
    "set_queue_values",
    "job_header_template",
    "plot_job_statistics",
    "scheduler_env_variables",
    "set_cache_dirs",
    "submit_jobs",
    "check_history_level",
    "get_sacct_output",
    "get_squeue_output",
    "filter_jobs",
    "run_sacct",
    "run_squeue",
    "calibration_sequence_job_template",
    "data_sequence_job_template",
    "save_job_information",
]

TAB = "\t".expandtabs(4)
FORMAT_SLURM = [
    "JobID",
    "JobName",
    "CPUTime",
    "CPUTimeRAW",
    "Elapsed",
    "TotalCPU",
    "MaxRSS",
    "State",
    "ExitCode",
]

PYTHON_IMPORTS = dedent(
    """\

    import os
    import subprocess
    import sys
    import tempfile

    """
)


def are_all_jobs_correctly_finished(sequence_list):
    """
    Check if all jobs are correctly finished by looking
    at the history file.

    Parameters
    ----------
    sequence_list: list
        List of sequence objects

    Returns
    -------
    flag: bool
    """
    # FIXME: check based on sequence.jobid exit status
    flag = True
    analysis_directory = Path(options.directory)
    for sequence in sequence_list:
        history_files_list = analysis_directory.rglob(f"*{sequence.seq}*.history")
        for history_file in history_files_list:
            # TODO: s.history should be SubRunObj attribute not RunObj
            # s.history only working for CALIBRATION sequence (run-wise), since it is
            # looking for .../sequence_LST1_04180.history files
            # we need to check all the subrun wise history files
            # .../sequence_LST1_04180.XXXX.history
            out, _ = historylevel(history_file, sequence.type)
            if out == 0:
                log.debug(f"Job {sequence.seq} ({sequence.type}) correctly finished")
                continue
            if out == 1 and options.no_dl2:
                log.debug(
                    f"Job {sequence.seq} ({sequence.type}) correctly "
                    f"finished up to DL1ab, but --no-dl2 option selected"
                )
                continue

            log.warning(
                f"Job {sequence.seq} (run {sequence.run}) not correctly finished [level {out}]"
            )
            flag = False
    return flag


def check_history_level(history_file: Path, program_levels: dict):
    """
    Check the history of the calibration sequence.

    Parameters
    ----------
    history_file: pathlib.Path
        Path to the history file
    program_levels: dict
        Dictionary with the program name and the level of the program

    Returns
    -------
    level: int
        Level of the history file
    exit_status: int
        Exit status pf the program according to the history file
    """

    # Check the program exit_status (last string of the line), if it is 0, go
    # to the next level and check the next program. If exit_status is not 0, return the
    # actual level and the exit status. Stop the iteration when reaching the level 0.
    with open(history_file, "r") as file:
        for line in file:
            program = line.split()[1]
            exit_status = int(line.split()[-1])
            if program in program_levels:
                if exit_status != 0:
                    level = program_levels[program]
                    return level, exit_status
                level = program_levels[program]
                continue

        return level, exit_status


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
        level = 4
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


def prepare_jobs(sequence_list):
    """Prepare job file template for each sequence."""
    if not options.simulate:
        log.info("Building job scripts for each sequence.")

    for sequence in sequence_list:
        log.debug(f"Creating sequence.py for sequence {sequence.seq}")
        if sequence.type == "PEDCALIB":
            calibration_sequence_job_template(sequence)
        elif sequence.type == "DATA":
            data_sequence_job_template(sequence)
        else:
            raise ValueError(f"Type {sequence.type} not expected")


def sequence_filenames(sequence):
    """Build names of the script, veto and history files."""
    basename = f"sequence_{sequence.jobname}"
    sequence.script = Path(options.directory) / f"{basename}.py"
    sequence.veto = Path(options.directory) / f"{basename}.veto"
    sequence.history = Path(options.directory) / f"{basename}.history"


def save_job_information():
    """
    Write job information from sacct (elapsed time, memory used, number of
    completed, failed and running jobs in the queue) to a file.
    """
    # Set directory and file path
    log_directory = Path(options.directory) / "log"
    log_directory.mkdir(exist_ok=True, parents=True)
    file_path = log_directory / "job_information.csv"

    sacct_output = run_sacct()
    jobs_df = get_sacct_output(sacct_output)

    # Fetch sacct output and prepare the data
    jobs_df_filtered = jobs_df.copy()
    jobs_df_filtered = jobs_df_filtered.dropna()
    # Remove the G from MaxRSS value and convert to float
    # jobs_df_filtered["MaxRSS"] = jobs_df_filtered["MaxRSS"].str.strip("G").astype(float)

    jobs_df_filtered.to_csv(file_path, index=False, sep=",")


def plot_job_statistics(sacct_output: pd.DataFrame, directory: Path):
    """
    Get statistics of the jobs. Check elapsed time used,
    the memory used, the number of jobs completed, the number of jobs failed,
    the number of jobs running, the number of jobs queued.
    It will fetch the information from the sacct output.

    Parameters
    ----------
    sacct_output: pd.DataFrame
    directory: Path
        Directory to save the plot.
    """
    # TODO: this function will be called in the closer loop after all
    #  the jobs are done for a given production.

    # Plot a 2D histogram of the used memory (MaxRSS) as a function of the
    # elapsed time taking also into account the State of the job.
    sacct_output_filter = sacct_output.copy()
    sacct_output_filter = sacct_output_filter.dropna()
    # Remove the G from MaxRSS value and convert to float
    sacct_output_filter["MaxRSS"] = sacct_output_filter["MaxRSS"].str.strip("G").astype(float)

    plt.figure()
    plt.hist2d(sacct_output_filter.MaxRSS, sacct_output_filter.CPUTimeRAW / 3600, bins=50)
    plt.xlabel("MaxRSS [GB]")
    plt.ylabel("Elapsed time [h]")
    directory.mkdir(exist_ok=True, parents=True)
    plot_path = directory / "job_statistics.pdf"
    plt.savefig(plot_path)


def scheduler_env_variables(sequence, scheduler="slurm"):
    """Return the environment variables for the scheduler."""
    # TODO: Create a class with the SBATCH variables we want to use in the pilot job
    #  and then use the string representation of the class to create the header.
    if scheduler != "slurm":
        log.warning("No other schedulers are currently supported")
        return None

    sbatch_parameters = [
        f"--job-name={sequence.jobname}",
        f"--time={cfg.get('SLURM', 'WALLTIME')}",
        f"--chdir={options.directory}",
        f"--output=log/Run{sequence.run:05d}.%4a_jobid_%A.out",
        f"--error=log/Run{sequence.run:05d}.%4a_jobid_%A.err",
    ]

    # Get the number of subruns counting from 0.
    subruns = sequence.subruns - 1

    # Depending on the type of sequence, we need to set
    # different sbatch environment variables
    if sequence.type == "DATA":
        sbatch_parameters.append(f"--array=0-{subruns}")

    sbatch_parameters.append(f"--partition={cfg.get('SLURM', f'PARTITION_{sequence.type}')}")
    sbatch_parameters.append(f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE_{sequence.type}')}")

    return ["#SBATCH " + line for line in sbatch_parameters]


def job_header_template(sequence):
    """
    Returns a string with the job header template
    including SBATCH environment variables for sequencerXX.py script

    Parameters
    ----------
    sequence: sequence object

    Returns
    -------
    header: str
        String with job header template
    """
    python_shebang = "#!/bin/env python"
    if options.test:
        return python_shebang
    sbatch_parameters = "\n".join(scheduler_env_variables(sequence))
    return python_shebang + 2 * "\n" + sbatch_parameters


def set_cache_dirs():
    """
    Export cache directories for the jobs provided they
    are defined in the config file.

    Returns
    -------
    content: string
        String with the command to export the cache directories
    """

    ctapipe_cache = cfg.get("CACHE", "CTAPIPE_CACHE")
    ctapipe_svc_path = cfg.get("CACHE", "CTAPIPE_SVC_PATH")
    mpl_config_path = cfg.get("CACHE", "MPLCONFIGDIR")

    content = []
    if ctapipe_cache:
        content.append(f"os.environ['CTAPIPE_CACHE'] = '{ctapipe_cache}'")

    if ctapipe_svc_path:
        content.append(f"os.environ['CTAPIPE_SVC_PATH'] = '{ctapipe_svc_path}'")

    if mpl_config_path:
        content.append(f"os.environ['MPLCONFIGDIR'] = '{mpl_config_path}'")

    return "\n".join(content)


def data_sequence_job_template(sequence):
    """
    This file contains instruction to be submitted to job scheduler.

    Parameters
    ----------
    sequence : sequence object

    Returns
    -------
    job_template : string
    """
    # TODO: refactor this function creating wrappers that handle slurm part

    # Get the job header template.
    job_header = job_header_template(sequence)

    flat_date = date_to_dir(options.date)

    commandargs = ["datasequence"]

    if options.verbose:
        commandargs.append("-v")
    if options.simulate:
        commandargs.append("-s")
    if options.configfile:
        commandargs.extend(("--config", f"{Path(options.configfile).resolve()}"))
    if sequence.type == "DATA" and options.no_dl2:
        commandargs.append("--no-dl2")

    commandargs.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
            f"--drs4-pedestal-file={sequence.drs4_file}",
            f"--time-calib-file={sequence.time_calibration_file}",
            f"--pedcal-file={sequence.calibration_file}",
            f"--systematic-correction-file={sequence.systematic_correction_file}",
            f"--drive-file={get_drive_file(flat_date)}",
            f"--run-summary={get_summary_file(flat_date)}",
        )
    )

    content = job_header + "\n" + PYTHON_IMPORTS

    if not options.test:
        content += set_cache_dirs()
        content += "\n"
        # Use the SLURM env variables
        content += "subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    else:
        # Just process the first subrun without SLURM
        content += "subruns = 0\n"

    content += "\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"

    content += TAB + "proc = subprocess.run([\n"

    for arg in commandargs:
        content += TAB * 2 + f"'{arg}',\n"

    if pedestal_ids_file_exists(sequence.run):
        pedestal_ids_file = get_pedestal_ids_file(sequence.run, flat_date)
        content += TAB * 2 + f"f'--pedestal-ids-file={pedestal_ids_file}',\n"

    content += TAB * 2 + f"f'{sequence.run:05d}.{{subruns:04d}}',\n"

    content += TAB * 2 + f"'{options.tel_id}'\n"
    content += TAB + "])\n"
    content += "\n"
    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        write_to_file(sequence.script, content)

    return content


def calibration_sequence_job_template(sequence):
    """
    This file contains instruction to be submitted to job scheduler.

    Parameters
    ----------
    sequence : sequence object

    Returns
    -------
    job_template : string
    """

    # Get the job header template.
    job_header = job_header_template(sequence)

    commandargs = ["calibration_pipeline"]

    if options.verbose:
        commandargs.append("-v")
    if options.simulate:
        commandargs.append("-s")
    if options.configfile:
        commandargs.extend(("--config", f"{Path(options.configfile).resolve()}"))
    commandargs.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--drs4-pedestal-run={sequence.drs4_run:05d}",
            f"--pedcal-run={sequence.run:05d}",
        )
    )

    content = job_header + "\n" + PYTHON_IMPORTS

    if not options.test:
        content += set_cache_dirs()
        content += "\n"
        # Use the SLURM env variables
        content += "subruns = os.getenv('SLURM_ARRAY_TASK_ID')\n"
    else:
        # Just process the first subrun without SLURM
        content += "subruns = 0\n"

    content += "\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"

    content += TAB + "proc = subprocess.run([\n"

    for arg in commandargs:
        content += TAB * 2 + f"'{arg}',\n"

    content += TAB * 2 + f"'{options.tel_id}'\n"
    content += TAB + "])\n"
    content += "\n"
    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        write_to_file(sequence.script, content)

    return content


def submit_jobs(sequence_list, batch_command="sbatch"):
    """
    Submit the jobs to the cluster.

    Parameters
    ----------
    sequence_list: list
        List of sequences to submit.
    batch_command: str
        The batch command to submit the job (Default: sbatch)

    Returns
    -------
    job_list: list
        List of submitted job IDs.
    """
    job_list = []
    no_display_backend = "--export=ALL,MPLBACKEND=Agg"

    for sequence in sequence_list:
        commandargs = [batch_command, "--parsable", no_display_backend]
        if sequence.type == "PEDCALIB":
            commandargs.append(str(sequence.script))
            if options.simulate or options.no_calib or options.test:
                log.debug("SIMULATE Launching scripts")
            else:
                try:
                    log.debug(f"Launching script {sequence.script}")
                    parent_jobid = sp.check_output(
                        commandargs, universal_newlines=True, shell=False
                    ).split()[0]
                except sp.CalledProcessError as error:
                    rc = error.returncode
                    log.exception(f"Command '{batch_command}' not found, error {rc}")

            log.debug(stringify(commandargs))

        # Here sequence.jobid has not been redefined, so it keeps the one
        # from previous time sequencer was launched.

        # Add the job dependencies after calibration sequence
        if sequence.type == "DATA":
            if not options.simulate and not options.no_calib and not options.test:
                log.debug("Adding dependencies to job submission")
                depend_string = f"--dependency=afterok:{parent_jobid}"
                commandargs.append(depend_string)

            commandargs.append(sequence.script)

            if options.simulate:
                log.debug("SIMULATE Launching scripts")
            elif options.test:
                log.debug(
                    "TEST launching datasequence scripts for " "first subrun without scheduler"
                )
                commandargs = ["python", sequence.script]
                sp.check_output(commandargs, shell=False)
            else:
                log.info("Submitting jobs to the cluster.")
                try:
                    log.debug(f"Launching script {sequence.script}")
                    sp.check_output(commandargs, shell=False)
                except sp.CalledProcessError as error:
                    log.exception(error)

            log.debug(stringify(commandargs))

        job_list.append(sequence.script)

    return job_list


def run_squeue() -> StringIO:
    """Run squeue command to get the status of the jobs."""
    if shutil.which("squeue") is None:
        log.warning("No job info available since squeue command is not available")
        return StringIO()

    out_fmt = "%i;%j;%T;%M"  # JOBID, NAME, STATE, TIME
    return StringIO(sp.check_output(["squeue", "--me", "-o", out_fmt]).decode())


def get_squeue_output(squeue_output: StringIO) -> pd.DataFrame:
    """
    Obtain the current job information from squeue output
    and return a pandas dataframe.
    """
    df = pd.read_csv(squeue_output, delimiter=";")
    df.rename(
        inplace=True,
        columns={
            "STATE": "State",
            "JOBID": "JobID",
            "NAME": "JobName",
            "TIME": "CPUTime",
        },
    )

    # Keep only the jobs corresponding to OSA sequences
    df = df[df["JobName"].str.contains("LST1")]

    try:
        # Remove the job array part of the jobid
        df["JobID"] = df["JobID"].apply(lambda x: x.split("_")[0]).astype("int")
    except AttributeError:
        log.debug("No job info could be obtained from squeue")

    df["CPUTimeRAW"] = df["CPUTime"].apply(time_to_seconds)

    return df


def run_sacct() -> StringIO:
    """Run sacct to obtain the job information."""
    if shutil.which("sacct") is None:
        log.warning("No job info available since sacct command is not available")
        return StringIO()

    sacct_cmd = [
        "sacct",
        "-n",
        "--parsable2",
        "--delimiter=,",
        "--units=G",
        "-o",
        ",".join(FORMAT_SLURM),
    ]
    if cfg.get("SLURM", "STARTTIME_DAYS_SACCT"):
        days = int(cfg.get("SLURM", "STARTTIME_DAYS_SACCT"))
        start_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
        sacct_cmd.extend(["--starttime", start_date])

    return StringIO(sp.check_output(sacct_cmd).decode())


def get_sacct_output(sacct_output: StringIO) -> pd.DataFrame:
    """
    Fetch the information of jobs in the queue using the sacct SLURM output
    and store it in a pandas dataframe.

    Returns
    -------
    queue_list: pd.DataFrame
    """
    sacct_output = pd.read_csv(sacct_output, names=FORMAT_SLURM)

    # Keep only the jobs corresponding to OSA sequences
    sacct_output = sacct_output[
        (sacct_output["JobName"].str.contains("batch"))
        | (sacct_output["JobName"].str.contains("LST1"))
    ]

    try:
        sacct_output["JobID"] = sacct_output["JobID"].apply(lambda x: x.split("_")[0])
        sacct_output["JobID"] = sacct_output["JobID"].str.strip(".batch").astype(int)

    except AttributeError:
        log.debug("No job info could be obtained from sacct")

    return sacct_output


def get_closer_sacct_output(sacct_output) -> pd.DataFrame:
    """
    Fetch the information of jobs in the queue launched by AUTOCLOSER using the sacct 
    SLURM output and store it in a pandas dataframe.

    Returns
    -------
    queue_list: pd.DataFrame
    """
    sacct_output = pd.read_csv(sacct_output, names=FORMAT_SLURM)

    # Keep only the jobs corresponding to AUTOCLOSER sequences 
    # Until the merging of muon files is fixed, check all jobs except "lstchain_merge_muon_files"
    sacct_output = sacct_output[
        (sacct_output["JobName"].str.contains("lstchain_merge_hdf5_files"))
        | (sacct_output["JobName"].str.contains("lstchain_check_dl1"))
        | (sacct_output["JobName"].str.contains("lstchain_longterm_dl1_check"))
        | (sacct_output["JobName"].str.contains("lstchain_cherenkov_transparency"))
        | (sacct_output["JobName"].str.contains("provproces"))
    ]

    try:
        sacct_output["JobID"] = sacct_output["JobID"].apply(lambda x: x.split("_")[0])
        sacct_output["JobID"] = sacct_output["JobID"].str.strip(".batch").astype(int)

    except AttributeError:
        log.debug("No job info could be obtained from sacct")

    return sacct_output


def filter_jobs(job_info: pd.DataFrame, sequence_list: Iterable):
    """Filter the job info list to get the values of the jobs in the current queue."""
    sequences_info = pd.DataFrame([vars(seq) for seq in sequence_list])
    # Keep the jobs in the sacct output that are present in the sequence list
    return job_info[job_info["JobName"].isin(sequences_info["jobname"])]


def set_queue_values(
    sacct_info: pd.DataFrame, squeue_info: pd.DataFrame, sequence_list: Iterable
) -> None:
    """
    Extract job info from sacct output and
    fetch them into the table of sequences.

    Parameters
    ----------
    sacct_info: pd.DataFrame
    squeue_info: pd.DataFrame
    sequence_list: list[Sequence object]
    """
    if sacct_info.empty and squeue_info.empty or sequence_list is None:
        return

    job_info = pd.concat([sacct_info, squeue_info])

    # Filter the jobs in the sacct output that are present in the sequence list
    job_info_filtered = filter_jobs(job_info, sequence_list)

    for sequence in sequence_list:
        df_jobname = job_info_filtered[job_info_filtered["JobName"] == sequence.jobname]
        sequence.tries = df_jobname["JobID"].nunique()
        sequence.action = "Check"

        if not df_jobname.empty:
            sequence.jobid = df_jobname["JobID"].max()  # Get latest JobID
            df_jobid_filtered = df_jobname[df_jobname["JobID"] == sequence.jobid]
            try:
                sequence.cputime = time.strftime(
                    "%H:%M:%S",
                    time.gmtime(df_jobid_filtered["CPUTimeRAW"].median(skipna=False)),
                )
            except ValueError:
                sequence.cputime = None

            update_sequence_state(sequence, df_jobid_filtered)


def update_sequence_state(sequence, filtered_job_info: pd.DataFrame) -> None:
    """
    Update the state of the sequence based on the job info.

    Parameters
    ----------
    sequence: Sequence object
    filtered_job_info: pd.DataFrame
    """
    if (filtered_job_info.State.values == "COMPLETED").all():
        sequence.state = "COMPLETED"
        sequence.exit = filtered_job_info["ExitCode"].iloc[0]
    elif (filtered_job_info.State.values == "PENDING").all():
        sequence.state = "PENDING"
    elif any("FAILED" in job for job in filtered_job_info.State):
        sequence.state = "FAILED"
        sequence.exit = filtered_job_info[filtered_job_info.State.values == "FAILED"][
            "ExitCode"
        ].iloc[0]
    elif any("CANCELLED" in job for job in filtered_job_info.State):
        sequence.state = "CANCELLED"
        mask = ["CANCELLED" in job for job in filtered_job_info.State]
        sequence.exit = filtered_job_info[mask]["ExitCode"].iloc[0]
    elif any("TIMEOUT" in job for job in filtered_job_info.State):
        sequence.state = "TIMEOUT"
        sequence.exit = "0:15"
    elif any("RUNNING" in job for job in filtered_job_info.State):
        sequence.state = "RUNNING"
