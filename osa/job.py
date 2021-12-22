"""Functions to handle the interaction with the job scheduler."""

import datetime
import logging
import shutil
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import List, Iterable

import matplotlib.pyplot as plt
import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.report import history
from osa.utils.iofile import write_to_file
from osa.utils.utils import date_in_yymmdd, lstdate_to_dir, time_to_seconds, stringify

log = logging.getLogger(__name__)

__all__ = [
    "run_program_with_history_logging",
    "are_all_jobs_correctly_finished",
    "historylevel",
    "prepare_jobs",
    "sequence_filenames",
    "sequence_calibration_filenames",
    "set_queue_values",
    "job_header_template",
    "plot_job_statistics",
    "scheduler_env_variables",
    "create_job_template",
    "set_cache_dirs",
    "setrunfromparent",
    "submit_jobs",
    "check_history_level",
    "get_sacct_output",
    "get_squeue_output",
    "filter_jobs",
    "run_sacct",
    "run_squeue",
    "get_time_calibration_file",
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
            elif out == 1 and options.no_dl2:
                log.debug(
                    f"Job {sequence.seq} ({sequence.type}) correctly "
                    f"finished up to DL1ab, but no-dL2 option selected"
                )
                continue
            else:
                log.warning(
                    f"Job {sequence.seq} (run {sequence.run}) not "
                    f"correctly finished [level {out}]"
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
            if program in program_levels and exit_status != 0:
                level = program_levels[program]
                return level, exit_status
            if program in program_levels and exit_status == 0:
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
        log.error(f"Type {data_type} not expected")
        sys.exit(1)

    exit_status = 0

    if history_file.exists():
        for line in history_file.read_text().splitlines():
            words = line.split()
            try:
                program = words[1]
                prod_id = words[2]
                exit_status = int(words[-1])
                log.debug(
                    f"{program}, finished with error {exit_status} and prod ID {prod_id}"
                )
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
                        log.debug(
                            f"DL1ab prod ID: {options.dl1_prod_id} already produced"
                        )
                        level = 2
                    else:
                        level = 3
                        log.debug(
                            f"DL1ab prod ID: {options.dl1_prod_id} not produced yet"
                        )
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
                    log.warning(f"Program name not identified {program}")

    return level, exit_status


def prepare_jobs(sequence_list):
    """Prepare job file template for each sequence."""
    for sequence in sequence_list:
        log.debug(f"Creating sequence.py for sequence {sequence.seq}")
        create_job_template(sequence)


def setrunfromparent(sequence_list):
    """
    Create a dictionary with run number and its parent run number.

    Parameters
    ----------
    sequence_list

    Returns
    -------
    dictionary with run number and its parent run number
    """
    dictionary = {}
    for s1 in sequence_list:
        if s1.parent is not None:
            for s2 in sequence_list:
                if s2.seq == s1.parent:
                    log.debug(f"Assigning run from parent({s1.parent}) = {s2.run}")
                    dictionary[s1.parent] = s2.run
                    break
    return dictionary


def sequence_filenames(sequence):
    """Build names of the script, veto and history files."""
    script_suffix = ".py"
    history_suffix = ".history"
    veto_suffix = ".veto"
    basename = f"sequence_{sequence.jobname}"

    sequence.script = Path(options.directory) / f"{basename}{script_suffix}"
    sequence.veto = Path(options.directory) / f"{basename}{veto_suffix}"
    sequence.history = Path(options.directory) / f"{basename}{history_suffix}"


def get_time_calibration_file(run_id: int) -> Path:
    """
    Return the time calibration file corresponding to a calibration run taken before
    the run id given. If run_id is smaller than the first run id from the time
    calibration files, return the first time calibration file available, which
    corresponds to 1625.
    """

    time_calibration_dir = Path(cfg.get("LST1", "TIMECALIB_DIR"))
    file_list = sorted(time_calibration_dir.rglob("pro/time_calibration.Run*.h5"))

    if not file_list:
        raise IOError("No time calibration file found")

    for file in file_list:
        run_in_list = int(file.name.split(".")[1].strip("Run"))
        if run_id < 1625:
            time_calibration_file = file_list[0]
        elif run_in_list <= run_id:
            time_calibration_file = file
        else:
            break

    return time_calibration_file.resolve()


def sequence_calibration_filenames(sequence_list):
    """Build names of the calibration and drive files."""
    nightdir = lstdate_to_dir(options.date)
    yy_mm_dd = date_in_yymmdd(nightdir)
    drive_file = f"drive_log_{yy_mm_dd}.txt"

    for sequence in sequence_list:

        if not sequence.parent_list:
            drs4_pedestal_run_id = sequence.previousrun
            calibration_run_id = sequence.run
        else:
            drs4_pedestal_run_id = sequence.parent_list[0].previousrun
            calibration_run_id = sequence.parent_list[0].run

        drs4_pedestal_file = f"drs4_pedestal.Run{drs4_pedestal_run_id:05d}.0000.h5"
        calibration_file = f"calibration_filters_52.Run{calibration_run_id:05d}.0000.h5"

        # Assign the calibration and drive files to the sequence object
        sequence.drive = drive_file
        sequence.pedestal = (
            Path(cfg.get("LST1", "PEDESTAL_DIR")) / nightdir / "pro" / drs4_pedestal_file
        )
        sequence.calibration = (
            Path(cfg.get("LST1", "CALIB_DIR")) / nightdir / "pro" / calibration_file
        )
        sequence.time_calibration = get_time_calibration_file(calibration_run_id)


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
    sacct_output_filter["MaxRSS"] = (
        sacct_output_filter["MaxRSS"].str.strip("G").astype(float)
    )

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
        "--cpus-per-task=1",
        f"--chdir={options.directory}",
        f"--output=log/slurm_{sequence.run:05d}.%4a_%A.out",
        f"--error=log/slurm_{sequence.run:05d}.%4a_%A.err",
    ]

    # Get the number of subruns. The number of subruns starts counting from 0.
    subruns = int(sequence.subrun_list[-1].subrun) - 1

    # Depending on the type of sequence, we need to set
    # different sbatch environment variables
    if sequence.type == "DATA":
        sbatch_parameters.append(f"--array=0-{subruns}")

    sbatch_parameters.append(
        f"--partition={cfg.get('SLURM', f'PARTITION_{sequence.type}')}"
    )
    sbatch_parameters.append(
        f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE_{sequence.type}')}"
    )

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


def create_job_template(sequence, get_content=False):
    """
    This file contains instruction to be submitted to job scheduler.

    Parameters
    ----------
    sequence : sequence object
    get_content: bool

    Returns
    -------
    job_template : string
    """
    # TODO: refactor this function creating wrappers that handle slurm part

    # Get the job header template.
    job_header = job_header_template(sequence)

    nightdir = lstdate_to_dir(options.date)
    drivedir = cfg.get("LST1", "DRIVE_DIR")
    run_summary_dir = cfg.get("LST1", "RUN_SUMMARY_DIR")

    if sequence.type == "PEDCALIB":
        command = "calibration_pipeline"
    elif sequence.type == "DATA":
        command = "datasequence"
    else:
        log.error(f"Unknown sequence type {sequence.type}")
        command = None

    commandargs = [command]

    if options.verbose:
        commandargs.append("-v")
    if options.simulate:
        commandargs.append("-s")
    if options.configfile:
        commandargs.append("--config")
        commandargs.append(f"{Path(options.configfile).resolve()}")
    if sequence.type == "DATA" and options.no_dl2:
        commandargs.append("--no-dl2")

    commandargs.append(f"--date={options.date}")

    if sequence.type == "PEDCALIB":
        commandargs.append(f"--drs4-pedestal-run={sequence.previousrun:05d}")
        commandargs.append(f"--pedcal-run={sequence.run:05d}")

    if sequence.type == "DATA":
        run_summary_file = Path(run_summary_dir) / f"RunSummary_{nightdir}.ecsv"
        commandargs.append(f"--prod-id={options.prod_id}")
        commandargs.append(f"--drs4-pedestal-file={sequence.pedestal.resolve()}")
        commandargs.append(f"--time-calib-file={sequence.time_calibration.resolve()}")
        commandargs.append(f"--pedcal-file={sequence.calibration.resolve()}")
        commandargs.append(f"--drive-file={Path(drivedir).resolve() / sequence.drive}")
        commandargs.append(f"--run-summary={run_summary_file.resolve()}")

    python_imports = dedent(
        """\

        import os
        import subprocess
        import sys
        import tempfile

        """
    )
    content = job_header + "\n" + python_imports

    if not options.test:
        content += set_cache_dirs()
        content += "\n"
        # Use the SLURM env variables
        content += "subruns = os.getenv('SLURM_ARRAY_TASK_ID')\n"
        content += "job_id = os.getenv('SLURM_JOB_ID')\n"
    else:
        # Just process the first subrun without SLURM
        content += "subruns = 0\n"

    content += "\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"

    content += TAB + "proc = subprocess.run([\n"
    for i in commandargs:
        content += TAB * 2 + f"'{i}',\n"
    if not options.test:
        content += (
            TAB * 2
            + f"'--stderr=log/sequence_{sequence.jobname}."
            + "{0}_{1}.err'.format(str(subruns).zfill(4), str(job_id)),\n"
        )
        content += (
            TAB * 2
            + f"'--stdout=log/sequence_{sequence.jobname}."
            + "{0}_{1}.out'.format(str(subruns).zfill(4), str(job_id)),\n"
        )
    if sequence.type == "DATA":
        content += (
            TAB * 2
            + "'{0}".format(str(sequence.run).zfill(5))
            + ".{0}'.format(str(subruns).zfill(4)),\n"
        )
    content += TAB * 2 + f"'{options.tel_id}'\n"
    content += TAB + "])\n"
    content += "\n"
    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        write_to_file(sequence.script, content)

    if get_content:
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
                    parent_jobid = subprocess.check_output(
                        commandargs, universal_newlines=True, shell=False
                    ).split()[0]
                except subprocess.CalledProcessError as error:
                    rc = error.returncode
                    log.exception(f"Command '{batch_command}' not found, error {rc}")

            log.debug(stringify(commandargs))

        # Here sequence.jobid has not been redefined, so it keeps the one
        # from previous time sequencer was launched.

        # Add the job dependencies after calibration sequence
        if sequence.parent_list and sequence.type == "DATA":
            if not options.simulate and not options.no_calib and not options.test:
                log.debug("Adding dependencies to job submission")
                depend_string = f"--dependency=afterok:{parent_jobid}"
                commandargs.append(depend_string)

            commandargs.append(sequence.script)

            if options.simulate:
                log.debug("SIMULATE Launching scripts")
            elif options.test:
                log.debug(
                    "TEST launching datasequence scripts for "
                    "first subrun without scheduler"
                )
                commandargs = ["python", sequence.script]
                subprocess.check_output(commandargs, shell=False)
            else:
                try:
                    log.debug(f"Launching script {sequence.script}")
                    subprocess.check_output(commandargs, shell=False)
                except subprocess.CalledProcessError as error:
                    log.exception(error)

            log.debug(commandargs)

        job_list.append(sequence.script)

    return job_list


def run_squeue() -> StringIO:
    """Run squeue command to get the status of the jobs."""
    if shutil.which("squeue") is None:
        log.warning("No job info available since sacct command is not available")
        return StringIO()

    out_fmt = "%i,%j,%T,%M"  # JOBID, NAME, STATE, TIME
    return StringIO(subprocess.check_output(["squeue", "--me", "-o", out_fmt]).decode())


def get_squeue_output(squeue_output: StringIO) -> pd.DataFrame:
    """
    Obtain the current job information from squeue output
    and return a pandas dataframe.
    """
    df = pd.read_csv(squeue_output)
    # Remove the job array part of the jobid
    df["JOBID"] = df["JOBID"].apply(lambda x: x.split("_")[0]).astype("int")
    df.rename(
        inplace=True,
        columns={
            "STATE": "State",
            "JOBID": "JobID",
            "NAME": "JobName",
            "TIME": "CPUTime",
        },
    )
    df["CPUTimeRAW"] = df["CPUTime"].apply(time_to_seconds)
    return df


def run_sacct() -> StringIO:
    """Run sacct to obtain the job information."""
    if shutil.which("sacct") is None:
        log.warning("No job info available since sacct command is not available")
        return StringIO()

    start_date = (datetime.date.today() - datetime.timedelta(weeks=1)).isoformat()
    sacct_cmd = [
        "sacct",
        "-n",
        "--parsable2",
        "--delimiter=,",
        "--units=G",
        "--starttime",
        start_date,
        "-o",
        ",".join(FORMAT_SLURM),
    ]
    return StringIO(subprocess.check_output(sacct_cmd).decode())


def get_sacct_output(sacct_output: StringIO) -> pd.DataFrame:
    """
    Fetch the information of jobs in the queue using the sacct SLURM output
    and store it in a pandas dataframe.

    Returns
    -------
    queue_list: pd.DataFrame
    """
    sacct_output = pd.read_csv(sacct_output, names=FORMAT_SLURM)
    sacct_output["JobID"] = sacct_output["JobID"].apply(lambda x: x.split("_")[0])
    sacct_output["JobID"] = sacct_output["JobID"].str.strip(".batch").astype(int)
    return sacct_output


def filter_jobs(job_info: pd.DataFrame, sequence_list: Iterable):
    """Filter the job info list to get the values of the jobs in the current queue."""
    sequences_info = pd.DataFrame([vars(seq) for seq in sequence_list])
    # Keep the jobs in the sacct output that are present in the sequence list
    return job_info[job_info["JobName"].isin(sequences_info["jobname"])]


def set_queue_values(
        sacct_info: pd.DataFrame,
        squeue_info: pd.DataFrame,
        sequence_list: Iterable
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
    elif any("FAIL" in job for job in filtered_job_info.State):
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


def run_program_with_history_logging(
        command_args: List[str],
        history_file: Path,
        run: str,
        prod_id: str,
        command: str,
        input_file=None,
        config_file=None,
):
    """
    Run the program and log the output in the history file

    Parameters
    ----------
    command_args: List[str]
    history_file: pathlib.Path
    run: str
    prod_id: str
    command: str
    input_file: pathlib.Path, optional
    config_file: pathlib.Path, optional

    Returns
    -------
    rc: int
        Return code of the program
    """
    try:
        log.info(f"Executing {stringify(command_args)}")
        rc = subprocess.run(command_args, check=True).returncode
    except subprocess.CalledProcessError as error:
        rc = error.returncode
        log.exception(f"Could not execute {stringify(command_args)}, error: {error}")

    history(
        run=run,
        prod_id=prod_id,
        stage=command,
        return_code=rc,
        history_file=history_file,
        input_file=input_file.name,
        config_file=config_file.name
    )

    if rc != 0:
        sys.exit(rc)

    return rc
