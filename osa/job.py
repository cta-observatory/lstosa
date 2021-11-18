"""Functions to handle the interaction with the job scheduler."""

import datetime
import logging
import os
import shutil
import subprocess
import sys
import time
from glob import glob
from pathlib import Path
from textwrap import dedent

import matplotlib.pyplot as plt
import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import read_from_file, write_to_file
from osa.utils.utils import date_in_yymmdd, lstdate_to_dir, time_to_seconds, stringify


log = logging.getLogger(__name__)

TAB = "\t".expandtabs(4)

__all__ = [
    "are_all_jobs_correctly_finished",
    "historylevel",
    "prepare_jobs",
    "sequence_filenames",
    "sequence_calibration_filenames",
    "queue_job_list",
    "queue_values",
    "job_header_template",
    "scheduler_env_variables",
    "create_job_template",
    "set_cache_dirs",
    "setrunfromparent",
    "submit_jobs",
    "check_history_level"
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
    flag = True
    for sequence in sequence_list:
        history_files_list = glob(
            rf"{options.directory}/*{sequence.run}*.history"
        )
        for history_file in history_files_list:
            # TODO: s.history should be SubRunObj attribute not RunObj
            # s.history only working for CALIBRATION sequence (run-wise), since it is
            # looking for .../sequence_LST1_04180.history files
            # we need to check all the subrun wise history files
            # .../sequence_LST1_04180.XXXX.history
            out, rc = historylevel(history_file, sequence.type)
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
            elif program in program_levels and exit_status == 0:
                level = program_levels[program]
                continue

        return level, exit_status


def historylevel(historyfile, data_type):
    """
    Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file.

    For PEDCALIB sequences:
     - DRS4->time calib is level 3->2
     - time calib->charge calib is level 2->1
     - charge calib 1->0 (sequence completed)

    For DATA sequences:
     - R0->DL1 is level 4->3
     - DL1->DL1AB is level 3->2
     - DATACHECK is level 2->1
     - DL1->DL2 is level 1->0 (sequence completed)

     TODO: Create a dict with the program exit status and prod id to
      take into account not only the last history line but also the others.

    Parameters
    ----------
    historyfile
    data_type: str
        Either 'DATA' or 'CALIBRATION'

    Returns
    -------
    level, exit_status: int, int
    """
    if data_type == "DATA":
        level = 4
    elif data_type == "PEDCALIB":
        level = 3
    else:
        log.error("Type {data_type} not expected")
        sys.exit(1)
    exit_status = 0
    if os.path.exists(historyfile):
        for line in read_from_file(historyfile).splitlines():
            words = line.split()
            try:
                program = words[1]
                prod_id = words[2]
                exit_status = int(words[-1])
                log.debug(
                    f"{program}, finished with error {exit_status} and prod ID {prod_id}"
                )
            except (IndexError, ValueError) as err:
                log.exception(f"Malformed history file {historyfile}, {err}")
            else:
                if program == cfg.get("lstchain", "r0_to_dl1"):
                    level = 3 if exit_status == 0 else 4
                elif program == "lstchain_dl1ab":
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
                elif program == "lstchain_check_dl1":
                    level = 1 if exit_status == 0 else 2
                elif program == cfg.get("lstchain", "dl1_to_dl2"):
                    if (exit_status == 0) and (prod_id == options.dl2_prod_id):
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} already produced")
                        level = 0
                    else:
                        level = 1
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} not produced yet")
                elif program == "drs4_baseline":
                    level = 2 if exit_status == 0 else 3
                elif program == "time_calibration":
                    level = 1 if exit_status == 0 else 2
                elif program == "charge_calibration":
                    level = 0 if exit_status == 0 else 1
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
    Create a dictionary with run number and its the parent run number.

    Parameters
    ----------
    sequence_list

    Returns
    -------
    dictionary with run number and its the parent run number
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
    script_suffix = cfg.get("LSTOSA", "SCRIPTSUFFIX")
    history_suffix = cfg.get("LSTOSA", "HISTORYSUFFIX")
    veto_suffix = cfg.get("LSTOSA", "VETOSUFFIX")
    basename = f"sequence_{sequence.jobname}"

    sequence.script = Path(options.directory) / f"{basename}{script_suffix}"
    sequence.veto = Path(options.directory) / f"{basename}{veto_suffix}"
    sequence.history = Path(options.directory) / f"{basename}{history_suffix}"


def sequence_calibration_filenames(sequence_list):
    """Build names of the calibration and drive files."""
    nightdir = lstdate_to_dir(options.date)
    yy_mm_dd = date_in_yymmdd(nightdir)
    calib_suffix = cfg.get("LSTOSA", "CALIBSUFFIX")
    pedestal_suffix = cfg.get("LSTOSA", "PEDESTALSUFFIX")
    drive_suffix = cfg.get("LSTOSA", "DRIVESUFFIX")
    drivefile = f"drive_log_{yy_mm_dd}{drive_suffix}"

    for sequence in sequence_list:
        if not sequence.parent_list:
            cal_run_string = str(sequence.run).zfill(5)
            calfile = f"calibration.Run{cal_run_string}.0000{calib_suffix}"
            timecalfile = f"time_calibration.Run{cal_run_string}.0000{calib_suffix}"
            ped_run_string = str(sequence.previousrun).zfill(5)
        else:
            run_string = str(sequence.parent_list[0].run).zfill(5)
            ped_run_string = str(sequence.parent_list[0].previousrun).zfill(5)
            calfile = f"calibration.Run{run_string}.0000{calib_suffix}"
            timecalfile = f"time_calibration.Run{run_string}.0000{calib_suffix}"
        pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
        # Assign the calibration and drive files to the sequence object
        sequence.drive = drivefile
        sequence.calibration = Path(options.directory) / calfile
        sequence.time_calibration = Path(options.directory) / timecalfile
        sequence.pedestal = Path(options.directory) / pedfile


def get_job_statistics():
    """
    Get statistics of the jobs. Check elapsed time used,
    the memory used, the number of jobs completed, the number of jobs failed,
    the number of jobs running, the number of jobs queued.
    It will fetch the information from the sacct output.
    """
    # Call sacct output and take information from it.
    sacct_output = subprocess.check_output(
        ["sacct", "-X", "-n", "-o", "JobID,JobName,State,Elapsed,MaxRSS,MaxVMSize"]
    )
    sacct_output = sacct_output.decode("utf-8")
    sacct_output = sacct_output.split("\n")
    sacct_output = sacct_output[1:]  # remove header
    # Copy the information to a pandas dataframe.
    job_information = pd.DataFrame(
        columns=["JobID", "JobName", "State", "Elapsed", "MaxRSS", "MaxVMSize"]
    )

    # Plot the an 2D histogram of the memory use (MaxRSS) as a function of the
    # elapsed time taking also into account the State of the job.
    plt.figure(figsize=(10, 8))
    plt.title("Elapsed time as a function of MaxRSS")
    plt.xlabel("MaxRSS")
    plt.ylabel("Elapsed time")
    plt.grid(True)
    plt.xscale("log")
    plt.yscale("log")
    plt.tight_layout()
    plt.hist2d(job_information["Elapsed"], job_information["MaxRSS"], bins=100)

    # TODO: this function will be called in the closer loop after all
    #  the jobs are done for a given production.


def scheduler_env_variables(sequence, scheduler="slurm"):
    """
    Return the environment variables for the scheduler.
    """
    # TODO: Create a class with the SBATCH variables we want to use in the pilot job
    #  and then use the string representation of the class to create the header.
    if scheduler != "slurm":
        log.warning("No other schedulers are currently supported")
        return None
    else:
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
            f"--partition={cfg.get('SLURM', f'PARTITION-{sequence.type}')}"
        )
        sbatch_parameters.append(
            f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE-{sequence.type}')}"
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
    drivedir = cfg.get("LST1", "DRIVEDIR")
    run_summary_dir = cfg.get("LST1", "RUN_SUMMARY_DIR")

    if sequence.type == "PEDCALIB":
        command = "calibrationsequence"
    elif sequence.type == "DATA":
        command = "datasequence"
    else:
        log.error(f"Unknown sequence type {sequence.type}")
        command = None

    commandargs = [command]

    if options.verbose:
        commandargs.append("-v")
    if options.warning:
        commandargs.append("-w")
    if options.configfile:
        commandargs.append("-c")
        commandargs.append(Path(options.configfile).resolve())
    if options.compressed:
        commandargs.append("-z")
    if sequence.type == "DATA" and options.no_dl2:
        commandargs.append("--no-dl2")

    commandargs.append("-d")
    commandargs.append(options.date)
    commandargs.append("--prod-id")
    commandargs.append(options.prod_id)

    if sequence.type == "PEDCALIB":
        commandargs.append(sequence.pedestal)
        commandargs.append(sequence.calibration)
        ped_run_number = str(sequence.previousrun).zfill(5)
        cal_run_number = str(sequence.run).zfill(5)
        commandargs.append(ped_run_number)
        commandargs.append(cal_run_number)
        commandargs.append(Path(run_summary_dir) / f"RunSummary_{nightdir}.ecsv")

    if sequence.type == "DATA":
        commandargs.append(os.path.abspath(sequence.calibration.resolve()))
        commandargs.append(sequence.pedestal.resolve())
        commandargs.append(sequence.time_calibration.resolve())
        commandargs.append(Path(drivedir) / sequence.drive)
        commandargs.append(Path(run_summary_dir) / f"RunSummary_{nightdir}.ecsv")

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
            commandargs.append(sequence.script)
            if options.simulate or options.no_calib or options.test:
                log.debug("SIMULATE Launching scripts")
            else:
                try:
                    log.debug(f"Launching script {sequence.script}")
                    parent_jobid = subprocess.check_output(
                        commandargs, universal_newlines=True, shell=False
                    ).split()[0]
                except subprocess.CalledProcessError as error:
                    log.exception(error)
                except OSError as error:
                    log.exception(f"Command '{batch_command}' not found, error {error}")
            log.debug(commandargs)

            # FIXME here s.jobid has not been redefined se it keeps the one
            #  from previous time sequencer was launched
        # Add the job dependencies after calibration sequence
        if sequence.parent_list and sequence.type == "DATA":
            if not options.simulate and not options.no_calib and not options.test:
                log.debug("Adding dependencies to job submission")
                depend_string = f"--dependency=afterok:{parent_jobid}"
                commandargs.append(depend_string)
            # Old MAGIC style:
            # for pseq in s.parent_list:
            #     if pseq.jobid is not None:
            #         if int(pseq.jobid) > 0:
            #             depend_string += ":{0}".format(pseq.jobid)
            #        """ Skip vetoed """
            #        if s.action == 'Veto':
            #            log.debug("job {0} has been vetoed".format(s.jobname))
            #        elif s.action == 'Closed':
            #            log.debug("job {0} is already closed".format(s.jobname))
            #        elif s.action == 'Check' and s.state != 'C':
            #            log.debug("job {0} checked to be dispatched but not completed yet".format(s.jobname))
            #            if s.state == 'H' or s.state == 'R':
            #                # Reset values
            #                s.exit = None
            #                if s.state == 'H':
            #                    s.jobhost = None
            #                    s.cputime = None
            #                    s.walltime = None
            #        elif s.action == 'Check' and s.state == 'C' and s.exit == 0:
            #            log.debug("job {0} checked to be successful".format(s.jobname))
            #        else:
            #            if options.simulate == True:
            #                commandargs.insert(0, 'echo')
            #                s.action = 'Simulate'
            #                # This jobid is negative showing it belongs to a simulated environment (not real jobid)
            #                s.jobid = -1 - s.seq
            #            else:
            #                s.action = 'Submit'
            #                # Reset the values to avoid misleading info from previous jobs
            #                s.jobhost = None
            #                s.state = 'Q'
            #                s.cputime = None
            #                s.walltime = None
            #                s.exit = None
            #            try:
            #                stdout = subprocess.check_output(commandargs)
            #            except subprocess.CalledProcessError as Error:
            #                log.exception(Error, 2)
            #            except OSError (ValueError, NameError):
            #                log.exception("Command {0}, {1}".format(stringify(commandargs), NameError), ValueError)
            #            else:
            #                if options.simulate == False:
            #                    try:
            #                        s.jobid = int(stdout.split('.', 1)[0])
            #                    except ValueError as e:
            #                        log.warning("Wrong parsing of jobid {0} not being an integer, {1}".format(stdout.split('.', 1)[0], e))
            #        job_list.append(s.jobid)
            #        log.debug("{0} {1}".format(s.action, stringify(commandargs)))
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


def queue_job_list(sequence_list):
    """
    Fetch the information of jobs in the queue using the sacct SLURM command

    Parameters
    ----------
    sequence_list

    Returns
    -------
    queue_list: list
        List of jobs in the queue
    """
    if shutil.which('sacct') is None:
        log.warning("No job info available since sacct command is not available")
    else:
        sacct_format = "--format=jobid%8,jobname%25,cputime,elapsed,state,exitcode"
        start_date = (datetime.date.today() - datetime.timedelta(weeks=1)).isoformat()
        commandargs = ["sacct", "--starttime", start_date, sacct_format]
        queue_list = []
        try:
            sacct_output = subprocess.check_output(
                commandargs, universal_newlines=True, shell=False
            )
        except subprocess.CalledProcessError as error:
            log.exception(f"Command '{stringify(commandargs)}' failed, {error}")
        except OSError as error:
            log.exception(f"Command '{stringify(commandargs)}' failed, {error}")
        else:
            queue_header = sacct_output.splitlines()[0].split()
            # FIXME: jobs are no longer named squeue_* but LST1*
            queue_lines = (
                sacct_output.replace("+", "")
                .replace("sequence_", "")
                .replace(".py", "")
                .splitlines()[2:]
            )
            queue_sequences = [
                line.split() for line in queue_lines if "batch" not in line
            ]
            queue_list = [
                dict(zip(queue_header, sequence)) for sequence in queue_sequences
            ]
            queue_values(queue_list, sequence_list)

        return queue_list


def queue_values(queue_list, sequence_list):
    """
    Extract queue values and fetch them into the table of sequences

    Parameters
    ----------
    queue_list
    sequence_list
    """
    # Create data frames for sequence list and queue array
    if not sequence_list:
        return
    sequences_df = pd.DataFrame([vars(seq) for seq in sequence_list])
    df_queue = pd.DataFrame.from_dict(queue_list)
    # Add column with elapsed seconds of the job run-time to be averaged
    if "JobName" in df_queue.columns:
        df_queue_filtered = df_queue[
            df_queue["JobName"].isin(sequences_df["jobname"])
        ].copy()
        df_queue_filtered["DeltaTime"] = df_queue_filtered["CPUTime"].apply(
            time_to_seconds
        )
        for sequence in sequence_list:
            df_jobname = df_queue_filtered[
                df_queue_filtered["JobName"] == sequence.jobname
            ]
            sequence.tries = len(df_jobname["JobID"].unique())
            sequence.action = "Check"
            try:
                sequence.jobid = max(df_jobname["JobID"])  # Get latest JobID
                df_jobid_filtered = df_jobname[df_jobname["JobID"] == sequence.jobid]
                sequence.cputime = time.strftime(
                    "%H:%M:%S", time.gmtime(df_jobid_filtered["DeltaTime"].median())
                )
                if (df_jobid_filtered.State.values == "COMPLETED").all():
                    sequence.state = "COMPLETED"
                    sequence.exit = df_jobid_filtered["ExitCode"].iloc[0]
                elif (df_jobid_filtered.State.values == "PENDING").all():
                    sequence.state = "PENDING"
                    sequence.exit = None
                elif (df_jobid_filtered.State.values == "FAILED").any():
                    sequence.state = "FAILED"
                    sequence.exit = df_jobid_filtered[
                        df_jobid_filtered.State.values == "FAILED"
                        ]["ExitCode"].iloc[0]
                elif (df_jobid_filtered.State.values == "CANCELLED").any():
                    sequence.state = "CANCELLED"
                    sequence.exit = df_jobid_filtered[
                        df_jobid_filtered.State.values == "CANCELLED"
                        ]["ExitCode"].iloc[0]
                elif (df_jobid_filtered.State.values == "TIMEOUT").any():
                    sequence.state = "TIMEOUT"
                    sequence.exit = "0:15"
                else:
                    sequence.state = "RUNNING"
                    sequence.exit = None
                log.debug(
                    f"Queue attributes: sequence {sequence.seq},"
                    f"JobName {sequence.jobname}, "
                    f"JobID {sequence.jobid}, State {sequence.state}, "
                    f"CPUTime {sequence.cputime}, Exit {sequence.exit} updated"
                )
            except ValueError:
                log.debug(
                    f"Queue attributes for sequence {sequence.seq} "
                    f"not present in sacct output."
                )
    else:
        log.debug("No jobs reported in sacct queue.")