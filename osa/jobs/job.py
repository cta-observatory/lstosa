"""
Functions to handle the job submission using SLURM
"""

import datetime
import logging
import os
import subprocess
import sys
import time
from glob import glob

import matplotlib.pyplot as plt
import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import readfromfile, writetofile
from osa.utils.utils import date_in_yymmdd, lstdate_to_dir, time_to_seconds, stringify

log = logging.getLogger(__name__)

TAB = "\t".expandtabs(4)


def are_all_jobs_correctly_finished(seqlist):
    """

    Parameters
    ----------
    seqlist

    Returns
    -------

    """
    flag = True
    for s in seqlist:  # Run wise
        history_files_list = glob(rf"{options.directory}/*{s.run}*.history")  # Subrun wise
        for history_file in history_files_list:
            # TODO: s.history should be SubRunObj attribute not RunObj
            # s.history only working for CALIBRATION sequence (run-wise), since it is
            # looking for .../sequence_LST1_04180.history files
            # we need to check all the subrun wise history files
            # .../sequence_LST1_04180.XXXX.history
            out, rc = historylevel(history_file, s.type)
            if out == 0:
                log.debug(f"Job {s.seq} ({s.type}) correctly finished")
                continue
            elif out == 1 and options.nodl2:
                log.debug(
                    f"Job {s.seq} ({s.type}) correctly finished up to DL1ab, but noDL2 option selected"
                )
                continue
            else:
                log.warning(
                    f"Job {s.seq} (run {s.run}) not correctly/completely finished [level {out}]"
                )
                flag = False
    return flag


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

    Parameters
    ----------
    historyfile
    data_type: str
        Either 'DATA' or 'CALIBRATION'

    Returns
    -------

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
        for line in readfromfile(historyfile).splitlines():
            # FIXME: create a dict with the program, exit status and prod id to take into account
            # not only the last history line but also the others.
            words = line.split()
            try:
                program = words[1]
                prod_id = words[2]
                exit_status = int(words[-1])
                log.debug(f"{program}, finished with error {exit_status} and prod ID {prod_id}")
            except (IndexError, ValueError) as err:
                log.exception(f"Malformed history file {historyfile}, {err}")
            else:
                if program == cfg.get("LSTOSA", "R0-DL1"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "R0-DL1").split(",")]
                    level = 3 if exit_status in nonfatalrcs else 4
                elif program == "lstchain_dl1ab":
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "R0-DL1").split(",")]
                    if (exit_status in nonfatalrcs) and (prod_id == options.dl1_prod_id):
                        log.debug(f"DL1ab prod ID: {options.dl1_prod_id} already produced")
                        level = 2
                    else:
                        level = 3
                        log.debug(f"DL1ab prod ID: {options.dl1_prod_id} not produced yet")
                        break
                elif program == "lstchain_check_dl1":
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "R0-DL1").split(",")]
                    level = 1 if exit_status in nonfatalrcs else 2
                elif program == cfg.get("LSTOSA", "DL1-DL2"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "DL1-DL2").split(",")]
                    if (exit_status in nonfatalrcs) and (prod_id == options.dl2_prod_id):
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} already produced")
                        level = 0
                    else:
                        level = 1
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} not produced yet")
                elif program == "drs4_pedestal":
                    level = 2 if exit_status == 0 else 3
                elif program == "time_calibration":
                    level = 1 if exit_status == 0 else 2
                elif program == "charge_calibration":
                    level = 0 if exit_status == 0 else 1
                else:
                    log.error(f"Programme name not identified {program}")

    return level, exit_status


def preparejobs(sequence_list):
    for s in sequence_list:
        log.debug(f"Creating sequence.py for sequence {s.seq}")
        createjobtemplate(s)


def preparestereojobs(sequence_list):
    for s in sequence_list:
        log.debug(f"Creating sequence.py for stereo sequence {s.seq}")
        createjobtemplate(s)


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


def setsequencefilenames(s):
    script_suffix = cfg.get("LSTOSA", "SCRIPTSUFFIX")
    history_suffix = cfg.get("LSTOSA", "HISTORYSUFFIX")
    veto_suffix = cfg.get("LSTOSA", "VETOSUFFIX")
    basename = f"sequence_{s.jobname}"

    s.script = os.path.join(options.directory, basename + script_suffix)
    s.veto = os.path.join(options.directory, basename + veto_suffix)
    s.history = os.path.join(options.directory, basename + history_suffix)


def setsequencecalibfilenames(sequence_list):
    calib_suffix = cfg.get("LSTOSA", "CALIBSUFFIX")
    pedestal_suffix = cfg.get("LSTOSA", "PEDESTALSUFFIX")
    drive_suffix = cfg.get("LSTOSA", "DRIVESUFFIX")
    for s in sequence_list:
        if len(s.parent_list) == 0:
            cal_run_string = str(s.run).zfill(5)
            calfile = f"calibration.Run{cal_run_string}.0000{calib_suffix}"
            timecalfile = f"time_calibration.Run{cal_run_string}.0000{calib_suffix}"
            ped_run_string = str(s.previousrun).zfill(5)
            pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
            nightdir = lstdate_to_dir(options.date)
            yy_mm_dd = date_in_yymmdd(nightdir)
            drivefile = f"drive_log_{yy_mm_dd}{drive_suffix}"
        else:
            run_string = str(s.parent_list[0].run).zfill(5)
            ped_run_string = str(s.parent_list[0].previousrun).zfill(5)
            nightdir = lstdate_to_dir(options.date)
            yy_mm_dd = date_in_yymmdd(nightdir)
            if options.mode == "P":
                calfile = f"calibration.Run{run_string}.0000{calib_suffix}"
                timecalfile = f"time_calibration.Run{run_string}.0000{calib_suffix}"
                pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
                drivefile = f"drive_log_{yy_mm_dd}{drive_suffix}"
            elif options.mode in ["S", "T"]:
                log.error("Mode not implemented yet. Try with 'P' mode instead")
        s.calibration = calfile
        s.time_calibration = timecalfile
        s.pedestal = pedfile
        s.drive = drivefile


def guesscorrectinputcard(s):
    """Returns guessed input card for:
    datasequencer
    calibrationsequence
    stereosequence
    """
    # if it fails, return the default one
    # FIXME: implement a way of selecting Nov cfg file

    # bindir = cfg.get("LSTOSA", "PYTHONDIR")

    # try:
    #     # assert(s.kind_obs)
    #     assert s.source
    #     assert s.hv_setting
    #     assert s.moonfilter
    # except:
    #     return options.configfile
    #
    # # Non standard input cards.
    # input_card_str = ""
    # if input_card_str != "":
    #     return join(bindir, 'cfg', f'osa{input_card_str}.cfg')

    return os.path.abspath(options.configfile)


def get_job_statistics(sequence_list):
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
    df = pd.DataFrame(
        columns=["JobID", "JobName", "State", "Elapsed", "MaxRSS", "MaxVMSize"]
    )

    # Plot the an 2D histogram of the memory use (MaxRSS) as a function of the elapsed time
    # taking also into account the State of the job.
    plt.figure(figsize=(10, 8))
    plt.title("Elapsed time as a function of MaxRSS")
    plt.xlabel("MaxRSS")
    plt.ylabel("Elapsed time")
    plt.grid(True)
    plt.xscale("log")
    plt.yscale("log")
    plt.tight_layout()
    plt.hist2d(
        df["Elapsed"],
        df["MaxRSS"],
        bins=100
    )

    # TODO: this function will be called in the closer loop after all
    #  the jobs are done for a given production.
    return None


def scheduler_env_variables(sequence, scheduler="slurm"):
    """
    Return the environment variables for the scheduler.
    """
    # FIXME: Create a class with the SBATCH variables we want to use in the pilot job
    #  and then use the string representation of the class to create the header.
    if scheduler == "slurm":
        sbatch_parameters = [
            f"--job-name={sequence.jobname}",
            "--cpus-per-task=1",
            f"--chdir={options.directory}",
            f"--output=log/slurm_{sequence.run:05d}.%4a_%A.out",
            f"--error=log/slurm_{sequence.run:05d}.%4a_%A.err",
        ]

        # Get the number of subruns. The number of subruns starts counting from 0.
        subruns = int(sequence.subrun_list[-1].subrun) - 1

        # Depending on the type of sequence, we need to set different sbatch environment variables
        if sequence.type == "DATA":
            sbatch_parameters.append(f"--array=0-{subruns}")

        sbatch_parameters.append(
            f"--partition={cfg.get('SBATCH', f'PARTITION-{sequence.type}')}"
        )
        sbatch_parameters.append(
            f"--mem-per-cpu={cfg.get('SBATCH', f'MEMSIZE-{sequence.type}')}"
        )

        return ["SBATCH " + line for line in sbatch_parameters]

    else:
        log.warning("No other schedulers are currently supported")


def job_header_template(sequence):
    """
    Returns a string with the job header template
    including SBATCH environment variables for sequencerXX.py script

    Parameters
    ----------
    sequence: sequence object

    Returns
    -------
    String with job header template: string
    """
    python_shebang = "#!/bin/env python"
    sbatch_parameters = "\n".join(scheduler_env_variables(sequence))

    return python_shebang + 2 * "\n" + sbatch_parameters


def createjobtemplate(sequence, get_content=False):
    """This file contains instruction to be submitted to SLURM"""
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
        commandargs.append(guesscorrectinputcard(sequence))
    if options.compressed:
        commandargs.append("-z")
    if sequence.type == "DATA" and options.nodl2:
        commandargs.append("--nodl2")

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
        commandargs.append(os.path.join(run_summary_dir, f"RunSummary_{nightdir}.ecsv"))

    if sequence.type == "DATA":
        commandargs.append(os.path.join(options.directory, sequence.calibration))
        commandargs.append(os.path.join(options.directory, sequence.pedestal))
        commandargs.append(os.path.join(options.directory, "time_" + sequence.calibration))
        commandargs.append(os.path.join(drivedir, sequence.drive))
        commandargs.append(os.path.join(run_summary_dir, f"RunSummary_{nightdir}.ecsv"))

    content = job_header + '\n' + " ".join(commandargs)

    content += "import subprocess \n"
    content += "import sys, os \n"
    content += "import tempfile \n"
    content += "\n" * 2

    if not options.test:
        # Exporting some cache directories
        ctapipe_cache = cfg.get("CACHE", "CTAPIPE_CACHE")
        ctapipe_svc_path = cfg.get("CACHE", "CTAPIPE_SVC_PATH")
        mpl_config_path = cfg.get("CACHE", "MPLCONFIGDIR")

        if ctapipe_cache:
            content += f"os.environ['CTAPIPE_CACHE'] = '{ctapipe_cache}'\n"

        if ctapipe_svc_path:
            content += f"os.environ['CTAPIPE_SVC_PATH'] = '{ctapipe_svc_path}'\n"

        if mpl_config_path:
            content += f"os.environ['MPLCONFIGDIR'] = '{mpl_config_path}'\n"

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
                + "{0}_{1}.err'.format(str(subruns).zfill(4), str(job_id)), \n"
        )
        content += (
                TAB * 2
                + f"'--stdout=log/sequence_{sequence.jobname}."
                + "{0}_{1}.out'.format(str(subruns).zfill(4), str(job_id)), \n"
        )
    if sequence.type == "DATA":
        content += (
                TAB * 2 + "'{0}".format(str(sequence.run).zfill(5)) + ".{0}'.format(str(subruns).zfill(4)), \n"
        )
    content += TAB * 2 + f"'{options.tel_id}'\n"

    content += TAB + "])\n"

    content += "\n"

    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        writetofile(sequence.script, content)

    if get_content:
        return content


def submitjobs(sequence_list):
    job_list = []
    command = cfg.get("ENV", "SBATCHBIN")
    no_display_backend = "--export=ALL,MPLBACKEND=Agg"

    for s in sequence_list:
        commandargs = [command, "--parsable", no_display_backend]
        if s.type == "PEDCALIB":
            commandargs.append(s.script)
            if options.simulate or options.nocalib or options.test:
                log.debug("SIMULATE Launching scripts")
            else:
                try:
                    log.debug(f"Launching script {s.script}")
                    parent_jobid = subprocess.check_output(
                        commandargs,
                        universal_newlines=True,
                        shell=False
                    ).split()[0]
                except subprocess.CalledProcessError as Error:
                    log.exception(Error, 2)
                except OSError as err:
                    log.exception(f"Command '{command}' not found", err)
            log.debug(commandargs)

            # FIXME here s.jobid has not been redefined se it keeps the one from previous time sequencer was launched
        # Introduce the job dependencies after calibration sequence
        if len(s.parent_list) != 0 and s.type == "DATA":
            if not options.simulate and not options.nocalib and not options.test:
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
            commandargs.append(s.script)
            if options.simulate:
                log.debug("SIMULATE Launching scripts")
            elif options.test:
                log.debug("Test launching datasequence scripts for first subrun without sbatch")
                commandargs = ["python", s.script]
                subprocess.check_output(commandargs, shell=False)
            else:
                try:
                    log.debug(f"Launching script {s.script}")
                    subprocess.check_output(commandargs, shell=False)
                except subprocess.CalledProcessError as Error:
                    log.exception(Error, 2)
                except OSError as err:
                    log.exception(f"Command '{command}' not found", err)
            log.debug(commandargs)
        job_list.append(s.script)

    return job_list


def getqueuejoblist(sequence_list):
    """
    Fetch the information of jobs in the queue using the sacct SLURM command

    Parameters
    ----------
    sequence_list

    Returns
    -------

    """
    user = cfg.get("ENV", "USER")
    sacct_format = "--format=jobid%8,jobname%25,cputime,elapsed,state,exitcode"
    start_date = (datetime.date.today() - datetime.timedelta(weeks=1)).isoformat()
    commandargs = ["sacct", "-u", user, "--starttime", start_date, sacct_format]
    queue_list = []
    try:
        sacct_output = subprocess.check_output(
            commandargs,
            universal_newlines=True,
            shell=False
        )
    except subprocess.CalledProcessError as Error:
        log.exception(f"Command '{stringify(commandargs)}' failed, {Error}")
    except OSError as Error:
        log.exception(f"Command '{stringify(commandargs)}' failed, {Error}")
    else:
        queue_header = sacct_output.splitlines()[0].split()
        queue_lines = (
            sacct_output.replace("+", "")
            .replace("sequence_", "")
            .replace(".py", "")
            .splitlines()[2:]
        )
        queue_sequences = [line.split() for line in queue_lines if "batch" not in line]
        queue_list = [dict(zip(queue_header, sequence)) for sequence in queue_sequences]
        setqueuevalues(queue_list, sequence_list)

    return queue_list


def setqueuevalues(queue_list, sequence_list):
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
    sequences_df = pd.DataFrame([vars(s) for s in sequence_list])
    df_queue = pd.DataFrame.from_dict(queue_list)
    # Add column with elapsed seconds of the job run-time to be averaged
    if "JobName" in df_queue.columns:
        df_queue_filtered = df_queue[df_queue["JobName"].isin(sequences_df["jobname"])].copy()
        df_queue_filtered["DeltaTime"] = df_queue_filtered["CPUTime"].apply(time_to_seconds)
        for s in sequence_list:
            df_jobname = df_queue_filtered[df_queue_filtered["JobName"] == s.jobname]
            s.tries = len(df_jobname["JobID"].unique())
            s.action = "Check"
            try:
                s.jobid = max(df_jobname["JobID"])  # Get latest JobID
                df_jobid_filtered = df_jobname[df_jobname["JobID"] == s.jobid]
                s.cputime = time.strftime(
                    "%H:%M:%S", time.gmtime(df_jobid_filtered["DeltaTime"].median())
                )
                if (df_jobid_filtered.State.values == "COMPLETED").all():
                    s.state = "COMPLETED"
                    s.exit = df_jobid_filtered["ExitCode"].iloc[0]
                elif (df_jobid_filtered.State.values == "PENDING").all():
                    s.state = "PENDING"
                    s.exit = None
                elif (df_jobid_filtered.State.values == "FAILED").any():
                    s.state = "FAILED"
                    s.exit = df_jobid_filtered[df_jobid_filtered.State.values == "FAILED"][
                        "ExitCode"
                    ].iloc[0]
                elif (df_jobid_filtered.State.values == "CANCELLED").any():
                    s.state = "CANCELLED"
                    s.exit = df_jobid_filtered[df_jobid_filtered.State.values == "CANCELLED"][
                        "ExitCode"
                    ].iloc[0]
                elif (df_jobid_filtered.State.values == "TIMEOUT").any():
                    s.state = "TIMEOUT"
                    s.exit = "0:15"
                else:
                    s.state = "RUNNING"
                    s.exit = None
                log.debug(
                    f"Queue attributes: sequence {s.seq}, JobName {s.jobname}, "
                    f"JobID {s.jobid}, State {s.state}, CPUTime {s.cputime}, Exit {s.exit} updated"
                )
            except ValueError:
                log.debug(f"Queue attributes for sequence {s.seq} not present in sacct output.")
    else:
        log.debug("No jobs reported in sacct queue.")
