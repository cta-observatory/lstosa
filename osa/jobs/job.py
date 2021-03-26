"""
Functions to handle the job submission using SLURM
"""

import logging
import os
import subprocess
import sys
import time
from glob import glob

import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import readfromfile, writetofile
from osa.utils.standardhandle import stringify
from osa.utils.utils import date_in_yymmdd, lstdate_to_dir, time_to_seconds

log = logging.getLogger(__name__)


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
                log.debug(
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
            except IndexError as err:
                log.exception(f"Malformed history file {historyfile}, {err}")
            except ValueError as err:
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
        log.debug(f"Creating sequence.txt and sequence.py for sequence {s.seq}")
        # FIXME: creating txt should be deprecated at some point
        # createsequencetxt(s, sequence_list)
        createjobtemplate(s)


def preparestereojobs(sequence_list):
    for s in sequence_list:
        log.debug(f"Creating sequence.py for sequence {s.seq}")
        createjobtemplate(s)


def preparedailyjobs(sequence_list):
    for s in sequence_list:
        log.debug(f"Creating sequence.sh for source {s.name}")
        createjobtemplate(s)


def setrunfromparent(sequence_list):
    # this is a dictionary, seq -> parent's run number
    dictionary = {}
    for s1 in sequence_list:
        if s1.parent is not None:
            for s2 in sequence_list:
                if s2.seq == s1.parent:
                    log.debug(f"Assigning runfromparent({s1.parent}) = {s2.run}")
                    dictionary[s1.parent] = s2.run
                    break
    return dictionary


def createsequencetxt(s, sequence_list):
    # Deprecated, right now we do not use this txt file. Everything is already
    # present in the sequenceXX.py script

    text_suffix = cfg.get("LSTOSA", "TEXTSUFFIX")
    f = os.path.join(options.directory, f"sequence_{s.jobname}{text_suffix}")
    start = s.subrun_list[0].timestamp
    ped = ""
    cal = ""
    dat = ""
    if s.type == "PEDCAL":
        ped = formatrunsubrun(s.previousrun, 1)
        cal = formatrunsubrun(s.run, 1)
    elif s.type == "DATA":
        ped = formatrunsubrun(s.parent_list[0].previousrun, 1)
        cal = formatrunsubrun(s.parent_list[0].run, 1)
        for sub in s.subrun_list:
            dat += formatrunsubrun(s.run, sub.subrun) + " "

    content = "# Sequence number (identifier)\n"
    content += f"Sequence: {s.run}\n"
    content += "# Date of sunrise of the observation night\n"
    content += f"Night: {s.night}\n"
    content += "# Start time of the sequence (first data run)\n"
    content += f"Start: {start}\n"
    content += "# Source name of all runs of sequence\n"
    content += f"Source: {s.sourcewobble}\n"
    content += f"Telescope: {options.tel_id.lstrip('M1')}\n"
    content += "\n"
    content += f"PedRuns: {ped}\n"
    content += f"CalRuns: {cal}\n"
    content += f"DatRuns: {dat}\n"

    if not options.simulate:
        writetofile(f, content)
    else:
        log.debug(f"SIMULATE Creating sequence txt {f}")


def formatrunsubrun(run, subrun):
    """
    It needs 5 digits for the runs and 4 digits for the subruns

    Parameters
    ----------
    run
    subrun

    Returns
    -------

    """
    if not run:
        run = 5 * "0"
    s = str(subrun).zfill(4)
    return f"{run}.{s}"


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

    return options.configfile


def createjobtemplate(s, get_content=False):
    """This file contains instruction to be submitted to SLURM"""

    nightdir = lstdate_to_dir(options.date)
    bindir = cfg.get("LSTOSA", "PYTHONDIR")
    scriptsdir = cfg.get("LSTOSA", "SCRIPTSDIR")
    drivedir = cfg.get("LST1", "DRIVEDIR")
    run_summary_dir = cfg.get("LST1", "RUN_SUMMARY_DIR")

    command = None
    if s.type == "PEDCALIB":
        command = os.path.join(scriptsdir, "calibrationsequence.py")
    elif s.type == "DATA":
        command = os.path.join(scriptsdir, "datasequence.py")
    elif s.type == "STEREO":
        command = os.path.join(scriptsdir, "stereosequence.py")

    commandargs = ["python", command]

    if options.verbose:
        commandargs.append("-v")
    if options.warning:
        commandargs.append("-w")
    if options.configfile:
        commandargs.append("-c")
        commandargs.append(os.path.join(bindir, guesscorrectinputcard(s)))
    if options.compressed:
        commandargs.append("-z")
    if s.type == "DATA" and options.nodl2:
        commandargs.append("--nodl2")

    commandargs.append("-d")
    commandargs.append(options.date)
    commandargs.append("--prod-id")
    commandargs.append(options.prod_id)

    if s.type == "PEDCALIB":
        commandargs.append(s.pedestal)
        commandargs.append(s.calibration)
        ped_run_number = str(s.previousrun).zfill(5)
        cal_run_number = str(s.run).zfill(5)
        commandargs.append(ped_run_number)
        commandargs.append(cal_run_number)
        commandargs.append(os.path.join(run_summary_dir, f"RunSummary_{nightdir}.ecsv"))

    if s.type == "DATA":
        commandargs.append(os.path.join(options.directory, s.calibration))
        commandargs.append(os.path.join(options.directory, s.pedestal))
        commandargs.append(os.path.join(options.directory, "time_" + s.calibration))
        commandargs.append(os.path.join(drivedir, s.drive))
        commandargs.append(os.path.join(run_summary_dir, f"RunSummary_{nightdir}.ecsv"))

    for sub in s.subrun_list:
        # FIXME: This is getting the last subrun starting from 0
        # We should get this parameter differently.
        n_subruns = int(sub.subrun)

    # Build the content of the sequencerXX.py script
    content = "#!/bin/env python\n"
    # Set sbatch parameters
    content += "\n"
    content += f"#SBATCH -A {cfg.get('SBATCH', 'ACCOUNT')} \n"
    if s.type == "DATA":
        content += f"#SBATCH --array=0-{int(n_subruns) - 1} \n"
    content += "#SBATCH --cpus-per-task=1 \n"
    if s.type == "PEDCALIB":
        content += f"#SBATCH -p {cfg.get('SBATCH', 'PARTITION-CALI')} \n"
        content += f"#SBATCH --mem-per-cpu={cfg.get('SBATCH', 'MEMSIZE-CALI')} \n"
    else:
        content += f"#SBATCH -p {cfg.get('SBATCH', 'PARTITION-DATA')} \n"
        content += f"#SBATCH --mem-per-cpu={cfg.get('SBATCH', 'MEMSIZE-DATA')} \n"
    content += f"#SBATCH -D {options.directory} \n"
    content += f"#SBATCH -o log/slurm_{str(s.run).zfill(5)}.%4a_%A.out \n"
    content += f"#SBATCH -e log/slurm_{str(s.run).zfill(5)}.%4a_%A.err \n"
    content += "\n"

    content += "import subprocess \n"
    content += "import sys, os \n"
    content += "import tempfile \n"
    content += "\n\n"

    if not options.test:
        content += "subruns = os.getenv('SLURM_ARRAY_TASK_ID')\n"
        content += "job_id = os.getenv('SLURM_JOB_ID')\n"
    else:
        content += "subruns = 0\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += "    os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"

    content += "    proc = subprocess.run([\n"
    for i in commandargs:
        content += f"        '{i}',\n"
    if not options.test:
        content += (
            f"        '--stderr=log/sequence_{s.jobname}."
            + "{0}_{1}.err'.format(str(subruns).zfill(4), str(job_id)), \n"
        )
        content += (
            f"        '--stdout=log/sequence_{s.jobname}."
            + "{0}_{1}.out'.format(str(subruns).zfill(4), str(job_id)), \n"
        )
    if s.type == "DATA":
        content += (
            "        '{0}".format(str(s.run).zfill(5))
            + ".{0}'"
            + ".format(str(subruns).zfill(4))"
            + ",\n"
        )
    content += f"        '{options.tel_id}'\n"
    content += "        ])\n"

    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        writetofile(s.script, content)

    if get_content:
        return content


def submitjobs(sequence_list):
    job_list = []
    command = cfg.get("ENV", "SBATCHBIN")
    env_nodisplay = "--export=ALL,MPLBACKEND=Agg"
    for s in sequence_list:
        commandargs = [command, "--parsable", env_nodisplay]
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
                    ).split()[0]
                except subprocess.CalledProcessError as Error:
                    log.exception(Error, 2)
                except OSError as err:
                    log.exception(f"Command '{command}' not found", err)
            log.debug(commandargs)

            # FIXME here s.jobid has not been redefined se it keeps the one from previous time sequencer was launched
        # Introduce the job dependencies after calibration sequence
        if len(s.parent_list) != 0 and s.type == "DATA":
            log.debug("Adding dependencies to job submission")
            if not options.simulate and not options.nocalib:
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
                subprocess.check_output(commandargs)
            else:
                try:
                    log.debug(f"Launching script {s.script}")
                    subprocess.check_output(commandargs)
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
    commandargs = ["sacct", "-u", user, sacct_format]
    queue_list = []
    try:
        sacct_output = subprocess.check_output(commandargs, universal_newlines=True)
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
