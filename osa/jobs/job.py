"""
Functions to handle the job submission using SLURM
"""

import datetime
import logging
import os
import subprocess
import time
from itertools import chain, islice, tee

import numpy as np

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import readfromfile, writetofile
from osa.utils.standardhandle import stringify
from osa.utils.utils import lstdate_to_dir

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
    for s in seqlist:
        out, rc = historylevel(s.history, s.type)
        if out == 0:
            log.debug(f"Job {s.seq} correctly finished")
            continue
        else:
            log.debug(f"Job {s.seq} not correctly/completely finished [{out}]")
            flag = False
    return flag


def historylevel(historyfile, type):
    """
    Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file

    Parameters
    ----------
    historyfile
    type

    Returns
    -------

    """
    level = 3
    exit_status = 0
    if type == "PEDESTAL":
        level -= 2
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
                log.exception(f"Malformed history file {historyfile}, {err}", 3)
            except ValueError as err:
                log.exception(f"Malformed history file {historyfile}, {err}", 3)
            else:
                if program == cfg.get("LSTOSA", "R0-DL1"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "R0-DL1").split(",")]
                    level = 2 if exit_status in nonfatalrcs else 3
                elif program == cfg.get("LSTOSA", "DL1-DL2"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "DL1-DL2").split(",")]
                    if (exit_status in nonfatalrcs) and (prod_id == options.dl2_prod_id):
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} already produced")
                        level = 0
                    else:
                        level = 2
                        log.debug(f"DL2 prod ID: {options.dl2_prod_id} not produced yet")
                elif program == "drs4_pedestal":
                    level = 2 if exit_status == 0 else 3
                elif program == "charge_calibration":
                    level = 1 if exit_status == 0 else 2
                elif program == "time_calibration":
                    level = 0 if exit_status == 0 else 1
                else:
                    log.error(f"Programme name not identified {program}")

    return level, exit_status


def preparejobs(sequence_list):
    for s in sequence_list:
        log.debug(f"Creating sequence.txt and sequence.py for sequence {s.seq}")
        createsequencetxt(s, sequence_list)
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
    text_suffix = cfg.get("LSTOSA", "TEXTSUFFIX")
    f = os.path.join(options.directory, f"sequence_{s.jobname}{text_suffix}")
    start = s.subrun_list[0].timestamp
    ped = ""
    cal = ""
    dat = ""
    #    these fields are written on sequenceXX.py
    #    ucts_t0_dragon = s.subrun_list[0].ucts_t0_dragon
    #    dragon_counter0 = s.subrun_list[0].dragon_counter0
    #    ucts_t0_tib = s.subrun_list[0].ucts_t0_tib
    #    tib_counter0 = s.subrun_list[0].tib_counter0
    if s.type == "CALI":
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
    #    these fields are written on sequenceXX.py
    #    content += "ucts_t0_dragon: {0}\n".format(ucts_t0_dragon)
    #    content += "dragon_counter0: {0}\n".format(dragon_counter0)
    #    content += "ucts_t0_tib: {0}\n".format(ucts_t0_tib)
    #    content += "tib_counter0: {0}\n".format(tib_counter0)

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
    # calibfiles cannot be set here, since they require the runfromparent


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
            yy, mm, dd = date_in_yymmdd(nightdir)
            drivefile = f"drive_log_{yy}_{mm}_{dd}{drive_suffix}"
        else:
            run_string = str(s.parent_list[0].run).zfill(5)
            ped_run_string = str(s.parent_list[0].previousrun).zfill(5)
            nightdir = lstdate_to_dir(options.date)
            yy, mm, dd = date_in_yymmdd(nightdir)
            if options.mode == "P":
                calfile = f"calibration.Run{run_string}.0000{calib_suffix}"
                timecalfile = f"time_calibration.Run{run_string}.0000{calib_suffix}"
                pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
                drivefile = f"drive_log_{yy}_{mm}_{dd}{drive_suffix}"
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

    bindir = cfg.get("LSTOSA", "PYTHONDIR")
    scriptsdir = cfg.get("LSTOSA", "SCRIPTSDIR")
    drivedir = cfg.get("LST1", "DRIVEDIR")

    command = None
    if s.type == "CALI":
        command = os.path.join(scriptsdir, "calibrationsequence.py")
    elif s.type == "DATA":
        command = os.path.join(scriptsdir, "datasequence.py")
    elif s.type == "STEREO":
        command = os.path.join(scriptsdir, "stereosequence.py")

    # beware we want to change this in the future
    commandargs = ["srun", "python", command]
    if options.verbose:
        commandargs.append("-v")
    if options.warning:
        commandargs.append("-w")
    if options.configfile:
        commandargs.append("-c")
        commandargs.append(os.path.join(bindir, guesscorrectinputcard(s)))
    if options.compressed:
        commandargs.append("-z")
    # commandargs.append('--stderr=sequence_{0}_'.format(s.jobname) + "{0}.err'" + ".format(str(job_id))")
    # commandargs.append('--stdout=sequence_{0}_'.format(s.jobname) + "{0}.out'" + ".format(str(job_id))")
    commandargs.append("-d")
    commandargs.append(options.date)
    commandargs.append("--prod-id")
    commandargs.append(options.prod_id)

    if s.type == "CALI":
        commandargs.append(s.pedestal)
        commandargs.append(s.calibration)
        ped_run_number = str(s.previousrun).zfill(5)
        cal_run_number = str(s.run).zfill(5)
        commandargs.append(ped_run_number)
        commandargs.append(cal_run_number)

    if s.type == "DATA":
        commandargs.append(os.path.join(options.directory, s.calibration))
        commandargs.append(os.path.join(options.directory, s.pedestal))
        commandargs.append(os.path.join(options.directory, "time_" + s.calibration))
        commandargs.append(os.path.join(drivedir, s.drive))
        # pedfile = s.pedestal
        ucts_t0_dragon = s.subrun_list[0].ucts_t0_dragon
        commandargs.append(ucts_t0_dragon)
        dragon_counter0 = s.subrun_list[0].dragon_counter0
        commandargs.append(dragon_counter0)
        ucts_t0_tib = s.subrun_list[0].ucts_t0_tib
        commandargs.append(ucts_t0_tib)
        tib_counter0 = s.subrun_list[0].tib_counter0
        commandargs.append(tib_counter0)

    # commandargs.append(str(s.run).zfill(5))
    #   if s.type != 'STEREO':
    #  commandargs.append(options.tel_id)
    for sub in s.subrun_list:
        n_subruns = int(sub.subrun)

    content = "#!/bin/env python\n"
    # Set sbatch parameters
    content += "\n"
    content += f"#SBATCH -A {cfg.get('SBATCH', 'ACCOUNT')} \n"
    if s.type == "DATA":
        content += f"#SBATCH --array=0-{int(n_subruns) - 1} \n"
    content += "#SBATCH --cpus-per-task=1 \n"
    if s.type == "CALI":
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
    content += "\n\n"

    content += "subruns=os.getenv('SLURM_ARRAY_TASK_ID')\n"
    content += "job_id=os.getenv('SLURM_JOB_ID')\n"

    content += "proc = subprocess.run([\n"
    for i in commandargs:
        content += f"    '{i}',\n"
    content += (
        f"    '--stderr=log/sequence_{s.jobname}."
        + "{0}_{1}.err'.format(str(subruns).zfill(4), str(job_id)), \n"
    )
    content += (
        f"    '--stdout=log/sequence_{s.jobname}."
        + "{0}_{1}.out'.format(str(subruns).zfill(4), str(job_id)), \n"
    )
    if s.type == "DATA":
        content += (
            "    '{0}".format(str(s.run).zfill(5))
            + ".{0}'"
            + ".format(str(subruns).zfill(4))"
            + ",\n"
        )
    content += f"    '{options.tel_id}'\n"
    content += "    ])\n"

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
        if s.type == "CALI":
            commandargs.append(s.script)
            if options.simulate or options.nocalib:
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
    command = cfg.get("ENV", "SACCTBIN")
    user = cfg.get("ENV", "USER")
    sacct_format = "--format=jobid%8,jobname%25,cputime,elapsed,state,exitcode"
    commandargs = [command, "-u", user, sacct_format]
    queue_list = []
    try:
        sacct_output = subprocess.check_output(commandargs, universal_newlines=True)
    except subprocess.CalledProcessError as Error:
        log.exception(f"Command '{stringify(commandargs)}' failed, {Error}", 2)
    except OSError as ValueError:
        log.exception(f"Command '{stringify(commandargs)}' failed, {ValueError}", ValueError)
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


def previous_and_next(iterable):
    prevs, items, nexts = tee(iterable, 3)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    return zip(prevs, items, nexts)


def setqueuevalues(queue_list, sequence_list):
    for s in sequence_list:
        s.tries = 0
        for previous, queue_item, nxt in previous_and_next(queue_list):
            try:
                if queue_item["JobName"] == "python" and s.jobname == previous["JobName"]:
                    s.action = "Check"
                    s.jobid = queue_item["JobID"]
                    s.state = queue_item["State"]
                    list_of_states = [
                        "COMPLETED",
                        "RUNNING",
                        "PENDING",
                        "FAILED",
                        "CANCELLED+",
                    ]
                    if s.state in list_of_states:
                        if s.tries == 0:
                            s.cputime = queue_item["CPUTime"]
                            s.walltime = queue_item["Elapsed"]
                        else:
                            try:
                                s.cputime = avg_time_duration(s.cputime, queue_item["CPUTime"])
                            except AttributeError as ErrorName:
                                log.warning(ErrorName)
                            try:
                                s.walltime = avg_time_duration(s.cputime, queue_item["Elapsed"])
                            except AttributeError as ErrorName:
                                log.warning(ErrorName)
                        if s.state in ["COMPLETED", "FAILED", "CANCELLED+"]:
                            s.exit = queue_item["ExitCode"]

                        if nxt is not None:
                            if queue_item["JobID"] != nxt["JobID"]:
                                s.tries += 1
                        else:
                            s.tries += 1  # Last item of the queue reached
                    log.debug(
                        f"Queue attributes: sequence {s.seq}, JobName {s.jobname}, "
                        f"JobID {s.jobid}, State {s.state}, CPUTime {s.cputime}, Exit {s.exit} updated",
                    )
            except TypeError as err:
                log.warning(f"Reached the end of queue: {err}")


def sumtime(a, b):
    """Beware of the strange format of timedelta:
    http://docs.python.org/library/datetime.html?highlight=datetime#datetime.timedelta"""

    a_hh, a_mm, a_ss = a.split(":")
    b_hh, b_mm, b_ss = b.split(":")

    # strange error: invalid literal for int() with base 10: '1 day, 0'
    if " day, " in a_hh:
        a_hh = int(a_hh.split(" day, ")[0]) * 24 + int(a_hh.split(" day, ")[1])
    elif " days, " in a_hh:
        a_hh = int(a_hh.split(" days, ")[0]) * 24 + int(a_hh.split(" days, ")[1])

    ta = datetime.timedelta(0, int(a_ss), 0, 0, int(a_mm), int(a_hh), 0)
    tb = datetime.timedelta(0, int(b_ss), 0, 0, int(b_mm), int(b_hh), 0)
    tc = ta + tb
    c = str(tc)
    if len(c) == 7:
        c = "0" + c
    return c


def avg_time_duration(a, b):

    if a is None:
        a = "00:00:00"
    elif b is None:
        b = "00:00:00"

    a_hh, a_mm, a_ss = a.split(":")
    b_hh, b_mm, b_ss = b.split(":")

    a_seconds = int(a_hh) * 3600 + int(a_mm) * 60 + int(a_ss)
    b_seconds = int(b_hh) * 3600 + int(b_mm) * 60 + int(b_ss)

    if a != "00:00:00" and b != "00:00:00":
        return time.strftime("%H:%M:%S", time.gmtime(np.mean((a_seconds, b_seconds))))
    elif a is None and b is not None:
        return b
    elif b is None and a is not None:
        return a
    elif a is None:
        return "00:00:00"
    else:
        return sumtime(a, b)


def date_in_yymmdd(datestring):
    """Convert date string(yyyy_mm_dd) from the NightSummary into
    (yy_mm_dd) format. Depending on the time, +1 is added to date to
    consider the convention of filenaming based on observation date.

    Parameters
    ----------
    datestring: in format yyyy_mm_dd
    """
    # date = datestring.split('-')
    # da = [ch for ch in date[0]]
    date = list(datestring)
    yy = "".join(date[2:4])
    mm = "".join(date[4:6])
    dd = "".join(date[6:8])
    # change the day
    # time = timestring.split(':')
    # if (int(time[0]) >= 17 and int(time[0]) <= 23):
    #   dd = str(int(date[2]))
    # else:
    #   dd   = date[2]
    return yy, mm, dd
