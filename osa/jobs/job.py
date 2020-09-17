import datetime
import subprocess
import time
from os.path import exists, join

import numpy as np

from osa.configs.config import cfg
from osa.utils import options
from osa.utils.iofile import readfromfile, writetofile
from osa.utils.standardhandle import error, gettag, stringify, verbose, warning
from osa.utils.utils import lstdate_to_dir


def arealljobscorrectlyfinished(seqlist):
    tag = gettag()
    flag = True
    for s in seqlist:
        out, rc = historylevel(s.history, s.type)
        if out == 0:
            verbose(tag, f"Job {s.seq} correctly finished")
            continue
        else:
            verbose(tag, f"Job {s.seq} not correctly/completely finished [{out}]")
            flag = False
    return flag


def historylevel(historyfile, type):
    """Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file
    """
    tag = gettag()
    level = 3
    exit_status = 0
    if type == "PEDESTAL":
        level -= 2
    if type == "CALIBRATION":
        level -= 1
    if exists(historyfile):
        for line in readfromfile(historyfile).splitlines():
            words = line.split()
            try:
                program = words[1]
                exit_status = int(words[10])
            except IndexError as err:
                error(tag, f"Malformed history file {historyfile}, {err}", 3)
            except ValueError as err:
                error(tag, f"Malformed history file {historyfile}, {err}", 3)
            else:
                if program == cfg.get("LSTOSA", "R0-DL1"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "R0-DL1").split(",")]
                    if exit_status in nonfatalrcs:
                        level = 2
                    else:
                        level = 3
                elif program == cfg.get("LSTOSA", "DL1-DL2"):
                    nonfatalrcs = [int(k) for k in cfg.get("NONFATALRCS", "DL1-DL2").split(",")]
                    if exit_status in nonfatalrcs:
                        level = 0
                    else:
                        level = 2
                elif program == "calibration":
                    if exit_status == 0:
                        level = 0
                    else:
                        level = 1
                elif program == "drs4_pedestal":
                    if exit_status == 0:
                        level = 1
                    else:
                        level = 0

                else:
                    error(tag, f"Programme name not identified {program}", 6)

    return level, exit_status


def preparejobs(sequence_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, f"Creating sequence.txt and sequence.py for sequence {s.seq}")
        createsequencetxt(s, sequence_list)
        createjobtemplate(s)


def preparestereojobs(sequence_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, f"Creating sequence.py for sequence {s.seq}")
        createjobtemplate(s)


def preparedailyjobs(sequence_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, f"Creating sequence.sh for source {s.name}")
        createjobtemplate(s)


def setrunfromparent(sequence_list):
    tag = gettag()
    # this is a dictionary, seq -> parent's run number
    dictionary = {}
    for s1 in sequence_list:
        if s1.parent is not None:
            for s2 in sequence_list:
                if s2.seq == s1.parent:
                    verbose(tag, f"Assigning runfromparent({s1.parent}) = {s2.run}")
                    dictionary[s1.parent] = s2.run
                    break
    return dictionary


def createsequencetxt(s, sequence_list):
    tag = gettag()
    text_suffix = cfg.get("LSTOSA", "TEXTSUFFIX")
    f = join(options.directory, f"sequence_{s.jobname}{text_suffix}")
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
    content += f"Start: {start}\n"  # TODO: get the right date and time
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
        verbose(tag, f"SIMULATE Creating sequence txt {f}")


def formatrunsubrun(run, subrun):
    # it needs 7 digits for the runs
    # it needs 3 digits (insert leading 0s needed)
    if not run:
        run = 7 * "0"
    s = str(subrun).zfill(3)
    string = f"{run}.{s}"
    return string


def setsequencefilenames(s):
    script_suffix = cfg.get("LSTOSA", "SCRIPTSUFFIX")
    history_suffix = cfg.get("LSTOSA", "HISTORYSUFFIX")
    veto_suffix = cfg.get("LSTOSA", "VETOSUFFIX")
    basename = f"sequence_{s.jobname}"

    s.script = join(options.directory, basename + script_suffix)
    s.veto = join(options.directory, basename + veto_suffix)
    s.history = join(options.directory, basename + history_suffix)
    # calibfiles cannot be set here, since they require the runfromparent


def setsequencecalibfilenames(sequence_list):
    tag = gettag()
    calib_suffix = cfg.get("LSTOSA", "CALIBSUFFIX")
    pedestal_suffix = cfg.get("LSTOSA", "PEDESTALSUFFIX")
    drive_suffix = cfg.get("LSTOSA", "DRIVESUFFIX")
    for s in sequence_list:
        if len(s.parent_list) == 0:
            cal_run_string = str(s.run).zfill(4)
            calfile = f"calibration.Run{cal_run_string}.0000{calib_suffix}"
            timecalfile = f"time_calibration.Run{cal_run_string}.0000{calib_suffix}"
            ped_run_string = str(s.previousrun).zfill(4)
            pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
            nightdir = lstdate_to_dir(options.date)
            yy, mm, dd = date_in_yymmdd(nightdir)
            drivefile = f"drive_log_{yy}_{mm}_{dd}{drive_suffix}"
        else:
            run_string = str(s.parent_list[0].run).zfill(4)
            ped_run_string = str(s.parent_list[0].previousrun).zfill(4)
            nightdir = lstdate_to_dir(options.date)
            yy, mm, dd = date_in_yymmdd(nightdir)
            if options.mode == "P":
                calfile = f"calibration.Run{run_string}.0000{calib_suffix}"
                timecalfile = f"time_calibration.Run{run_string}.0000{calib_suffix}"
                pedfile = f"drs4_pedestal.Run{ped_run_string}.0000{pedestal_suffix}"
                drivefile = f"drive_log_{yy}_{mm}_{dd}{drive_suffix}"
            elif options.mode == "S" or options.mode == "T":
                error(tag, "Exiting, mode not implemented yet. Try with 'P' mode instead", 6)
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
    calibdir = cfg.get("LST1", "CALIBDIR")
    pedestaldir = cfg.get("LST1", "PEDESTALDIR")
    drivedir = cfg.get("LST1", "DRIVEDIR")
    nightdir = lstdate_to_dir(options.date)
    version = cfg.get("LST1", "VERSION")

    command = None
    if s.type == "CALI":
        command = join(bindir, "calibrationsequence.py")
    elif s.type == "DATA":
        command = join(bindir, "datasequence.py")
    elif s.type == "STEREO":
        command = join(bindir, "stereosequence.py")

    # directly use python interpreter from current working environment
    # python = join(config.cfg.get('ENV', 'PYTHONBIN'), 'python')
    srunbin = cfg.get("ENV", "SRUNBIN")

    # beware we want to change this in the future
    commandargs = [srunbin, "python", command]
    if options.verbose:
        commandargs.append("-v")
    if options.warning:
        commandargs.append("-w")
    if options.configfile:
        commandargs.append("-c")
        commandargs.append(join(bindir, guesscorrectinputcard(s)))
    if options.compressed:
        commandargs.append("-z")
    # commandargs.append('--stderr=sequence_{0}_'.format(s.jobname) + "{0}.err'" + ".format(str(job_id))")
    # commandargs.append('--stdout=sequence_{0}_'.format(s.jobname) + "{0}.out'" + ".format(str(job_id))")
    commandargs.append("-d")
    commandargs.append(options.date)
    commandargs.append("--prod_id")
    commandargs.append(options.prod_id)

    if s.type == "CALI":
        commandargs.append(join(pedestaldir, nightdir, version, s.pedestal))
        commandargs.append(join(calibdir, nightdir, version, s.calibration))
        ped_run = str(s.previousrun).zfill(5)
        commandargs.append(ped_run)

    if s.type == "DATA":
        commandargs.append(join(calibdir, nightdir, version, s.calibration))
        commandargs.append(join(pedestaldir, nightdir, version, s.pedestal))
        commandargs.append(join(calibdir, nightdir, version, "time_" + s.calibration))
        commandargs.append(join(drivedir, s.drive))
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
    # SLURM assignments
    content += "\n"
    content += "#SBATCH -A dpps \n"
    content += "#SBATCH -p short \n"
    if s.type == "DATA":
        content += f"#SBATCH --array=0-{int(n_subruns) - 1} \n"
    content += "#SBATCH --cpus-per-task=1 \n"
    content += "#SBATCH --mem-per-cpu=15G \n"
    content += f"#SBATCH -D {options.directory} \n"
    content += f"#SBATCH -o log/slurm.{str(s.run).zfill(5)}_%a_%A.out \n"
    content += f"#SBATCH -e log/slurm.{str(s.run).zfill(5)}_%a_%A.err \n"
    content += "\n"

    content += "import subprocess \n"
    content += "import os \n"
    content += "\n\n"

    content += "subruns=os.getenv('SLURM_ARRAY_TASK_ID')\n"
    content += "job_id=os.getenv('SLURM_JOB_ID')\n"

    content += "subprocess.call([\n"
    for i in commandargs:
        content += f"    '{i}',\n"
    content += f"    '--stderr=log/sequence_{s.jobname}" + "_{0}_{1}.err'" + '.format(str(subruns).zfill(4), str(str(job_id)))' + ',\n'
    content += f"    '--stdout=log/sequence_{s.jobname}" + "_{0}_{1}.out'" + '.format(str(subruns).zfill(4), str(str(job_id)))' + ',\n'
    if s.type == "DATA":
        content += "    '{0}".format(str(s.run).zfill(5)) + ".{0}'" + '.format(str(subruns).zfill(4))' + ',\n'
    else:
        content += f"    '{str(s.run).zfill(5)}',\n"
    content += f"    '{options.tel_id}'\n"
    content += "    ])"

    if not options.simulate:
        writetofile(s.script, content)

    if get_content:
        return content


# def submitjobs(sequence_list, queue_list, veto_list):
def submitjobs(sequence_list):
    tag = gettag()
    job_list = []
    command = cfg.get("ENV", "SBATCHBIN")
    for s in sequence_list:
        commandargs = [command, s.script]
        #        """ Introduce the job dependencies """
        #        if len(s.parent_list) != 0:
        #            commandargs.append('-W')
        #            depend_string = 'depend='
        #            if s.type == 'DATA':
        #                depend_string += 'afterok'
        #            elif s.type == 'STEREO':
        #                depend_string += 'afterany'
        #            for pseq in s.parent_list:
        #                if pseq.jobid > 0:
        #                    depend_string += ":{0}".format(pseq.jobid)
        #            commandargs.append(depend_string)
        #        """ Skip vetoed """
        #        if s.action == 'Veto':
        #            verbose(tag, "job {0} has been vetoed".format(s.jobname))
        #        elif s.action == 'Closed':
        #            verbose(tag, "job {0} is already closed".format(s.jobname))
        #        elif s.action == 'Check' and s.state != 'C':
        #            verbose(tag, "job {0} checked to be dispatched but not completed yet".format(s.jobname))
        #            if s.state == 'H' or s.state == 'R':
        #                # Reset values
        #                s.exit = None
        #                if s.state == 'H':
        #                    s.jobhost = None
        #                    s.cputime = None
        #                    s.walltime = None
        #        elif s.action == 'Check' and s.state == 'C' and s.exit == 0:
        #            verbose(tag, "job {0} checked to be successful".format(s.jobname))
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
        #                error(tag, Error, 2)
        #            except OSError (ValueError, NameError):
        #                error(tag, "Command {0}, {1}".format(stringify(commandargs), NameError), ValueError)
        #            else:
        #                if options.simulate == False:
        #                    try:
        #                        s.jobid = int(stdout.split('.', 1)[0])
        #                    except ValueError as e:
        #                        warning(tag, "Wrong parsing of jobid {0} not being an integer, {1}".format(stdout.split('.', 1)[0], e))
        #        job_list.append(s.jobid)
        #        verbose(tag, "{0} {1}".format(s.action, stringify(commandargs)))
        if not options.simulate:
            try:
                verbose(tag, f"Launching script {s.script}")
                subprocess.check_output(commandargs)
            except subprocess.CalledProcessError as Error:
                error(tag, Error, 2)
            except OSError as err:
                error(tag, f"Command '{command}' not found", err)
        else:
            verbose(tag, "SIMULATE Launching scripts")

        verbose(tag, commandargs)
        job_list.append(s.script)

    return job_list


def getqueuejoblist(sequence_list):
    tag = gettag()
    command = cfg.get("ENV", "SACCTBIN")
    user = cfg.get("ENV", "USER")
    sacct_format = "--format=jobid%8,jobname%25,cputime,elapsed,state,exitcode"
    commandargs = [command, "-u", user, sacct_format]
    queue_list = []
    try:
        sacct_output = subprocess.check_output(commandargs, universal_newlines=True)
    except subprocess.CalledProcessError as Error:
        error(tag, f"Command '{stringify(commandargs)}' failed, {Error}", 2)
    except OSError as ValueError:
        error(tag, f"Command '{stringify(commandargs)}' failed, {ValueError}", ValueError)
    else:
        queue_header = sacct_output.splitlines()[0].split()
        queue_lines = sacct_output.replace("+", "").replace("sequence_", "").replace(".py", "").splitlines()[2:]
        queue_sequences = [line.split() for line in queue_lines]
        queue_list = [dict(zip(queue_header, sequence)) for sequence in queue_sequences]
        setqueuevalues(queue_list, sequence_list)

    return queue_list


def setqueuevalues(queue_list, sequence_list):
    tag = gettag()
    for s in sequence_list:
        s.tries = 0
        for q in queue_list:
            if s.jobname == q["JobName"]:
                s.action = "Check"
                s.jobid = q["JobID"]
                s.state = q["State"]
                if s.state == "COMPLETED" or s.state == "RUNNING":
                    if s.tries == 0:
                        s.cputime = q["CPUTime"]
                        s.walltime = q["Elapsed"]
                    else:
                        try:
                            s.cputime = avg_time_duration(s.cputime, q["CPUTime"])
                        except AttributeError as ErrorName:
                            warning(tag, ErrorName)
                        try:
                            s.walltime = avg_time_duration(s.cputime, q["Elapsed"])
                        except AttributeError as ErrorName:
                            warning(tag, ErrorName)
                    if s.state == "COMPLETED":
                        s.exit = q["ExitCode"]
                s.tries += 1
                verbose(
                    tag,
                    f"Queue attributes: sequence {s.seq}, JobName {s.jobname}, "
                    f"JobID {s.jobid}, State {s.state}, CPUTime {s.cputime}, Exit {s.exit} updated",
                )


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

    a_hh, a_mm, a_ss = a.split(":")
    b_hh, b_mm, b_ss = b.split(":")

    a_seconds = a_hh * 3600 + a_mm * 60 + a_ss
    b_seconds = b_hh * 3600 + b_mm * 60 + b_ss

    if a != "00:00:00" and b != "00:00:00":
        time_duration = time.strftime('%H:%M:%S', time.gmtime(np.mean((a_seconds,b_seconds))))
    else:
        time_duration = sumtime(a, b)

    return time_duration




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
