#!/usr/bin/env python

import os
from decimal import Decimal
from glob import glob
from os.path import join

from osa.autocloser.closer import is_day_closed
# from dev.dot import writeworkflow
from osa.configs.config import cfg
from osa.jobs.job import getqueuejoblist, preparedailyjobs, preparejobs, preparestereojobs, submitjobs
from osa.nightsummary.extract import extractruns, extractsequences, extractsequencesstereo, extractsubruns
from osa.nightsummary.nightsummary import readnightsummary
from osa.reports.report import rule, start
from osa.configs import options
from osa.utils.cliopts import sequencercliparsing, set_default_directory_if_needed
from osa.utils.standardhandle import gettag, output, verbose
from osa.veto.veto import getvetolist, getclosedlist

__all__ = ["sequencer", "single_process"]


def sequencer():
    """Runs the sequencer
    This is the main script to be called in crontab by LSTOSA
    For every run in the NightSummary.txt file it preparares 
    a SLURM job array which sends a datasequence.py 
    for every subrun in the run
    """

    process_mode = None
    single_array = ["LST1", "LST2"]
    start(tag)
    if options.tel_id in single_array:
        process_mode = "single"
        single_process(options.tel_id, process_mode)
    else:
        if options.tel_id == "all":
            process_mode = "complete"
        elif options.tel_id == "ST":
            process_mode = "stereo"
        sequence_lst1 = single_process("LST1", process_mode)
        sequence_lst2 = single_process("LST2", process_mode)
        sequence_st = stereo_process("ST", sequence_lst1, sequence_lst2)


def single_process(telescope, process_mode):
    """Runs the single process for a single telescope
    
    Parameters
    ----------
    telescope : str
        Options: 'LST1', 'LST2' or 'ST'
    process_mode : str
        Options: 'single', 'stereo' or 'complete'

    Returns
    -------
    sequence_list : 
    """

    # define global variables and create night directory
    sequence_list = []
    options.tel_id = telescope
    options.directory = set_default_directory_if_needed()
    options.log_directory = os.path.join(options.directory, "log")

    if not options.simulate:
        os.makedirs(options.log_directory, exist_ok=True)

    simulate_save = options.simulate
    is_report_needed = True

    if process_mode == "single":
        if is_day_closed():
            output(tag, f"Day {options.date} for {options.tel_id} already closed")
            return sequence_list
    else:
        if process_mode == "stereo":
            # only simulation for single array required
            options.nightsum = True
            options.simulate = True
            is_report_needed = False

    # building the sequences
    night = readnightsummary()
    subrun_list = extractsubruns(night)
    run_list = extractruns(subrun_list)
    # modifies run_list by adding the seq and parent info into runs
    sequence_list = extractsequences(run_list)

    # FIXME: Does this makes sense or should be removed?
    # workflow and submission
    # if not options.simulate:
    #     writeworkflow(sequence_list)

    # adds the scripts
    preparejobs(sequence_list)

    #if test in order to be able to run it locally
    if not options.test:
        queue_list = getqueuejoblist(sequence_list)
    veto_list = getvetolist(sequence_list)
    closed_list = getclosedlist(sequence_list)
    updatelstchainstatus(sequence_list)
    # updatesequencedb(sequence_list)
    # actually, submitjobs does not need the queue_list nor veto_list
    # job_list = submitjobs(sequence_list, queue_list, veto_list)

    job_list = submitjobs(sequence_list)

    # report
    if is_report_needed:
        # insert_if_new_activity_db(sequence_list)
        # updatesequencedb(sequence_list)
        # rule()
        reportsequences(sequence_list)

    # cleaning
    # options.directory = None
    # options.simulate = simulate_save
    return sequence_list


def stereo_process(telescope, s1_list, s2_list):

    options.tel_id = telescope
    options.directory = set_default_directory_if_needed()

    # building the sequences
    sequence_list = extractsequencesstereo(s1_list, s2_list)
    # workflow and Submission
    # writeworkflow(sequence_list)
    # adds the scripts
    preparestereojobs(sequence_list)
    # preparedailyjobs(dailysrc_list)
    queue_list = getqueuejoblist(sequence_list)
    veto_list = getvetolist(sequence_list)
    closed_list = getclosedlist(sequence_list)
    updatelstchainstatus(sequence_list)
    # actually, submitjobs does not need the queue_list nor veto_list
    job_list = submitjobs(sequence_list)
    # finalizing report
    rule()
    reportsequences(sequence_list)
    # cleaning
    options.directory = None
    return sequence_list


def updatelstchainstatus(seq_list):

    for s in seq_list:
        if s.type == "CALI":
            s.calibstatus = int(Decimal(getlstchainforsequence(s, "CALIB") * 100) / s.subruns)
        elif s.type == "DATA":
            s.dl1status = int(Decimal(getlstchainforsequence(s, "DL1") * 100) / s.subruns)
            s.datacheckstatus = int(Decimal(getlstchainforsequence(s, "DATACHECK") * 100) / s.subruns)
            s.muonstatus = int(Decimal(getlstchainforsequence(s, "MUON") * 100) / s.subruns)
            s.dl2status = int(Decimal(getlstchainforsequence(s, "DL2") * 100) / s.subruns)


def getlstchainforsequence(s, program):

    prefix = cfg.get("LSTOSA", program + "PREFIX")
    suffix = cfg.get("LSTOSA", program + "SUFFIX")
    files = glob(join(options.directory, f"{prefix}*{s.run}*{suffix}"))
    numberoffiles = len(files)
    verbose(tag, f"Found {numberoffiles} {program} files for sequence name {s.jobname}")
    return numberoffiles


def reportsequences(seqlist):

    matrix = []
    header = [
        "Tel",
        "Seq",
        "Parent",
        "Type",
        "Run",
        "Subruns",
        "Source",
        "Wobble",
        "Action",
        "Tries",
        "JobID",
        "State",
        "Host",
        "CPU_time",
        "Walltime",
        "Exit",
    ]
    if options.tel_id == "LST1" or options.tel_id == "LST2":
        header.append("DL1%")
        header.append("DATACHECK%")
        header.append("MUONS%")
        header.append("DL2%")

    matrix.append(header)
    for s in seqlist:
        row_list = [
            s.telescope,
            s.seq,
            s.parent,
            s.type,
            s.run,
            s.subruns,
            s.source,
            s.wobble,
            s.action,
            s.tries,
            s.jobid,
            s.state,
            s.jobhost,
            s.cputime,
            s.walltime,
            s.exit,
        ]
        if s.type == "CALI":
            # repeat None for every data level
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
        elif s.type == "DATA":
            row_list.append(s.dl1status)
            row_list.append(s.datacheckstatus)
            row_list.append(s.muonstatus)
            row_list.append(s.dl2status)
        matrix.append(row_list)
    padding = int(cfg.get("OUTPUT", "PADDING"))
    prettyoutputmatrix(matrix, padding)


# def insert_if_new_activity_db(sequence_list):
#     tag = gettag()
#     from osa.configs import config
#     from datetime import datetime
#     from mysql import insert_db, select_db
#
#     server = cfg.get('MYSQL', 'SERVER')
#     user = cfg.get('MYSQL', 'USER')
#     database = cfg.get('MYSQL', 'DATABASE')
#     table = cfg.get('MYSQL', 'SUMMARYTABLE')
#
#     if len(sequence_list) != 0:
#         """ Declare the beginning of OSA activity """
#         start = datetime.now()
#         selections = ['ID']
#         conditions = {
#             'NIGHT': options.date,
#             'TELESCOPE': options.tel_id,
#             'ACTIVITY': 'LSTOSA'
#         }
#         matrix = select_db(server, user, database, table, selections, conditions)
#         id = None
#         if matrix:
#             id = matrix[0][0]
#         if id and int(id) > 0:
#             """ Activity already started """
#         else:
#             """ Insert it into the database """
#             assignments = conditions
#             assignments['IS_FINISHED'] = 0
#             assignments['START'] = start
#             assignments['END'] = None
#             conditions = {}
#             insert_db(server, user, database, table, assignments, conditions)
#
#
# def updatesequencedb(seqlist):
#     tag = gettag()
#     from osa.configs import config
#     from mysql import update_db, insert_db, select_db
#
#     server = cfg.get('MYSQL', 'SERVER')
#     user = cfg.get('MYSQL', 'USER')
#     database = cfg.get('MYSQL', 'DATABASE')
#     table = cfg.get('MYSQL', 'SEQUENCETABLE')
#     for s in seqlist:
#         """ Fine tuning """
#         hostname = None
#         id_processor = None
#         if s.jobhost is not None:
#             hostname, id_processor = s.jobhost.split('/')
#         """ Select ID if exists """
#         selections = ['ID']
#         conditions = {
#             'TELESCOPE': s.telescope,
#             'NIGHT': s.night,
#             'ID_NIGHTLY': s.seq
#         }
#         matrix = select_db(server, user, database, table, selections, conditions)
#         id = None
#         if matrix:
#             id = matrix[0][0]
#         verbose(tag, f"To this sequence corresponds an entry in the {table} with ID {id}")
#         assignments = {
#             'TELESCOPE': s.telescope,
#             'NIGHT': s.night,
#             'ID_NIGHTLY': s.seq,
#             'TYPE': s.type,
#             'RUN': s.run,
#             'SUBRUNS': s.subruns,
#             'SOURCEWOBBLE': s.sourcewobble,
#             'ACTION': s.action,
#             'TRIES': s.tries,
#             'JOBID': s.jobid,
#             'STATE': s.state,
#             'HOSTNAME': hostname,
#             'ID_PROCESSOR': id_processor,
#             'CPU_TIME': s.cputime,
#             'WALL_TIME': s.walltime,
#             'EXIT_STATUS': s.exit,
#         }
#
#         if s.type == 'CALI':
#             assignments.update({'PROGRESS_SCALIB': s.scalibstatus})
#         elif s.type == 'DATA':
#             # FIXME: translate to LST related stuff
#             assignments.update({
#                 'PROGRESS_SORCERER': s.sorcererstatus,
#                 'PROGRESS_SSIGNAL': s.ssignalstatus,
#                 'PROGRESS_MERPP': s.merppstatus,
#                 'PROGRESS_STAR': s.starstatus,
#                 'PROGRESS_STARHISTOGRAM': s.starhistogramstatus,
#             })
#         elif s.type == 'STEREO':
#             assignments.update({
#                 'PROGRESS_SUPERSTAR': s.superstarstatus,
#                 'PROGRESS_SUPERSTARHISTOGRAM': s.superstarhistogramstatus,
#                 'PROGRESS_MELIBEA': s.melibeastatus,
#                 'PROGRESS_MELIBEAHISTOGRAM': s.melibeahistogramstatus,
#             })
#
#         if s.parent is not None:
#             assignments['ID_NIGHTLY_PARENTS'] = f'{s.parent},'
#         if not id:
#             conditions = {}
#             insert_db(server, user, database, table, assignments, conditions)
#         else:
#             conditions = {'ID': id}
#             update_db(server, user, database, table, assignments, conditions)


def prettyoutputmatrix(m, paddingspace):

    maxfieldlength = []
    for i in range(len(m)):
        row = m[i]
        for j in range(len(row)):
            col = row[j]
            # verbose(tag, "Row {0}, Col {1}, Val {2} Len {3}".format(i, j, col, l))
            if m.index(row) == 0:
                maxfieldlength.append(len(str(col)))
            elif len(str(col)) > maxfieldlength[j]:
                # Insert or update the first length
                maxfieldlength[j] = len(str(col))
    for row in m:
        stringrow = ""
        for j in range(len(row)):
            col = row[j]
            lpadding = (maxfieldlength[j] - len(str(col))) * " "
            rpadding = paddingspace * " "
            if isinstance(col, int):
                # we got an integer, right aligned
                stringrow += f"{lpadding}{col}{rpadding}"
            else:
                # should be a string, left aligned
                stringrow += f"{col}{lpadding}{rpadding}"
        output(tag, stringrow)


if __name__ == "__main__":
    """Sequencer called as a script does the full job."""

    tag = gettag()
    # set the options through parsing of the command line interface
    sequencercliparsing()
    # run the routine
    sequencer()
