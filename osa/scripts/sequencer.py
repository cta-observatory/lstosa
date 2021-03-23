#!/usr/bin/env python

"""
Main LSTOSA script. It creates and execute the calibration sequence and
prepares a SLURM job array which launches the data sequences for every subrun.
"""

import logging
import os
from decimal import Decimal
from glob import glob
from os.path import join

from osa.configs import options
from osa.configs.config import cfg
from osa.jobs.job import getqueuejoblist, preparejobs, preparestereojobs, submitjobs
from osa.nightsummary.extract import (
    extractruns,
    extractsequences,
    extractsequencesstereo,
    extractsubruns,
)
from osa.nightsummary.nightsummary import run_summary_table
from osa.reports.report import rule, start
from osa.utils.cliopts import sequencercliparsing, set_default_directory_if_needed
from osa.utils.logging import MyFormatter
from osa.utils.standardhandle import gettag
from osa.utils.utils import is_day_closed
from osa.veto.veto import getvetolist, getclosedlist

__all__ = [
    'single_process',
    'stereo_process',
    'update_sequence_status',
    'get_status_for_sequence',
    'prettyoutputmatrix',
    'reportsequences'
]

log = logging.getLogger(__name__)

# Logging
fmt = MyFormatter()
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logging.root.addHandler(handler)


def main():
    """
    Main script to be called as cron job. It creates and execute
    the calibration sequence and afterward it prepares a SLURM job
    array which launches the data sequences for every subrun.
    """
    # Set the options through parsing of the command line interface
    sequencercliparsing()

    if options.verbose:
        logging.root.setLevel(logging.DEBUG)
    else:
        logging.root.setLevel(logging.INFO)

    process_mode = None
    single_array = ["LST1", "LST2"]
    tag = gettag()
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
        # stereo_process is missing right now


def single_process(telescope, process_mode):
    """
    Runs the single process for a single telescope

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

    # Define global variables and create night directory
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
            log.info(f"Day {options.date} for {options.tel_id} already closed")
            return sequence_list
    else:
        if process_mode == "stereo":
            # only simulation for single array required
            options.nightsummary = True
            options.simulate = True
            is_report_needed = False
    
    # building the sequences
    summary_table = run_summary_table(options.date)
    subrun_list = extractsubruns(summary_table)
    run_list = extractruns(subrun_list)
    # modifies run_list by adding the seq and parent info into runs
    sequence_list = extractsequences(run_list)

    # FIXME: Does this makes sense or should be removed?
    # workflow and submission
    # if not options.simulate:
    #     writeworkflow(sequence_list)

    # adds the scripts
    preparejobs(sequence_list)

    # if test in order to be able to run it locally
    if not options.test:
        queue_list = getqueuejoblist(sequence_list)
    veto_list = getvetolist(sequence_list)
    closed_list = getclosedlist(sequence_list)
    update_sequence_status(sequence_list)
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
    """
    Runs the stereo process for two or more telescopes
    Currently not implemented.

    Parameters
    ----------
    telescope
    s1_list
    s2_list

    Returns
    -------

    """
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
    update_sequence_status(sequence_list)
    # actually, submitjobs does not need the queue_list nor veto_list
    job_list = submitjobs(sequence_list)
    # finalizing report
    rule()
    reportsequences(sequence_list)
    # cleaning
    options.directory = None
    return sequence_list


def update_sequence_status(seq_list):
    """
    Update the percentage of files produced of each type (calibration, DL1,
    DATACHECK, MUON and DL2) for every run considering the total number of subruns.

    Parameters
    ----------
    seq_list
        List of sequences of a given night corresponding to each run.
    """
    for seq in seq_list:
        if seq.type == "PEDCALIB":
            seq.calibstatus = int(
                Decimal(get_status_for_sequence(seq, "CALIB") * 100) / seq.subruns
            )
        elif seq.type == "DATA":
            seq.dl1status = int(Decimal(get_status_for_sequence(seq, "DL1") * 100) / seq.subruns)
            seq.dl1abstatus = int(Decimal(get_status_for_sequence(seq, "DL1AB") * 100) / seq.subruns)
            seq.datacheckstatus = int(
                Decimal(get_status_for_sequence(seq, "DATACHECK") * 100) / seq.subruns
            )
            seq.muonstatus = int(Decimal(get_status_for_sequence(seq, "MUON") * 100) / seq.subruns)
            seq.dl2status = int(Decimal(get_status_for_sequence(seq, "DL2") * 100) / seq.subruns)


def get_status_for_sequence(sequence, program):
    """
    Get number of files produced for a given sequence and data level.

    Parameters
    ----------
    sequence
    program : str
        Options: 'CALIB', 'DL1', 'DL1AB', 'DATACHECK', 'MUON' or 'DL2'

    Returns
    -------
    number_of_files : int

    """
    if program == "DL1AB":
        # Search for files in the dl1ab subdirectory
        prefix = cfg.get("LSTOSA", "DL1PREFIX")
        suffix = cfg.get("LSTOSA", "DL1SUFFIX")
        dl1ab_subdirectory = os.path.join(options.directory, "dl1ab" + "_" + options.dl1_prod_id)
        files = glob(join(dl1ab_subdirectory, f"{prefix}*{sequence.run}*{suffix}"))

    elif program == "DATACHECK":
        # Search for files in the dl1ab subdirectory
        prefix = cfg.get("LSTOSA", program + "PREFIX")
        suffix = cfg.get("LSTOSA", program + "SUFFIX")
        datacheck_subdirectory = os.path.join(options.directory, "datacheck" + "_" + options.dl1_prod_id)
        files = glob(join(datacheck_subdirectory, f"{prefix}*{sequence.run}*{suffix}"))

    else:
        prefix = cfg.get("LSTOSA", program + "PREFIX")
        suffix = cfg.get("LSTOSA", program + "SUFFIX")
        files = glob(join(options.directory, f"{prefix}*{sequence.run}*{suffix}"))
    number_of_files = len(files)
    log.debug(f"Found {number_of_files} {program} files for sequence name {sequence.jobname}")
    return number_of_files


def reportsequences(seqlist):
    """
    Update the status report table shown by the sequencer.

    Parameters
    ----------
    seqlist: List of sequence objects
        List of sequences of a given date
    """
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
    if options.tel_id in ["LST1", "LST2"]:
        header.append("DL1%")
        header.append("MUONS%")
        header.append("DL1AB%")
        header.append("DATACHECK%")
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
        if s.type in ["DRS4", "PEDCALIB"]:
            # repeat None for every data level
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
        elif s.type == "DATA":
            row_list.append(s.dl1status)
            row_list.append(s.muonstatus)
            row_list.append(s.dl1abstatus)
            row_list.append(s.datacheckstatus)
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
#         log.debug(f"To this sequence corresponds an entry in the {table} with ID {id}")
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
    """
    Build the status table shown by the sequencer.

    Parameters
    ----------
    m
    paddingspace
    """
    maxfieldlength = []
    for i in range(len(m)):
        row = m[i]
        for j in range(len(row)):
            col = row[j]
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
        log.info(stringrow)


if __name__ == "__main__":
    main()
