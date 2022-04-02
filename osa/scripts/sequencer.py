#!/usr/bin/env python

"""
Orchestrator script that creates and execute the calibration sequence and
prepares a SLURM job array which launches the data sequences for every subrun.
"""

import logging
import os
from decimal import Decimal

from osa.configs import options
from osa.configs.config import cfg
from osa.job import (
    set_queue_values,
    prepare_jobs,
    submit_jobs,
    get_sacct_output,
    get_squeue_output,
    run_sacct,
    run_squeue,
)
from osa.nightsummary.extract import build_sequences
from osa.paths import analysis_path
from osa.report import start
from osa.utils.cliopts import sequencer_cli_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import is_day_closed, gettag, date_to_iso
from osa.veto import get_closed_list, get_veto_list

__all__ = [
    "single_process",
    "update_sequence_status",
    "get_status_for_sequence",
    "output_matrix",
    "report_sequences",
    "update_job_info"
]

log = myLogger(logging.getLogger())


def main():
    """
    Main script to be called as cron job. It creates and execute
    the calibration sequence and afterward it prepares a SLURM job
    array which launches the data sequences for every subrun.
    """
    sequencer_cli_parsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    single_array = ["LST1", "LST2"]
    tag = gettag()
    start(tag)
    if options.tel_id in single_array:
        single_process(options.tel_id)
    else:
        log.error("Process mode not supported yet")


def single_process(telescope):
    """
    Runs the single process for a single telescope

    Parameters
    ----------
    telescope : str
        Options: 'LST1'

    Returns
    -------
    sequence_list : list
    """

    # Define global variables and create night directory
    sequence_list = []
    options.tel_id = telescope
    options.directory = analysis_path(options.tel_id)
    options.log_directory = options.directory / "log"

    if not options.simulate:
        os.makedirs(options.log_directory, exist_ok=True)

    is_report_needed = True

    if is_day_closed():
        log.info(
            f"Date {date_to_iso(options.date)} is already closed for {options.tel_id}"
        )
        return sequence_list

    # Build the sequences
    sequence_list = build_sequences(options.date)

    # Workflow and submission
    # if not options.simulate:
    #     write_workflow(sequence_list)

    # Adds the scripts
    prepare_jobs(sequence_list)

    # Update sequences objects with information from SLURM
    update_job_info(sequence_list)

    get_veto_list(sequence_list)
    get_closed_list(sequence_list)
    update_sequence_status(sequence_list)
    # updatesequencedb(sequence_list)
    if not options.no_submit:
        submit_jobs(sequence_list)

    # report
    if is_report_needed:
        # insert_if_new_activity_db(sequence_list)
        # updatesequencedb(sequence_list)
        report_sequences(sequence_list)

    return sequence_list


def update_job_info(sequence_list):
    """
    Updates the job information from SLURM

    Parameters
    ----------
    sequence_list : list
        List of sequences to be updated
    """
    if options.test:
        return

    sacct_output, squeue_output = run_sacct(), run_squeue()
    set_queue_values(
        sacct_info=get_sacct_output(sacct_output),
        squeue_info=get_squeue_output(squeue_output),
        sequence_list=sequence_list
    )


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
            seq.dl1status = int(
                Decimal(get_status_for_sequence(seq, "DL1") * 100) / seq.subruns
            )
            seq.dl1abstatus = int(
                Decimal(get_status_for_sequence(seq, "DL1AB") * 100) / seq.subruns
            )
            seq.datacheckstatus = int(
                Decimal(get_status_for_sequence(seq, "DATACHECK") * 100) / seq.subruns
            )
            seq.muonstatus = int(
                Decimal(get_status_for_sequence(seq, "MUON") * 100) / seq.subruns
            )
            seq.dl2status = int(
                Decimal(get_status_for_sequence(seq, "DL2") * 100) / seq.subruns
            )


def get_status_for_sequence(sequence, data_level) -> int:
    """
    Get number of files produced for a given sequence and data level.

    Parameters
    ----------
    sequence
    data_level : str
        Options: 'CALIB', 'DL1', 'DL1AB', 'DATACHECK', 'MUON' or 'DL2'

    Returns
    -------
    number_of_files : int
    """
    if data_level == "DL1AB":
        directory = options.directory / options.dl1_prod_id
        files = list(directory.glob(f"dl1_LST-1*{sequence.run}*.h5"))

    elif data_level == "DL2":
        directory = options.directory / options.dl2_prod_id
        files = list(directory.glob(f"dl2_LST-1*{sequence.run}*.h5"))

    elif data_level == "DATACHECK":
        directory = options.directory / options.dl1_prod_id
        files = list(directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))

    else:
        prefix = cfg.get("PATTERN", data_level + "PREFIX")
        suffix = cfg.get("PATTERN", data_level + "SUFFIX")
        files = list(options.directory.glob(f"{prefix}*{sequence.run}*{suffix}"))

    return len(files)


def report_sequences(sequence_list):
    """
    Update the status report table shown by the sequencer.

    Parameters
    ----------
    sequence_list: list
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
        "Action",
        "Tries",
        "JobID",
        "State",
        "CPU_time",
        "Exit",
    ]
    if options.tel_id in ["LST1", "LST2"]:
        header.append("DL1%")
        header.append("MUONS%")
        header.append("DL1AB%")
        header.append("DATACHECK%")
        header.append("DL2%")

    matrix.append(header)
    for sequence in sequence_list:
        row_list = [
            sequence.telescope,
            sequence.seq,
            sequence.parent,
            sequence.type,
            sequence.run,
            sequence.subruns,
            sequence.source_name,
            sequence.action,
            sequence.tries,
            sequence.jobid,
            sequence.state,
            sequence.cputime,
            sequence.exit,
        ]
        if sequence.type in ["DRS4", "PEDCALIB"]:
            # repeat None for every data level
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
        elif sequence.type == "DATA":
            row_list.append(sequence.dl1status)
            row_list.append(sequence.muonstatus)
            row_list.append(sequence.dl1abstatus)
            row_list.append(sequence.datacheckstatus)
            row_list.append(sequence.dl2status)
        matrix.append(row_list)
    padding = int(cfg.get("OUTPUT", "PADDING"))
    output_matrix(matrix, padding)


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
#     for sequence in seqlist:
#         """ Fine tuning """
#         hostname = None
#         id_processor = None
#         if sequence.jobhost is not None:
#             hostname, id_processor = sequence.jobhost.split('/')
#         """ Select ID if exists """
#         selections = ['ID']
#         conditions = {
#             'TELESCOPE': sequence.telescope,
#             'NIGHT': sequence.night,
#             'ID_NIGHTLY': sequence.seq
#         }
#         matrix = select_db(server, user, database, table, selections, conditions)
#         id = None
#         if matrix:
#             id = matrix[0][0]
#         log.debug(f"To this sequence corresponds an entry in the {table} with ID {id}")
#         assignments = {
#             'TELESCOPE': sequence.telescope,
#             'NIGHT': sequence.night,
#             'ID_NIGHTLY': sequence.seq,
#             'TYPE': sequence.type,
#             'RUN': sequence.run,
#             'SUBRUNS': sequence.subruns,
#             'SOURCEWOBBLE': sequence.sourcewobble,
#             'ACTION': sequence.action,
#             'TRIES': sequence.tries,
#             'JOBID': sequence.jobid,
#             'STATE': sequence.state,
#             'HOSTNAME': hostname,
#             'ID_PROCESSOR': id_processor,
#             'CPU_TIME': sequence.cputime,
#             'WALL_TIME': sequence.walltime,
#             'EXIT_STATUS': sequence.exit,
#         }
#
#         if sequence.type == 'CALI':
#             assignmentsequence.update({'PROGRESS_SCALIB': sequence.scalibstatus})
#         elif sequence.type == 'DATA':
#             # FIXME: translate to LST related stuff
#             assignmentsequence.update({
#                 'PROGRESS_SORCERER': sequence.sorcererstatus,
#                 'PROGRESS_SSIGNAL': sequence.ssignalstatus,
#                 'PROGRESS_MERPP': sequence.merppstatus,
#                 'PROGRESS_STAR': sequence.starstatus,
#                 'PROGRESS_STARHISTOGRAM': sequence.starhistogramstatus,
#             })
#         elif sequence.type == 'STEREO':
#             assignmentsequence.update({
#                 'PROGRESS_SUPERSTAR': sequence.superstarstatus,
#                 'PROGRESS_SUPERSTARHISTOGRAM': sequence.superstarhistogramstatus,
#                 'PROGRESS_MELIBEA': sequence.melibeastatus,
#                 'PROGRESS_MELIBEAHISTOGRAM': sequence.melibeahistogramstatus,
#             })
#
#         if sequence.parent is not None:
#             assignments['ID_NIGHTLY_PARENTS'] = f'{sequence.parent},'
#         if not id:
#             conditions = {}
#             insert_db(server, user, database, table, assignments, conditions)
#         else:
#             conditions = {'ID': id}
#             update_db(server, user, database, table, assignments, conditions)


def output_matrix(matrix: list, padding_space: int):
    """
    Build the status table shown by the sequencer.

    Parameters
    ----------
    matrix: list
    padding_space: int
    """
    max_field_length = []
    for row in matrix:
        for j, col in enumerate(row):
            if matrix.index(row) == 0:
                max_field_length.append(len(str(col)))
            elif len(str(col)) > max_field_length[j]:
                # Insert or update the first length
                max_field_length[j] = len(str(col))
    for row in matrix:
        stringrow = ""
        for j, col in enumerate(row):
            lpadding = (max_field_length[j] - len(str(col))) * " "
            rpadding = padding_space * " "
            if isinstance(col, int):
                # we got an integer, right aligned
                stringrow += f"{lpadding}{col}{rpadding}"
            else:
                # should be a string, left aligned
                stringrow += f"{col}{lpadding}{rpadding}"
        log.info(stringrow)


if __name__ == "__main__":
    main()
