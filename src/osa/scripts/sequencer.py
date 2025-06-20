#!/usr/bin/env python

"""
Orchestrator script that creates and execute the calibration sequence and
prepares a SLURM job array which launches the data sequences for every subrun.
"""

import warnings
import logging
import os
import sys
from decimal import Decimal
import datetime

from osa import osadb
from osa.configs import options
from osa.configs.config import cfg
from osa.veto import get_closed_list, get_veto_list
from osa.utils.logging import myLogger

warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API.*",
    category=UserWarning
)
from osa.job import ( # noqa: E402
    set_queue_values,
    prepare_jobs,
    submit_jobs,
    get_sacct_output,
    get_squeue_output,
    run_sacct,
    run_squeue,
    are_all_jobs_correctly_finished,
)
from osa.nightsummary.extract import ( # noqa: E402
    build_sequences,
    extract_runs,
    extract_sequences
)
from osa.nightsummary.nightsummary import run_summary_table # noqa: E402
from osa.paths import analysis_path, destination_dir # noqa: E402
from osa.report import start # noqa: E402
from osa.utils.cliopts import sequencer_cli_parsing # noqa: E402
from osa.utils.utils import is_day_closed, gettag, date_to_iso # noqa: E402
from osa.scripts.gain_selection import GainSel_finished # noqa: E402

__all__ = [
    "single_process",
    "update_sequence_status",
    "get_status_for_sequence",
    "output_matrix",
    "report_sequences",
    "update_job_info",
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

    database = cfg.get("database", "path")
    if database:
        osadb.start_processing(date_to_iso(options.date))

    # Define global variables and create night directory
    sequence_list = []
    options.tel_id = telescope
    options.directory = analysis_path(options.tel_id)
    options.log_directory = options.directory / "log"

    if not options.simulate:
        os.makedirs(options.log_directory, exist_ok=True)

    summary_table = run_summary_table(options.date)
    if len(summary_table) == 0:
        log.warning("No runs found for this date. Nothing to do. Exiting.")
        sys.exit(0)

    if not options.no_gainsel and not GainSel_finished(options.date):
        log.info(
            f"Gain selection did not finish successfully for date {date_to_iso(options.date)}. "
            "Try again later, once gain selection has finished."
        )
        sys.exit()

    if is_day_closed():
        log.info(f"Date {date_to_iso(options.date)} is already closed for {options.tel_id}")
        return sequence_list

    if not options.test and not options.simulate:
        if is_sequencer_running(options.date):
            log.info(f"Sequencer is still running for date {date_to_iso(options.date)}. Try again later.")
            sys.exit(0)

        if is_sequencer_completed(options.date) and not options.force_submit:
            log.info(f"Sequencer already finished for date {date_to_iso(options.date)}. Exiting")
            sys.exit(0)

        elif timeout_in_sequencer(options.date) and not options.force_submit:
            log.info(f"Some jobs of sequencer finished in TIMEOUT for date {date_to_iso(options.date)}."
                " Please relaunch the affected sequences manually.")
            sys.exit(0)

    # Build the sequences
    sequence_list = build_sequences(options.date)

    # Create job pilot scripts
    prepare_jobs(sequence_list)

    # Update sequences objects with information from SLURM
    update_job_info(sequence_list)

    get_veto_list(sequence_list)
    get_closed_list(sequence_list)
    update_sequence_status(sequence_list)

    if not options.no_submit:
        submit_jobs(sequence_list)

    # TODO: insert_new_activity_db(sequence_list)

    # Display the sequencer table with processing status
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
        sequence_list=sequence_list,
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
            seq.dl1status = int(Decimal(get_status_for_sequence(seq, "DL1") * 100) / seq.subruns)
            seq.dl1abstatus = int(
                Decimal(get_status_for_sequence(seq, "DL1AB") * 100) / seq.subruns
            )
            seq.datacheckstatus = int(
                Decimal(get_status_for_sequence(seq, "DATACHECK") * 100) / seq.subruns
            )
            seq.muonstatus = int(Decimal(get_status_for_sequence(seq, "MUON") * 100) / seq.subruns)
            seq.dl2status = int(Decimal(get_status_for_sequence(seq, "DL2") * 100))


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
        try:
            directory = options.directory / sequence.dl1_prod_id
            files = list(directory.glob(f"dl1_LST-1*{sequence.run}*.h5"))
        except AttributeError:
            return 0
        
    elif data_level == "DL2":
        try:
            directory = destination_dir(concept="DL2", create_dir=False, dl2_prod_id=sequence.dl2_prod_id)
            files = list(directory.glob(f"dl2_LST-1*{sequence.run}*.h5"))
        except AttributeError:
            return 0
        
    elif data_level == "DATACHECK":
        try:
            directory = options.directory / sequence.dl1_prod_id
            files = list(directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))
        except AttributeError:
            return 0
        
    else:
        prefix = cfg.get("PATTERN", f"{data_level}PREFIX")
        suffix = cfg.get("PATTERN", f"{data_level}SUFFIX")
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
        header.extend(("DL1%", "MUONS%", "DL1AB%", "DATACHECK%", "DL2%"))
    matrix = [header]
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
            row_list.extend((None, None, None, None, None))
        elif sequence.type == "DATA":
            row_list.extend(
                (
                    sequence.dl1status,
                    sequence.muonstatus,
                    sequence.dl1abstatus,
                    sequence.datacheckstatus,
                    sequence.dl2status,
                )
            )

        matrix.append(row_list)
    padding = int(cfg.get("OUTPUT", "PADDING"))
    output_matrix(matrix, padding)


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
        rpadding = padding_space * " "
        for j, col in enumerate(row):
            lpadding = (max_field_length[j] - len(str(col))) * " "
            stringrow += (
                f"{lpadding}{col}{rpadding}"
                if isinstance(col, int)
                else f"{col}{lpadding}{rpadding}"
            )

        log.info(stringrow)


def is_sequencer_running(date: datetime.datetime) -> bool:
    """Check if the jobs launched by sequencer are running or pending for the given date."""
    summary_table = run_summary_table(date)
    sacct_output = run_sacct()
    sacct_info = get_sacct_output(sacct_output)

    for run in summary_table["run_id"]:
        jobs_run = sacct_info[sacct_info["JobName"]==f"LST1_{run:05d}"]
        queued_jobs = jobs_run[(jobs_run["State"] == "RUNNING") | (jobs_run["State"] == "PENDING")]
        if len(queued_jobs) != 0:
            return True

    return False


def is_sequencer_completed(date: datetime.datetime) -> bool:
    """Check if the jobs launched by sequencer are already completed."""
    summary_table = run_summary_table(date)
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    run_list = extract_runs(data_runs)
    sequence_list = extract_sequences(options.date, run_list)

    if are_all_jobs_correctly_finished(sequence_list):
        return True
    else:
        log.info("Jobs did not correctly/yet finish")
        return False

def timeout_in_sequencer(date: datetime.datetime) -> bool:
    """Check if any of the jobs launched by sequencer finished in timeout."""
    summary_table = run_summary_table(date)
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    sacct_output = run_sacct()
    sacct_info = get_sacct_output(sacct_output)

    for run in data_runs["run_id"]:
        jobs_run = sacct_info[sacct_info["JobName"]==f"LST1_{run:05d}"]
        if len(jobs_run["JobID"].unique())>1:
            last_job_id = sorted(jobs_run["JobID"].unique())[-1]
            jobs_run = sacct_info[sacct_info["JobID"]==last_job_id]
        timeout_jobs = jobs_run[(jobs_run["State"] == "TIMEOUT")]
        if len(timeout_jobs) != 0:
            return True

    return False


if __name__ == "__main__":
    main()
