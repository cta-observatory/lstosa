#!/usr/bin/env python3
"""
Sequencer: orchestrates r0->dl1 arrays, per-run CatB/tailcuts pilots and dl1ab arrays.

Behavior:
 - For each DATA run (see osa.job.submit_jobs):
   * submit r0->dl1 array job (if not completed/active)
   * submit CatB/tailcuts pilot dependent on r0 (if needed)
   * submit dl1ab array dependent on the pilot (if needed) or on r0 (if no pilot)
 - Keeps per-subrun history entries as before (scripts append per-subrun lines).
 - Produces a textual sequencer table snapshot (sequencer_table.txt) in options.directory
   and a timestamped copy in options.log_directory.
 - Honors --simulate, --test and --force-submit.

All the SLURM logic (scripts, submission, job state queries) lives in osa.job.
"""
import datetime
import logging
import os
import warnings
from pathlib import Path

from osa import job as job_module
from osa.configs import options
from osa.configs.config import cfg
from osa.job import (
    CAT_A_DATACHECK_DIR,
    array_job_status,
    catb_jobname,
    dl1ab_jobname,
    get_last_job_state,
    get_sacct_output,
    prepare_jobs,
    r0_jobname,
    run_sacct,
    submit_jobs,
    update_job_info,
)
from osa.nightsummary.extract import build_sequences
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import analysis_path, destination_dir, get_dl1_prod_id_and_config
from osa.utils.cliopts import sequencer_cli_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_iso
from osa.veto import get_closed_list, get_veto_list

warnings.filterwarnings("ignore", message="pkg_resources is deprecated as an API.*", category=UserWarning)

log = myLogger(logging.getLogger(__name__))


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
def format_sequence_table(sequence_list) -> str:
    """
    Build the same table as report_sequences but return it as a formatted string.
    (Used to save a textual snapshot of the sequencer output.)
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
        header.extend(("DL1%", "DC-A%", "MUONS%", "CAT-B", "DL1AB%", "DATACHECK%", "DL2%"))
    matrix = [header]
    for sequence in sequence_list:
        row_list = [
            getattr(sequence, "telescope", None),
            getattr(sequence, "seq", None),
            getattr(sequence, "parent", None),
            getattr(sequence, "type", None),
            getattr(sequence, "run", None),
            getattr(sequence, "subruns", None),
            getattr(sequence, "source_name", None),
            getattr(sequence, "action", None),
            getattr(sequence, "tries", None),
            getattr(sequence, "jobid", None),
            getattr(sequence, "state", None),
            getattr(sequence, "cputime", None),
            getattr(sequence, "exit", None),
        ]
        if getattr(sequence, "type", None) in ["DRS4", "PEDCALIB"]:
            row_list.extend((None, None, None, None, None, None, None))
        elif getattr(sequence, "type", None) == "DATA":
            row_list.extend(
                (
                    getattr(sequence, "dl1status", None),
                    getattr(sequence, "datacheck_a_status", None),
                    getattr(sequence, "muonstatus", None),
                    getattr(sequence, "catbstatus", None),
                    getattr(sequence, "dl1abstatus", None),
                    getattr(sequence, "datacheckstatus", None),
                    getattr(sequence, "dl2status", None),
                )
            )
        matrix.append(row_list)

    # build padded string; convert None->"" for display
    padding = int(cfg.get("OUTPUT", "PADDING"))
    max_field_length = []
    for row in matrix:
        for j, col in enumerate(row):
            length = len("" if col is None else str(col))
            if len(max_field_length) <= j:
                max_field_length.append(length)
            elif length > max_field_length[j]:
                max_field_length[j] = length

    out_lines = []
    rpadding = padding * " "
    for row in matrix:
        stringrow = ""
        for j, col in enumerate(row):
            col_str = "" if col is None else str(col)
            lpad = (max_field_length[j] - len(col_str)) * " "
            # right-align integers
            if isinstance(col, int):
                stringrow += f"{lpad}{col}{rpadding}"
            else:
                stringrow += f"{col_str}{lpad}{rpadding}"
        out_lines.append(stringrow)
    return "\n".join(out_lines) + "\n"


# ---------------------------------------------------------------------------
# Status of the products on disk
# ---------------------------------------------------------------------------
def get_status_for_sequence(sequence, data_level) -> int:
    """
    Get number of files produced for a given sequence and data level.

    Parameters
    ----------
    sequence
    data_level : str
        Options: 'CALIB', 'DL1', 'DATACHECK_A' (cat A datacheck of the DL1a files),
        'DL1AB', 'DATACHECK', 'MUON' or 'DL2'

    Returns
    -------
    number_of_files : int
    """
    try:
        if data_level == "DL1AB":
            directory = options.directory / sequence.dl1_prod_id
            files = list(directory.glob(f"dl1_LST-1*{sequence.run}*.0*.h5"))

        elif data_level == "DATACHECK_A":
            directory = options.directory / CAT_A_DATACHECK_DIR
            files = list(directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.0*.h5"))

        elif data_level == "DL2":
            directory = destination_dir(
                concept="DL2",
                create_dir=False,
                dl2_prod_id=sequence.dl2_prod_id,
            )
            files = list(directory.glob(f"dl2_LST-1*{sequence.run}*.h5"))

        elif data_level == "DATACHECK":
            # try both options.directory/<dl1_prod_id> and DATACHECK destination_dir
            files = []

            try:
                directory = options.directory / sequence.dl1_prod_id
                files += list(
                    directory.glob(
                        f"datacheck_dl1_LST-1*{sequence.run}*.0*.h5"
                    )
                )
            except Exception:
                log.debug(
                    f"get_status_for_sequence: could not list DATACHECK files "
                    f"in options.directory for run "
                    f"{getattr(sequence, 'run', None)}",
                    exc_info=True,
                )

            try:
                alternative_directory = destination_dir(
                    concept="DATACHECK",
                    create_dir=False,
                    dl1_prod_id=sequence.dl1_prod_id,
                )
                files += list(
                    alternative_directory.glob(
                        f"datacheck_dl1_LST-1*{sequence.run}*.0*.h5"
                    )
                )
            except Exception:
                log.debug(
                    f"get_status_for_sequence: could not list DATACHECK files "
                    f"in the destination_dir for run "
                    f"{getattr(sequence, 'run', None)}",
                    exc_info=True,
                )

        else:
            prefix = cfg.get("PATTERN", f"{data_level}PREFIX")
            suffix = cfg.get("PATTERN", f"{data_level}SUFFIX")
            files = list(
                options.directory.glob(
                    f"{prefix}*{sequence.run}*{suffix}"
                )
            )

    except AttributeError:
        return 0

    except Exception:
        log.debug(
            f"get_status_for_sequence: unexpected error for run "
            f"{getattr(sequence, 'run', None)} and level {data_level}",
            exc_info=True,
        )
        return 0

    return len(files)


def check_catB_status(seq) -> str:
    """
    Determine the catB status of a DATA sequence:
      - "CLOSED" if a catB*<run>*.closed file exists in options.directory
      - otherwise the sacct state of the latest CatB/tailcuts pilot job of the run
      - "None" if there is neither
    """
    if seq.type != "DATA":
        return "None"

    if list(options.directory.glob(f"catB*{seq.run}*.closed")):
        return "CLOSED"

    return get_last_job_state(catb_jobname(seq.run)) or "None"


def _percentage(n_files: int, n_subruns: int) -> int:
    return 100 * n_files // (n_subruns or 1)


def _ensure_dl1_prod_id(seq) -> None:
    """DL1AB/DATACHECK products are searched in a directory named after the dl1 prod id."""
    if getattr(seq, "dl1_prod_id", None):
        return
    try:
        seq.dl1_prod_id, seq.dl1b_config = get_dl1_prod_id_and_config(seq.run)
    except Exception:
        log.debug(f"Could not determine dl1 prod id for run {seq.run}", exc_info=True)


def update_sequence_status(seq_list):
    """
    Update the percentage of files produced of each type (calibration, DL1,
    cat A datacheck, DL1AB, DATACHECK, MUON and DL2) for every run considering the total number of subruns.

    Parameters
    ----------
    seq_list
        List of sequences of a given night corresponding to each run.
    """
    for seq in seq_list:
        try:
            if seq.type == "PEDCALIB":
                seq.calibstatus = _percentage(get_status_for_sequence(seq, "CALIB"), seq.subruns)
            elif seq.type == "DATA":
                _ensure_dl1_prod_id(seq)
                seq.dl1status = _percentage(get_status_for_sequence(seq, "DL1"), seq.subruns)
                seq.datacheck_a_status = _percentage(get_status_for_sequence(seq, "DATACHECK_A"), seq.subruns)
                seq.dl1abstatus = _percentage(get_status_for_sequence(seq, "DL1AB"), seq.subruns)
                seq.datacheckstatus = _percentage(get_status_for_sequence(seq, "DATACHECK"), seq.subruns)
                seq.muonstatus = _percentage(get_status_for_sequence(seq, "MUON"), seq.subruns)
                # For DL2 keep old behaviour: count files and multiply by 100 (no division by subruns)
                seq.dl2status = 100 * get_status_for_sequence(seq, "DL2")
                seq.catbstatus = check_catB_status(seq)
        except Exception:
            log.exception(f"Could not update status for sequence run {getattr(seq, 'run', None)}")


# ---------------------------------------------------------------------------
# Array summaries
# ---------------------------------------------------------------------------
def _write_run_summary_line(run_dir: Path, tel: str, run: int, kind: str, status: int):
    """Append a summary line for the overall status of an array job."""
    try:
        summary_file = run_dir / f"{kind.lower()}_{tel}_{run:05d}.status"
        with summary_file.open("a") as fh:
            fh.write(f"{status}\n")
    except Exception:
        log.debug(f"Could not write run summary line for {kind} {tel} {run}")


def write_array_summaries(sequence_list):
    """Write the overall status of the r0->dl1 and dl1ab arrays of each DATA run (when finished)."""
    try:
        sacct_info = get_sacct_output(run_sacct())
    except Exception:
        log.debug("write_array_summaries: sacct not available", exc_info=True)
        return

    tel = options.tel_id
    for seq in sequence_list:
        if seq.type != "DATA":
            continue

        for kind, jobname in (
            ("R0_ARRAY", r0_jobname(seq.run)),
            ("DL1AB_ARRAY", dl1ab_jobname(seq.run)),
        ):
            status = array_job_status(sacct_info, jobname)
            if status is not None:
                _write_run_summary_line(options.directory, tel, seq.run, kind, status)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def save_sequence_table(sequence_list) -> None:
    """Print the sequencer table and, unless simulating, save it to disk."""
    try:
        # Ensure statuses reflect disk products right before printing/saving
        update_sequence_status(sequence_list)
        table_str = format_sequence_table(sequence_list)

        # ALWAYS print table to stdout so it's visible even with --simulate
        print(table_str)

        if options.simulate:
            log.info("[SIMULATE] Would write sequencer table to disk")
            return

        table_file = options.directory / "sequencer_table.txt"
        table_file.write_text(table_str)

        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        logfile = options.log_directory / f"sequencer_table_{stamp}.log"
        logfile.write_text(table_str)
        log.info(f"Saved sequencer table to {table_file} and {logfile}")
    except Exception:
        log.exception("Could not write sequencer table to disk")


def single_process(telescope: str):
    sequencer_cli_parsing()  # ensure options set
    options.tel_id = telescope
    options.directory = analysis_path(options.tel_id)
    options.log_directory = options.directory / "log"

    # ensure base directories exist so script files can be written
    options.directory.mkdir(parents=True, exist_ok=True)
    if not options.simulate:
        options.log_directory.mkdir(parents=True, exist_ok=True)

    log.debug(
        f"options.directory = {options.directory} "
        f"(exists={options.directory.exists()}, writable={os.access(str(options.directory), os.W_OK)})"
    )
    log.info(
        f"Starting sequencer for {options.tel_id} on date {date_to_iso(options.date)} "
        f"(input_state={options.input_state})"
    )

    summary_table = run_summary_table(options.date)
    if len(summary_table) == 0:
        log.warning("No runs found for this date. Nothing to do.")
        return []

    sequence_list = build_sequences(options.date)
    get_veto_list(sequence_list)
    get_closed_list(sequence_list)

    data_runs = [f"{seq.run} ({seq.subruns} subruns)" for seq in sequence_list if seq.type == "DATA"]
    log.info(f"Found {len(data_runs)} DATA run(s): {', '.join(data_runs) if data_runs else 'none'}")

    # SLURM info and products on disk (DL1, MUON, DATACHECK, DL2, Cat-B)
    update_job_info(sequence_list)
    update_sequence_status(sequence_list)
    write_array_summaries(sequence_list)

    # Scripts + submission (all the logic lives in osa.job)
    prepare_jobs(sequence_list)
    submit_jobs(sequence_list)

    save_sequence_table(sequence_list)
    return sequence_list


def main():
    sequencer_cli_parsing()  # parse CLI into options
    level = logging.DEBUG if options.verbose else logging.INFO
    log.setLevel(level)
    job_module.log.setLevel(level)  # otherwise osa.job messages (submissions, skips) are hidden

    log.info(
        f"=================================== Starting sequencer.py at "
        f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC for LST, "
        f"Telescope: {options.tel_id}, Date: {date_to_iso(options.date)} "
        f"==================================="
    )

    if options.tel_id in ["LST1", "LST2"]:
        single_process(options.tel_id)
    else:
        log.error("Process mode not supported yet")


if __name__ == "__main__":
    main()
