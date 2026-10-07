#!/usr/bin/env python3
"""Produce the HTML file with the processing status from the sequencer report and
update per-run global history entries based on per-subrun histories and .closed files.
"""

import logging
import subprocess as sp
import sys
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import Iterable, List

import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.job import r0_job_completed, run_fully_processed
from osa.nightsummary.nightsummary import run_summary_table
from osa.utils.cliopts import sequencer_webmaker_argparser
from osa.utils.logging import myLogger
from osa.utils.utils import is_day_closed, date_to_iso, date_to_dir, get_lstchain_version
from osa.paths import get_major_version, all_dl1ab_config_files_exist, analysis_path

log = myLogger(logging.getLogger())


def html_content(body: str, warnings: str, date: str, title: str) -> str:
    time_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return dedent(
        f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
        <html xmlns="http://www.w3.org/1999/xhtml">
         <head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
          <title>{title} status</title><link href="osa.css" rel="stylesheet"
          type="text/css" /><style>table{{width:152ex;}}</style>
         </head>
         <body>
         <h1>{title} processing status</h1>
         <p>Processing data from: {date}. Last updated: {time_update} UTC.</p>
         {warnings}
         {body}
         </body>
        </html>"""
    )


def get_sequencer_output(
    date: str,
    config: str,
    input_state: str,
    test=False,
    no_gainsel=False,
) -> List[str]:
    log.info("Calling sequencer...")

    commandargs = [
        "sequencer",
        "-c",
        config,
        "-s",
        "-d",
        date,
        "--input-state",
        input_state,
        options.tel_id,
    ]

    if no_gainsel:
        commandargs.insert(-1, "--no-gainsel")

    if test:
        commandargs.insert(-1, "-t")

    if not all_dl1ab_config_files_exist(date):
        commandargs.insert(-1, "--no-dl1ab")

    try:
        log.info(f"Using input_state={input_state}")
        log.info(f"{commandargs}")

        output = sp.run(
            commandargs,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
            encoding="utf-8",
            check=True,
        )

    except sp.CalledProcessError as error:
        log.error(f"Command {commandargs} failed, {error.returncode}")
        sys.exit(1)

    else:
        return output.stdout.splitlines()


def lines_to_matrix(lines: Iterable) -> tuple[list, list]:
    """
    Extract the sequencer table (header + data rows) from the sequencer's stdout.

    The header row is the one starting with "Tel Seq" (see
    `format_sequence_table` in sequencer.py). Its number of fields is used to
    recognize the following data rows, instead of a hard-coded column count,
    so this does not silently break whenever a column is added to or removed
    from the sequencer table.
    """
    matrix = []
    warnings = []
    n_fields = None
    for line in lines:
        l_fields = line.split()
        if n_fields is None and l_fields[:2] == ["Tel", "Seq"]:
            n_fields = len(l_fields)
        if n_fields is not None and len(l_fields) == n_fields:
            matrix.append(l_fields)
        elif "No source information found in the database" in line:
            warnings.append(line)
    return matrix, warnings


def matrix_to_html(matrix: list) -> str:
    log.info("Building the html table from sequencer output")
    if len(matrix) < 2:
        return "<p>No data found</p>"
    df = pd.DataFrame(matrix[1:], columns=matrix[0])
    return df.to_html(index=False)


def warnings_to_html(warnings: list) -> str:
    if not warnings:
        return ""
    items = "".join(f"<li>{w}</li>" for w in warnings)
    return f'<div><h2>Warnings</h2><ul>{items}</ul></div>'


# --- update global per-run history based on per-subrun history ---
def _history_has_program(history_path: Path, program: str) -> bool:
    if not history_path.exists():
        return False
    try:
        for line in history_path.read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == program:
                return True
    except Exception:
        return False
    return False


def _append_global_history_line(global_history: Path, run_id: int, tag: str, version: str) -> None:
    """Append a `tag` completion line to the run's global history file (once)."""
    if _history_has_program(global_history, tag):
        return

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    line = f"{run_id:05d} {tag} {version} {ts} None None 0\n"

    if options.simulate:
        log.info(f"[SIMULATE] Would write {tag} -> {global_history}: {line.strip()}")
        return

    global_history.parent.mkdir(parents=True, exist_ok=True)
    with open(global_history, "a") as fh:
        fh.write(line)
    log.info(f"Wrote {tag} summary for run {run_id} in {global_history.name}")


def update_global_history():
    """
    For each DATA run on options.date, append a summary line to the run's
    global history file once each stage is complete for every subrun:
      - R0_ARRAY: r0->dl1 (+ Cat-A datacheck) done -> osa.job.r0_job_completed
      - DL1AB_ARRAY: dl1ab (+ DL1b datacheck) done -> osa.job.run_fully_processed

    These use the same, order-aware criteria as the sequencer itself, so a
    Cat-A datacheck (which also runs `lstchain_check_dl1`, right after
    r0->dl1) is not mistaken for the DL1b one.

    Note: the CATB_CLOSED line is written by the SLURM job, not by this script.
    """
    log.info("Updating global run histories from per-subrun histories")

    # ensure options.directory is set (and options.prod_id)
    options.directory = analysis_path(options.tel_id)

    run_table = run_summary_table(options.date)
    if len(run_table) == 0:
        log.debug("No runs in summary table")
        return

    try:
        version = get_major_version(get_lstchain_version())
    except Exception:
        version = "unknown"

    for row in run_table:
        if row["run_type"] != "DATA":
            continue
        run_id = int(row["run_id"])

        global_history = Path(options.directory) / f"{options.tel_id}_{run_id:05d}.history"

        if r0_job_completed(run_id):
            _append_global_history_line(global_history, run_id, "R0_ARRAY", version)

        if run_fully_processed(run_id):
            _append_global_history_line(global_history, run_id, "DL1AB_ARRAY", version)

# --- end of update_global_history ------------------------------------------------


def main():
    """Produce the html file with the processing status from the sequencer report."""

    log.setLevel(logging.INFO)

    args = sequencer_webmaker_argparser().parse_args()

    # set tel_id if provided by the parser (it usually is)
    if hasattr(args, "tel_id") and args.tel_id:
        options.tel_id = args.tel_id

    if args.date:
        flat_date = date_to_dir(args.date)
        options.date = args.date
    else:
        # yesterday by default
        yesterday = datetime.now() - timedelta(days=1)
        options.date = yesterday
        flat_date = date_to_dir(yesterday)

    date = date_to_iso(options.date)

    if is_day_closed():
        log.info(f"Date {date} is already closed for {options.tel_id}")
        sys.exit(1)

    run_summary_directory = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    run_summary_file = run_summary_directory / f"RunSummary_{flat_date}.ecsv"

    if not run_summary_file.is_file():
        log.error(f"No RunSummary file found for {date}")
        sys.exit(1)

    log.info(f"Using input_state={args.input_state}")

    # Update global history entries before asking sequencer for the table
    try:
        update_global_history()
    except Exception:
        log.exception("update_global_history failed but continuing to build HTML")

    # Get the table with the sequencer status report:
    lines = get_sequencer_output(
        date,
        args.config,
        args.input_state,
        test=args.test,
        no_gainsel=args.no_gainsel,
    )

    # Build the html sequencer table that will be placed in the body
    matrix, warnings = lines_to_matrix(lines)

    html_table = matrix_to_html(matrix)
    html_warnings = warnings_to_html(warnings)

    # Save the HTML file
    directory = Path(cfg.get("LST1", "SEQUENCER_WEB_DIR"))
    directory.mkdir(parents=True, exist_ok=True)

    html_file = directory / f"osa_status_{flat_date}.html"

    html_file.write_text(
        html_content(
            html_table,
            html_warnings,
            date,
            "LST OSA Sequencer",
        ),
        encoding="utf-8",
    )

    log.info("Done")


if __name__ == "__main__":
    main()
