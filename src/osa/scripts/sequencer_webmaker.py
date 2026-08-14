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
from osa.utils.cliopts import sequencer_webmaker_argparser
from osa.utils.logging import myLogger
from osa.utils.utils import is_day_closed, date_to_iso, date_to_dir
from osa.paths import all_dl1ab_config_files_exist, analysis_path, catB_closed_file_exists

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
    matrix = []
    warnings = []
    for line in lines:
        l_fields = line.split()
        if len(l_fields) == 19:
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


# --- NEW: update global per-run history based on per-subrun history and .closed ---
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


def update_global_history():
    """
    For each DATA run on options.date:
     - If all per-subrun history files contain lstchain_data_r0_to_dl1 exit 0 -> write R0_ARRAY line
     - If all per-subrun history files contain lstchain_check_dl1 exit 0 -> write DL1AB_ARRAY line
    
    Note: CATB_CLOSED line is written by the SLURM job, not by this script.
    """
    log.info("Updating global run histories from per-subrun histories")

    # ensure options.directory is set (and options.prod_id)
    options.directory = analysis_path(options.tel_id)

    run_table = run_summary_table(options.date)
    if len(run_table) == 0:
        log.debug("No runs in summary table")
        return

    for row in run_table:
        if row["run_type"] != "DATA":
            continue
        run_id = int(row["run_id"])

        # paths
        global_history = Path(options.directory) / f"{options.tel_id}_{run_id:05d}.history"

        # collect per-subrun history files for this run
        subrun_hist_files = sorted(Path(options.directory).glob(f"sequence_{options.tel_id}_{run_id:05d}.*.history"))
        
        # 1) R0_ARRAY: require every subrun file exists and contains lstchain_data_r0_to_dl1 with exit 0
        r0_ok = True
        if not subrun_hist_files:
            r0_ok = False
        else:
            for hf in subrun_hist_files:
                try:
                    lines = hf.read_text().splitlines()
                except Exception:
                    r0_ok = False
                    break
                found = any("lstchain_data_r0_to_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    r0_ok = False
                    break
        if r0_ok and not _history_has_program(global_history, "R0_ARRAY"):
            # append summary line
            try:
                version = get_major_version(get_lstchain_version()) if get_lstchain_version() else "unknown"
            except Exception:
                version = "unknown"
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            line = f"{run_id:05d} R0_ARRAY {version} {ts} None None 0\n"
            if not options.simulate:
                global_history.parent.mkdir(parents=True, exist_ok=True)
                with open(global_history, "a") as fh:
                    fh.write(line)
                log.info(f"Wrote R0_ARRAY summary for run {run_id} in {global_history.name}")
            else:
                log.info(f"[SIMULATE] Would write R0_ARRAY -> {global_history}: {line.strip()}")

        # 2) DL1AB_ARRAY (check_dl1): every subrun history file contains lstchain_check_dl1 exit 0
        dl1ab_ok = True
        if not subrun_hist_files:
            dl1ab_ok = False
        else:
            for hf in subrun_hist_files:
                try:
                    lines = hf.read_text().splitlines()
                except Exception:
                    dl1ab_ok = False
                    break
                found = any("lstchain_check_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    dl1ab_ok = False
                    break
        if dl1ab_ok and not _history_has_program(global_history, "DL1AB_ARRAY"):
            try:
                version = get_major_version(get_lstchain_version()) if get_lstchain_version() else "unknown"
            except Exception:
                version = "unknown"
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            line = f"{run_id:05d} DL1AB_ARRAY {version} {ts} None None 0\n"
            if not options.simulate:
                global_history.parent.mkdir(parents=True, exist_ok=True)
                with open(global_history, "a") as fh:
                    fh.write(line)
                log.info(f"Wrote DL1AB_ARRAY summary for run {run_id} in {global_history.name}")
            else:
                log.info(f"[SIMULATE] Would write DL1AB_ARRAY -> {global_history}: {line.strip()}")

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

    # NEW: update global history entries before asking sequencer for the table
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
