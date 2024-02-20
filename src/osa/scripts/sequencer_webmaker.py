"""Produce the HTML file with the processing status from the sequencer report."""


import logging
import subprocess as sp
import sys
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import Iterable

import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import sequencer_webmaker_argparser
from osa.utils.logging import myLogger
from osa.utils.utils import is_day_closed, date_to_iso, date_to_dir

log = myLogger(logging.getLogger())


def html_content(body: str, date: str) -> str:
    """Build the HTML content.

    Parameters
    ----------
    body : str
        Table with the sequencer status report.
    date : str
        Date of the processing YYYY-MM-DD.

    Returns
    -------
    str
        HTML content.
    """
    time_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return dedent(
        f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
        <html xmlns="http://www.w3.org/1999/xhtml">
         <head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
          <title>OSA Sequencer status</title><link href="osa.css" rel="stylesheet"
          type="text/css" /><style>table{{width:152ex;}}</style>
         </head>
         <body>
         <h1>OSA processing status</h1>
         <p>Processing data from: {date}. Last updated: {time_update} UTC</p>
         {body}
         </body>
        </html>"""
    )


def get_sequencer_output(date: str, config: str, test=False) -> list:
    """Call sequencer to get table with the sequencer status report.

    Parameters
    ----------
    date : str
        Date of the processing YYYY-MM-DD.
    config : str
        OSA configuration file to use.
    test : bool

    Returns
    -------
    list
        Lines of the sequencer output.
    """
    log.info("Calling sequencer...")
    commandargs = [
        "sequencer",
        "-c",
        config,
        "-s",
        "-d",
        date,
        options.tel_id,
    ]

    if test:
        commandargs.insert(-1, "-t")

    try:
        output = sp.run(commandargs, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8", check=True)
    except sp.CalledProcessError as error:
        log.error(f"Command {commandargs} failed, {error.returncode}")
        sys.exit(1)
    else:
        # Strip newlines and fit it into a table:
        return output.stdout.splitlines()


def lines_to_matrix(lines: Iterable) -> list:
    """Build the matrix from the sequencer output lines."""
    matrix = []
    for line in lines:
        l_fields = line.split()
        if len(l_fields) == 18:
            matrix.append(l_fields)
    return matrix


def matrix_to_html(matrix: list) -> str:
    """Build the html table with the sequencer status report."""
    log.info("Building the html table from sequencer output")
    if len(matrix) < 2:
        return "<p>No data found</p>"
    df = pd.DataFrame(matrix[1:], columns=matrix[0])
    return df.to_html(index=False)


def main():
    """Produce the html file with the processing status from the sequencer report."""

    log.setLevel(logging.INFO)

    args = sequencer_webmaker_argparser().parse_args()

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

    # Get the table with the sequencer status report:
    lines = get_sequencer_output(date, args.config, args.test)

    # Build the html sequencer table that will be place in the body of the HTML file
    matrix = lines_to_matrix(lines)
    html_table = matrix_to_html(matrix)

    # Save the HTML file
    log.info("Saving the HTML file")
    directory = Path(cfg.get("LST1", "SEQUENCER_WEB_DIR"))
    directory.mkdir(parents=True, exist_ok=True)

    html_file = directory / Path(f"osa_status_{flat_date}.html")
    html_file.write_text(html_content(html_table, date), encoding="utf-8")

    log.info("Done")


if __name__ == "__main__":
    main()
