import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from astropy.table import Table

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import DEFAULT_CFG
from osa.scripts.sequencer_webmaker import html_content
from osa.utils.utils import date_to_dir, date_to_iso

log = logging.getLogger(__name__)


def valid_date(string):
    """Check if the string is a valid date and return a datetime object."""
    return datetime.strptime(string, "%Y-%m-%d")


common_parser = ArgumentParser(add_help=False)
common_parser.add_argument(
    "-c",
    "--config",
    type=Path,
    default=DEFAULT_CFG,
    help="Use specific config file [default configs/sequencer.cfg]",
)
common_parser.add_argument(
    "-d",
    "--date",
    help="Date of the start of the night in ISO format (YYYY-MM-DD). Defaults to yesterday",
    type=valid_date,
)


def check_gainsel_jobs_runwise(date: datetime, run_id: int) -> bool:
    """Search for failed jobs in the log directory."""
    base_dir = Path(cfg.get("LST1", "BASE"))
    flat_date = date_to_dir(date)
    log_dir = base_dir / f"R0G/log/{flat_date}"
    history_files = log_dir.glob(f"gain_selection_{run_id:05d}.????.history")

    success_subruns = 0
    failed_subruns = 0
    pending_subruns = 0

    for file in history_files:
        if file.read_text() != "":
            gainsel_rc = file.read_text().splitlines()[-1][-1]

            if gainsel_rc == "1":
                failed_subruns += 1

            elif gainsel_rc == "0":
                success_subruns += 1

        else:
            pending_subruns += 1

    return {"pending": pending_subruns, "success": success_subruns, "failed": failed_subruns}


def check_failed_jobs(date: datetime) -> pd.DataFrame:
    """Search for failed jobs in the log directory."""
    summary_table = run_summary_table(date)
    data_runs = summary_table[summary_table["run_type"] == "DATA"]

    gainsel_status_dict = {}
    for run in data_runs:
        run_id = run["run_id"]
        gainsel_job_status = check_gainsel_jobs_runwise(date, run_id)
        gainsel_status_dict[run_id] = gainsel_job_status

    gainsel_df = pd.DataFrame(gainsel_status_dict.values(), index=gainsel_status_dict.keys())
    gainsel_df.reset_index(inplace=True)
    gainsel_df.rename(columns={"index": "run_id"}, inplace=True)
    summary_table = summary_table.to_pandas()

    final_table = pd.merge(summary_table, gainsel_df, on="run_id")[
        [
            "run_id",
            "n_subruns",
            "pending",
            "success",
            "failed",
        ]
    ]

    def determine_status(row):
        if row["failed"] > 0:
            return "FAILED"
        elif row["pending"] == row["n_subruns"]:
            return "PENDING"
        elif row["success"] == row["n_subruns"]:
            return "COMPLETED"
        elif row["pending"] > 0:
            return "RUNNING"
        else:
            return "NOT STARTED"

    final_table["GainSel%"] = round(final_table["success"] * 100 / final_table["n_subruns"])
    final_table["GainSelStatus"] = final_table.apply(determine_status, axis=1)

    return final_table


def main():
    """Produce the html file with the processing OSA Gain Selection status.

    It creates an HTML file osa_gainsel_status_YYYY-MM-DD.html
    """
    args = ArgumentParser(
        description=(
            "Script to create an HTML file with the gain selection status "
            "(osa_gainsel_status_YYYY-MM-DD.html)"
        ),
        parents=[common_parser],
    ).parse_args()

    if args.date:
        flat_date = date_to_dir(args.date)
        options.date = args.date

    else:
        # yesterday by default
        yesterday = datetime.now() - timedelta(days=1)
        options.date = yesterday
        flat_date = date_to_dir(yesterday)

    date = date_to_iso(options.date)
    run_summary_directory = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    run_summary_file = run_summary_directory / f"RunSummary_{flat_date}.ecsv"

    gain_selection_web_directory = Path(cfg.get("LST1", "GAIN_SELECTION_WEB_DIR"))
    gain_selection_web_directory.mkdir(parents=True, exist_ok=True)
    html_file = gain_selection_web_directory / f"osa_gainsel_status_{date}.html"

    # Create and save the HTML file
    if not run_summary_file.is_file() or len(Table.read(run_summary_file)["run_id"]) == 0:
        content = "<p>No data found</p>"
        log.warning(f"No data found for date {date}, creating an empty HTML file.")

    else:
        # Get the table with the gain selection check report in HTML format:
        table_gain_selection_jobs = check_failed_jobs(options.date)
        content = table_gain_selection_jobs.to_html(justify="left")

    html_file.write_text(html_content(content, date, "OSA Gain Selection"))
    log.info(f"Created HTML file {html_file}")


if __name__ == "__main__":
    main()
