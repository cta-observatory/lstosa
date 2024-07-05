from pathlib import Path
from astropy.table import Table
from datetime import datetime, timedelta
from osa.utils.utils import date_to_dir, date_to_iso
from osa.configs.config import cfg
from osa.paths import DEFAULT_CFG
from osa.nightsummary.nightsummary import run_summary_table
import numpy as np
import pandas as pd
from argparse import ArgumentParser
from osa.configs import options
from osa.scripts.sequencer_webmaker import html_content

def valid_date(string):
    """Check if the string is a valid date and return a datetime object."""
    return datetime.strptime(string, "%Y%m%d")


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
    help="Date (YYYYMMDD) of the start of the night",
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
                failed_subruns = failed_subruns+1

            elif gainsel_rc == "0":
                success_subruns = success_subruns+1
        else:
            pending_subruns = pending_subruns+1
    return [pending_subruns, success_subruns, failed_subruns]

def check_failed_jobs(date: datetime):
    """Search for failed jobs in the log directory."""
    summary_table = run_summary_table(date)
    data_runs = summary_table[summary_table["run_type"] == "DATA"]

    gainsel_summary = []
    for run in data_runs:
        run_id = run["run_id"]
        gainsel_job_status = check_gainsel_jobs_runwise(date, run_id)
        gainsel_summary.append([run_id, gainsel_job_status])

    gainsel_df = pd.DataFrame(gainsel_summary, columns=['run_id', 'pending','success','failed'])
    gainsel_df['GainSelStatus'] = np.where(gainsel_df['failed'] != 0,
                                           'FAILED',
                                           np.where(gainsel_df['pending'] != 0,
                                                    'PENDING',
                                                    'COMPLETED'))

    total_job_number = gainsel_df['pending'] + gainsel_df['failed'] + gainsel_df['success']
    gainsel_df['GainSel%'] = round(gainsel_df['success'] * 100 / total_job_number, 1)

    summary_table = summary_table.to_pandas()

    final_table = pd.merge(summary_table, gainsel_df, on="run_id")[['run_id',
                                                                    'n_subruns',
                                                                    'run_type',
                                                                    'pending',
                                                                    'success',
                                                                    'failed',
                                                                    'GainSelStatus',
                                                                    'GainSel%']]

    return final_table

def main():
    """Produce the html file with the processing OSA Gain Selection status."""
    args = ArgumentParser(
        description="Script to make an xhtml from LSTOSA sequencer output", parents=[common_parser]
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

    if not run_summary_file.is_file() or len(Table.read(run_summary_file)["run_id"]) == 0:

        html_table = "<p>No data found</p>"
        # Save the HTML file
        directory = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
        directory.mkdir(parents=True, exist_ok=True)
        html_file = directory / f"osa_gainsel_status_{flat_date}.html"
        html_file.write_text(html_content(html_table, date, "OSA Gain Selection"), encoding="utf-8")

    else:
       # Get the table with the gain selection check report:
        table_gain_selection_jobs = check_failed_jobs(date)

        table_gain_selection_jobs.reset_index(drop=True, inplace=True)
        html_table = table_gain_selection_jobs.to_html()

        # Save the HTML file
        directory = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))

        directory.mkdir(parents=True, exist_ok=True)

        html_file = directory / f"osa_gainsel_status_{flat_date}.html"
        html_file.write_text(html_content(html_table, date, "OSA Gain Selection"), encoding="utf-8")

if __name__ == "__main__":
    main()
