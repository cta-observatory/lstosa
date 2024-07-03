
import logging
import re
import glob
from pathlib import Path
import argparse
import sys
from textwrap import dedent

from astropy.table import Table
from datetime import datetime, timedelta
from osa.utils.utils import date_to_dir, date_to_iso
from osa.configs.config import cfg
from osa.paths import DEFAULT_CFG
from osa.nightsummary.nightsummary import run_summary_table
import numpy as np
import pandas as pd
from typing import Iterable
from argparse import ArgumentParser
from osa.configs import options

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
          <title>OSA Gain Selection status</title><link href="osa.css" rel="stylesheet"
          type="text/css" /><style>table{{width:152ex;}}</style>
         </head>
         <body>
         <h1>OSA Gain Selection processing status</h1>
         <p>Processing data from: {date}. Last updated: {time_update} UTC</p>
         {body}
         </body>
        </html>"""
    )

    
def check_gainsel_jobs_runwise(date: str, run_id: int) -> bool:
    """Search for failed jobs in the log directory."""
    base_dir = Path("/fefs/aswg/data/real") #Path(cfg.get("LST1", "BASE"))
    log_dir = base_dir / f"R0G/log/{date}"
    history_files = log_dir.glob(f"gain_selection_{run_id:05d}.????.history")

    success_subruns = 0
    failed_subruns = 0
    pending_subruns = 0
        
    for file in history_files:
        match = re.search(f"gain_selection_{run_id:05d}.(\d+).history", str(file))
        subrun = match.group(1)
        if file.read_text() != "":
            gainsel_rc = file.read_text().splitlines()[-1][-1]
            
            if gainsel_rc == "1":
                failed_subruns = failed_subruns+1

            elif gainsel_rc == "0":
                success_subruns = success_subruns+1
        else:
            pending_subruns = pending_subruns+1
    return [pending_subruns, success_subruns, failed_subruns]



def check_failed_jobs(date: str):
    """Search for failed jobs in the log directory."""
    summary_table = run_summary_table(datetime.fromisoformat(date))
    data_runs = summary_table[summary_table["run_type"] == "DATA"]

    gainsel_summary = []
    for run in data_runs:
        run_id = run["run_id"]
        checkgainsel = check_gainsel_jobs_runwise(date.replace('-',''), run_id)
        gainsel_summary.append([run_id, checkgainsel[0], checkgainsel[1], checkgainsel[2]])
        
    gainsel_df = pd.DataFrame(gainsel_summary, columns=['run_id', 'pending','success','failed'])
    gainsel_df['GainSelStatus'] = np.where(gainsel_df['failed'] != 0, 'FAILED', np.where(gainsel_df['pending'] != 0, 'PENDING', 'COMPLETED'))
    gainsel_df['GainSel%'] = round(gainsel_df['success']*100/(gainsel_df['pending']+gainsel_df['failed']+gainsel_df['success'])
,1)
    runs = summary_table["run_id"]
    summary_table = summary_table.to_pandas()
    final_table = pd.merge(summary_table, gainsel_df, on="run_id")[['run_id','n_subruns','run_type','pending','success','failed','GainSelStatus', 'GainSel%']]
    
    return final_table

def main():
    """Produce the html file with the processing OSA Gain Selection status."""
    args = ArgumentParser(
        description="Script to make an xhtml from LSTOSA sequencer output", parents=[common_parser]
    ).parse_args()

    
    html_table = ''
    
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
        directory = Path("/fefs/aswg/data/real/OSA/GainSelWeb")#Path(cfg.get("LST1", "SEQUENCER_WEB_DIR"$
        directory.mkdir(parents=True, exist_ok=True)
        html_file = directory / Path(f"osa_gainsel_status_{flat_date}.html")
        html_file.write_text(html_content(html_table, date), encoding="utf-8")
        
    else:
       # Get the table with the sequencer status report:
        lines = check_failed_jobs(date)


        lines.reset_index(drop=True, inplace=True)
        if html_table == '':
            html_table = lines.to_html()
      
        # Save the HTML file
        directory = Path("/fefs/aswg/data/real/OSA/GainSelWeb")#Path(cfg.get("LST1", "SEQUENCER_WEB_DIR"))
    
        directory.mkdir(parents=True, exist_ok=True)

        html_file = directory / Path(f"osa_gainsel_status_{flat_date}.html")
        html_file.write_text(html_content(html_table, date), encoding="utf-8")
    
if __name__ == "__main__":
    main()

