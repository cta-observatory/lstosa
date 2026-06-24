import logging
import pandas as pd
import os
import fnmatch
from pathlib import Path

import click
from astropy.table import Table

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

@click.command()

def find_file(root_dir, partial_name):
    matches = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in fnmatch.filter(filenames, f'*{partial_name}*'):
            matches.append(os.path.join(dirpath, filename))
    return matches

def main():
    log.setLevel(logging.INFO)
    
    with open('date_list.txt', 'r') as dates_file:
        list_of_dates = dates_file.read().splitlines()


    for date in list_of_dates:
#        print(date)
        tailcut_list=[]
        dl1_data_dir = f"/fefs/onsite/data/lst-pipe/LSTN-01/DL1/{date}/v0.11/"  
        for subdir in os.listdir(dl1_data_dir):
            if "tailcut" in subdir:
                tailcut_list += subdir.split("tailcut")[1:]

        run_summary_dir = Path("/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary")  
        run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
        summary_table = Table.read(run_summary_file)
        data_runs = summary_table[summary_table["run_type"] == "DATA"]

        dl1_filenames = []
        #d_runs=summary_table[summary_table["run_id"] == 12955] 
        for run in data_runs:
            run_id = run["run_id"]
            subruns = run["n_subruns"]

            dl1_exist = False

# JJLB
            for tailcut in tailcut_list:
                dl1_filename = f"/fefs/onsite/data/lst-pipe/LSTN-01/DL1/{date}/v0.11/tailcut{tailcut}/dl1_LST-1.Run{run_id:05d}.????.h5"

                for subrun in range (subruns):
#                    root_directory = f"/fefs/onsite/data/lst-pipe/LSTN-01/DL1/{date}/v0.11/tailcut{tailcut}"  # Change this to your root directory
#                    partial_filename = f"dl1_LST-1.Run{run_id:05d}.{subruns:04d}.h5"         # Part of the filename you're looking for
#                    found_files = find_file(root_directory, partial_filename)
                    dl1_file = Path(f"/fefs/onsite/data/lst-pipe/LSTN-01/DL1/{date}/v0.11/tailcut{tailcut}/dl1_LST-1.Run{run_id:05d}.{subrun:04d}.h5")
                    if dl1_file.exists():
                        dl1_exist = True
                        break

                if dl1_file.exists():
                    dl1_exist = True
                    break

            if dl1_exist:
#                print(dl1_filenames)
                dl1_filenames.append(dl1_filename)

        if dl1_filenames:
            with open('all_runs.txt', 'a') as output_file:
                for dl1_filename in dl1_filenames:
                    output_file.write(f"{dl1_filename}\n")

if __name__ == "__main__":
    main()

