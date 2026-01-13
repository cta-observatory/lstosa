"""
Remove interleaved directories for data runs that have already been processed.

Given an input date (<YYYYMMDD>), the script searches for interleaved directories
within the last month. For each date in that period, it retrieves the runs taken
and identifies the corresponding observed sources. 
It creates a shell file to remove all of the interleaved files identified
(those that do not correspond to Crab observations). Then, it have to be removed
using SLURM command sbatch. 

At the moment, this shell files to be removed are saved in my workspace:
fefs/aswg/workspace/maria.rivero/remove_sh
"""

import os
import sys
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import glob

# Data directories to look for links of interleaved directories
base_dirs = [
    "/fefs/onsite/data/lst-pipe/LSTN-01/running_analysis",
    "/fefs/aswg/data/real/running_analysis"
]

# Directory of RunSummary files
summary_dir = "/fefs/aswg/data/real/monitoring/RunSummary"
backup_summary_dir = "/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary"

# Directories to check run's sources in RunCatalogs
catalog_dir = "/fefs/aswg/data/real/monitoring/RunCatalog"
backup_catalog_dir = "/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunCatalog"


def find_interleaved(target_date_str):
    """Look into base directories to find interleaved directories to be removed.
    Args:
        target_date_str (str): Last date to search within a month.  
    Returns: 
        interleaved_paths (list): Paths to interleaved directory.
        interleaved_dates (list): Observation dates of each interleaved.
    """

    # Change date format (YYYYMMDD to python date)
    try:
        target_date = datetime.strptime(target_date_str, "%Y%m%d").date()
    except ValueError:
        print("Invalid format. Use YYYYMMDD")
        sys.exit(1)

    interleaved_paths = []
    interleaved_dates = []
    
    # Let's look into both dirs and check if both exist
    for base_dir in base_dirs:
        if not os.path.isdir(base_dir):
            print(f"Path not found: {base_dir}")
            continue
        # Look in each date directory
        for date_dir in sorted(os.listdir(base_dir)):
            date_path = os.path.join(base_dir, date_dir) # given date path
            if not os.path.isdir(date_path):
                continue
            
            if not (len(date_dir) == 8 and date_dir.isdigit()):
                continue

            try:
                date_obj = datetime.strptime(date_dir, "%Y%m%d").date() #save it as date python object
            except ValueError:
                continue
            # search only in the last month to the input date
            if date_obj > target_date or date_obj < (target_date - relativedelta(months=1)):
                continue

            # look for interleaved directory (save path and date)
            for root, dirs, _ in os.walk(date_path):
                for d in dirs:
                    if d == "interleaved":
                        interleaved_paths.append(os.path.join(root, d))
                        interleaved_dates.append(date_dir)
 
    return interleaved_paths, interleaved_dates

def info_dates(date, catalog_dir, runs_id):
    """Given an observation date, it classifies runs in RunCatalog by their sources (Crab or not).
    Args:
        date (str): <YYYYMMDD> format.
        catalog_dir (str): path to RunCatalog files.
        runs_id (list): DATA runs taken from RunSummary
    Returns:
        entry (dict): stores runs by source (Crab or other) and saves other sources' names.   
    """
 
    entry = {"crab": [], "other_source": [], "others_names": []}
    filename = f"RunCatalog_{date}.ecsv"
    catalog_file = os.path.join(catalog_dir,filename)
    if not os.path.isfile(catalog_file):
        print(f"File not found: {catalog_file}")
        return entry

    else:
        with open(catalog_file, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row[0].startswith("#") or row[0].startswith("run"):
                    continue # skip comment and header lines
                if int(row[0]) in runs_id:
                    if "Crab" in row[1]:
                        entry["crab"].append(int(row[0]))
                    elif "crab" in row[1]:
                        entry["crab"].append(int(row[0]))
                    else:
                        entry["other_source"].append(int(row[0]))
                        entry["others_names"].append(row[1])
                else:
                    continue
        return(entry)


def summary_dates(date, summary_dir):
    """Given an observation date, it stores run_ids and types.
    Args:
        date (str): <YYYYMMDD> format.
        summary_dir (str): path to RunSummary files.
    Returns:
        entry (dict): stores run_id and run_type.
    """

    entry = {"run_id": [], "run_type": []}
    filename = f"RunSummary_{date}.ecsv"
    summary_file = os.path.join(summary_dir, filename)
    if not os.path.isfile(summary_file):
         print(f"File not found: {summary_file}")
         return entry
    else: 
        with open(summary_file, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue # skip comment lines
                try:
                    entry["run_id"].append(int(row[0]))
                    entry["run_type"].append(row[2])

                except ValueError:
                    continue
        return(entry)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Use: python interleaved_date.py <YYYYMMDD>")
        sys.exit(1)

    date_arg = sys.argv[1]
    found_paths, found_dates = find_interleaved(date_arg)
    
    month = date_arg[:6]
    recordfile = f'/fefs/aswg/workspace/maria.rivero/remove_sh/entries_rm{month}.sh'
    with open(recordfile, 'w') as file:
        file.write('#!/bin/bash \n')

    dl1_paths = [
        p.replace("/running_analysis/", "/DL1/")
        for p in found_paths
    ]
    print('Interleaved path: ' , dl1_paths)

    for path,link_path, date in zip(dl1_paths, found_paths, found_dates):
        summary = summary_dates(date, summary_dir)
        if not summary['run_id']:
            summary = summary_dates(date, backup_summary_dir)
        
        if not summary['run_id']:
            continue

        data_runs = [
            run_id
            for run_id, run_type in zip(summary["run_id"], summary["run_type"])
            if run_type == "DATA"]

        
        entry = info_dates(date, catalog_dir, data_runs)
        if entry["crab"] == [] and entry["other_source"] == []:
            entry = info_dates(date, backup_catalog_dir, data_runs)
        
        if not entry["other_source"]:
            continue

        print('\n Dates with interleaved: ' , date)

        print('RunSummary info:')
        print(summary)
        print('Run info (Crab or not):')
        print(entry)
        

        found_dataruns = sorted(entry["crab"] + entry["other_source"])
        if not entry["crab"] and len(found_dataruns) == len(data_runs):
            print(f"rm -r {path}")
            with open(recordfile, 'a') as file:
                file.write(f"rm -r {path} \n")
                file.write(f"rm -r {link_path} \n")
 
        else:
            for runid in entry["other_source"]:
                run_str = f"{runid:05d}" # run_ids must be always five digits
                filename = f"interleaved_LST-1.Run{run_str}.*.h5"
                filepath = os.path.join(path, filename)
                link_filepath = os.path.join(link_path, filename)
                matching_files = glob.glob(filepath) # check that exist files with run_id
                if matching_files:
                    print(f"rm {filepath}")
                    with open(recordfile, 'a') as file:
                        file.write(f"rm {filepath} \n")
                        file.write(f"rm {link_filepath} \n")
                        
                else:
                    continue
