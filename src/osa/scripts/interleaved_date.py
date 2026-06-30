"""
interleaved_date.py

Remove interleaved DL1 data products based on source type.

This script:
- Finds interleaved data within a given date range (last month)
- Preserves Crab runs
- Removes interleaved data for non-Crab sources
- Generates a bash script with rm commands (does NOT delete automatically) in OSA/interleaved_cleanup_sh

Usage:
------
python interleaved_date.py YYYYMMDD -c config.cfg

Example:
--------
python interleaved_date.py 20260101 -c sequencer.cfg

The script generates a bash file that must be manually reviewed and executed.

Configuration file must contain:
[LST1]
BASE=/path/to/base
DL1_DIR=%(BASE)s/DL1
RUN_SUMMARY_DIR=%(BASE)s/monitoring/RunSummary
RUN_CATALOG=%(BASE)s/monitoring/RunCatalog
OSA_DIR=%(BASE)s/OSA
"""

import os
import sys
import csv
import glob
import configparser
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

__all__ = [
    "clean_path",
    "load_config",
    "summary_dates",
    "info_dates",
    "find_interleaved",
]




def clean_path(raw_path, base):
    raw_path = raw_path.strip()

    if "%(BASE)s" in raw_path:
        raw_path = raw_path.replace("%(BASE)s", base)

    return raw_path



def load_config(cfg_file):
    cfg_path = Path(cfg_file)

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_file}")

    config = configparser.ConfigParser(delimiters=(":", "="))
    config.optionxform = str
    config.read(cfg_file)

    if "LST1" not in config:
        raise ValueError("Missing [LST1] section in cfg")

    section = config["LST1"]

    required = ["BASE", "DL1_DIR", "RUN_SUMMARY_DIR", "RUN_CATALOG", "OSA_DIR", "ANALYSIS_DIR"]
    missing = [k for k in required if not section.get(k) or not section.get(k).strip()]
    if missing:
        raise ValueError(f"Missing config option(s) in [LST1]: {', '.join(missing)}")

    base = section.get("BASE").strip()

    dl1_dir = clean_path(section.get("DL1_DIR"), base)
    summary_dir = clean_path(section.get("RUN_SUMMARY_DIR"), base)
    catalog_dir = clean_path(section.get("RUN_CATALOG"), base)
    osa_dir = clean_path(section.get("OSA_DIR"), base)
    analysis_dir = clean_path(section.get("ANALYSIS_DIR"), base)

    return dl1_dir, summary_dir, catalog_dir, osa_dir, analysis_dir




def find_interleaved(target_date_str, data_dir):
    """Find interleaved directories within the last month."""
    try:
        target_date = datetime.strptime(target_date_str, "%Y%m%d").date()
    except ValueError:
        print("Invalid format. Use YYYYMMDD")
        sys.exit(1)

    interleaved_paths = []
    interleaved_dates = []

    if not os.path.isdir(data_dir):
        return interleaved_paths, interleaved_dates

    for date_dir in sorted(os.listdir(data_dir)):
        date_path = os.path.join(data_dir, date_dir)

        if not os.path.isdir(date_path):
            continue

        if not (len(date_dir) == 8 and date_dir.isdigit()):
            continue

        try:
            date_obj = datetime.strptime(date_dir, "%Y%m%d").date()
        except ValueError:
            continue

        if date_obj > target_date or date_obj < (target_date - relativedelta(months=1)):
            continue

        for root, dirs, _ in os.walk(date_path):
            if "interleaved" in dirs:
                interleaved_paths.append(os.path.join(root, "interleaved"))
                interleaved_dates.append(date_dir)

    return interleaved_paths, interleaved_dates


def summary_dates(date, summary_dir):

    entry = {"run_id": [], "run_type": []}
    filepath = os.path.join(summary_dir, f"RunSummary_{date}.ecsv")

    if not os.path.isfile(filepath):
        return entry

    with open(filepath, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            try:
                entry["run_id"].append(int(row[0]))
                entry["run_type"].append(row[2])
            except (ValueError, IndexError):
                continue

    return entry


def info_dates(date, catalog_dir, runs_id):

    entry = {"crab": [], "other_source": []}
    filepath = os.path.join(catalog_dir, f"RunCatalog_{date}.ecsv")

    if not os.path.isfile(filepath):
        return entry

    runs_id_set = set(runs_id)

    with open(filepath, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#") or row[0].startswith("run"):
                continue

            try:
                run_id = int(row[0])
            except ValueError:
                continue

            if run_id in runs_id_set:
                try:
                    source_name = row[1]
                except IndexError:
                    continue

                if "crab" in source_name.lower():
                    entry["crab"].append(run_id)
                else:
                    entry["other_source"].append(run_id)

    return entry




if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Use: python interleaved_date.py YYYYMMDD -c config.cfg")
        sys.exit(1)

    date_arg = sys.argv[1]

    cfg_file = None
    for i, arg in enumerate(sys.argv):
        if arg == "-c":
            if i + 1 >= len(sys.argv):
                print("Config file not provided")
                sys.exit(1)
            cfg_file = sys.argv[i + 1]
            break

    if cfg_file is None:
        print("Config file not provided")
        sys.exit(1)

    dl1_dir, summary_dir, catalog_dir, osa_dir, analysis_dir = load_config(cfg_file)
    try:
        datetime.strptime(date_arg, "%Y%m%d")
    except ValueError:
        print("Invalid format. Use YYYYMMDD")
        sys.exit(1)

    output_dir = os.path.join(osa_dir, "interleaved_cleanup_sh")
    os.makedirs(output_dir, exist_ok=True)
    month = date_arg[:6]
    recordfile = os.path.join(output_dir, f"entries_rm_{month}.sh")

    with open(recordfile, "w") as file:
        file.write("#!/bin/bash\n")

    found_paths, found_dates = find_interleaved(date_arg, dl1_dir)


    new_paths = [
        p.replace(dl1_dir, analysis_dir)
        for p in found_paths
    ]

    for path, link_path, date in zip(found_paths, new_paths, found_dates):

        summary = summary_dates(date, summary_dir)

        if not summary["run_id"]:
            continue

        data_runs = [
            run_id for run_id, run_type in zip(summary["run_id"], summary["run_type"])
            if run_type == "DATA"
        ]

        entry = info_dates(date, catalog_dir, data_runs)

        if not entry["other_source"]:
            continue

        found_dataruns = sorted(entry["crab"] + entry["other_source"])

        if not entry["crab"] and len(found_dataruns) == len(data_runs):

            filename = "interleaved_LST-1.Run*.h5"

            with open(recordfile, "a") as file:
                file.write(f'rm -f -- "{link_path}"/{filename}\n')
                file.write(f'rm -rf -- "{path}"\n')

        else:
            for runid in entry["other_source"]:
                run_str = f"{runid:05d}"
                filename = f"interleaved_LST-1.Run{run_str}.*.h5"

                filepath = os.path.join(path, filename)
                link_filepath = os.path.join(link_path, filename)

                if glob.glob(filepath) or glob.glob(link_filepath):
                    with open(recordfile, "a") as file:
                        file.write(f'rm -f -- "{path}"/{filename}\n')
                        file.write(f'rm -f -- "{link_path}"/{filename}\n')
