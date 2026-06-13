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
]


# =========================================================
# CONFIG
# =========================================================

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

    base = section.get("BASE").strip()

    summary_dir = clean_path(section.get("RUN_SUMMARY_DIR"), base)
    catalog_dir = clean_path(section.get("RUN_CATALOG"), base)
    osa_dir = clean_path(section.get("OSA_DIR"), base)

    return summary_dir, catalog_dir, osa_dir


# =========================================================
# DATA DIRECTORIES
# =========================================================

data_dirs = [
    "/fefs/onsite/data/lst-pipe/LSTN-01/DL1",
    "/fefs/aswg/data/real/DL1"
]


# =========================================================
# FUNCTIONS
# =========================================================

def find_interleaved(target_date_str):

    try:
        target_date = datetime.strptime(target_date_str, "%Y%m%d").date()
    except ValueError:
        print("Invalid format. Use YYYYMMDD")
        sys.exit(1)

    interleaved_paths = []
    interleaved_dates = []

    for base_dir in data_dirs:
        if not os.path.isdir(base_dir):
            continue

        for date_dir in sorted(os.listdir(base_dir)):
            date_path = os.path.join(base_dir, date_dir)

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

    with open(filepath, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#") or row[0].startswith("run"):
                continue

            try:
                run_id = int(row[0])
            except ValueError:
                continue

            if run_id in runs_id:
                try:
                    source_name = row[1]
                except IndexError:
                    continue

                if "crab" in source_name.lower():
                    entry["crab"].append(run_id)
                else:
                    entry["other_source"].append(run_id)

    return entry


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Use: python interleaved_date.py <YYYYMMDD> -c <cfg_file>")
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

    summary_dir, catalog_dir, osa_dir = load_config(cfg_file)

    backup_summary_dir = summary_dir
    backup_catalog_dir = catalog_dir

    output_dir = os.path.join(osa_dir, "interleaved_cleanup_sh")
    os.makedirs(output_dir, exist_ok=True)

    month = date_arg[:6]
    recordfile = os.path.join(output_dir, f"entries_rm_{month}.sh")

    with open(recordfile, "w") as file:
        file.write("#!/bin/bash\n")

    found_paths, found_dates = find_interleaved(date_arg)

    new_paths = [
        p.replace("/DL1/", "/running_analysis/").removesuffix("/interleaved")
        for p in found_paths
    ]

    for path, link_path, date in zip(found_paths, new_paths, found_dates):

        summary = summary_dates(date, summary_dir)
        if not summary["run_id"]:
            summary = summary_dates(date, backup_summary_dir)

        if not summary["run_id"]:
            continue

        data_runs = [
            run_id for run_id, run_type in zip(summary["run_id"], summary["run_type"])
            if run_type == "DATA"
        ]

        entry = info_dates(date, catalog_dir, data_runs)
        if not entry["crab"] and not entry["other_source"]:
            entry = info_dates(date, backup_catalog_dir, data_runs)

        if not entry["other_source"]:
            continue

        found_dataruns = sorted(entry["crab"] + entry["other_source"])

        if not entry["crab"] and len(found_dataruns) == len(data_runs):

            filename = "interleaved_LST-1.Run*.h5"
            link_filepath = os.path.join(link_path, filename)

            with open(recordfile, "a") as file:
                file.write(f"rm {link_filepath}\n")
                file.write(f"rm -r {path}\n")

        else:
            for runid in entry["other_source"]:
                run_str = f"{runid:05d}"
                filename = f"interleaved_LST-1.Run{run_str}.*.h5"

                filepath = os.path.join(path, filename)
                link_filepath = os.path.join(link_path, filename)

                if glob.glob(filepath):
                    with open(recordfile, "a") as file:
                        file.write(f"rm {filepath}\n")
                        file.write(f"rm {link_filepath}\n")
