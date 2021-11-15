"""
Show the run summary for a given date containing the number of subruns,
the start and end time of the run and type pf the run: DATA, DRS4, PEDCALIB."""

import argparse
import logging
import os
from pathlib import Path

import numpy as np
from astropy import units as u
from astropy.table import Table
from astropy.time import Time
from lstchain.scripts.lstchain_create_run_summary import (
    get_list_of_files,
    get_list_of_runs,
    get_runs_and_subruns,
    type_of_run,
    read_counters,
)

from osa.utils.logging import myLogger

log = myLogger(logging.getLogger())

parser = argparse.ArgumentParser(description="Create run summary file")

parser.add_argument(
    "-d",
    "--date",
    help="Date for the creation of the run summary in format YYYYMMDD",
    required=True,
)

parser.add_argument(
    "--r0-path",
    type=Path,
    help="Path to the R0 files. Default is /fefs/aswg/data/real/R0",
    default=Path("/fefs/aswg/data/real/R0"),
)

dtypes = {
    "time_start": str,
    "time_end": str,
    "elapsed": u.quantity.Quantity,
}


def start_end_of_run_files_stat(date_path, run_number, num_files):
    """
    Get first timestamps from the last subrun.
    Write down the reference Dragon module used, reference event_id.

    Parameters
    ----------
    date_path: pathlib.Path
        Directory that contains the R0 files
    run_number: int
        Number of the run
    num_files: int
        Number of the sequential files (subruns) of a given run

    Returns
    -------
    end_timestamp
    """

    last_subrun = num_files - 1  # first subrun is 0
    pattern_first_subrun = date_path / f"LST-1.1.Run{run_number:05d}.0000.fits.fz"
    pattern_last_subrun = (
            date_path / f"LST-1.1.Run{run_number:05d}.{last_subrun:04d}.fits.fz"
    )
    try:
        run_start_first_file = Time(os.path.getctime(pattern_first_subrun), format="unix")
        run_end_last_file = Time(os.path.getmtime(pattern_last_subrun), format="unix")
        elapsed_time = run_end_last_file - run_start_first_file

        return dict(
            time_start=run_start_first_file.iso,
            time_end=run_end_last_file.iso,
            elapsed=np.round(elapsed_time.to_value("min"), decimals=1),
        )

    except Exception as err:
        log.error(
            f"Files {pattern_first_subrun} or {pattern_last_subrun} have error: {err}"
        )

        return dict(
            time_start=None,
            time_end=None,
            elapsed=0.0,
        )


def main():
    """
    Build an astropy Table with run summary information and write it
    as ECSV file with the following information (one row per run):
     - run_id
     - number of subruns
     - type of run (DRS4, CALI, DATA, CONF)
     - start of the run
     - dragon reference UCTS timestamp if available (-1 otherwise)
     - dragon reference time source ("ucts" or "run_date")
     - dragon_reference_module_id
     - dragon_reference_module_index
     - dragon_reference_counter
    """

    log.setLevel(logging.INFO)

    args = parser.parse_args()

    date_path = args.r0_path / args.date

    file_list = get_list_of_files(date_path)
    runs = get_list_of_runs(file_list)
    run_numbers, n_subruns = get_runs_and_subruns(runs)

    reference_counters = [read_counters(date_path, run) for run in run_numbers]

    run_types = [
        type_of_run(date_path, run, counters)
        for run, counters in zip(run_numbers, reference_counters)
    ]

    start_end_timestamps = [
        start_end_of_run_files_stat(date_path, run, n_files)
        for run, n_files in zip(run_numbers, n_subruns)
    ]

    run_summary = Table(
        {
            col: np.array([d[col] for d in start_end_timestamps], dtype=dtype)
            for col, dtype in dtypes.items()
        }
    )

    run_summary.add_column(run_numbers, name="run_id", index=0)
    run_summary.add_column(n_subruns, name="n_subruns", index=1)
    run_summary.add_column(run_types, name="run_type", index=2)

    run_summary["elapsed"].unit = u.min

    header = " Run summary "
    print(f"{header.center(50, '*')}")
    run_summary.pprint_all()
    print("\n")

    # Sum elapsed times:
    obs_by_type = run_summary.group_by("run_type")
    obs_by_type["number_of_runs"] = 1
    total_obs_time = obs_by_type[
        "run_type", "number_of_runs", "n_subruns", "elapsed"
    ].groups.aggregate(np.sum)
    total_obs_time["elapsed"].format = "7.1f"

    header = " Observation time per run type "
    print(f"{header.center(50, '*')}")
    total_obs_time.pprint_all()
    print("\n")

    run_summary["number_of_runs"] = 1
    total_obs = run_summary["number_of_runs", "n_subruns", "elapsed"].groups.aggregate(
        np.sum
    )
    total_obs["elapsed"].format = "7.1f"
    header = " Total observation time "
    print(f"{header.center(50, '*')}")
    total_obs.pprint_all()


if __name__ == "__main__":
    main()
