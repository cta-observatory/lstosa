"""
Print out basic run summary for a given date with run numbers, type of runs,
start and stop timestamps and elapsed times.
"""

import argparse
import datetime
from pathlib import Path

import astropy.units as u
import numpy as np
from astropy.table import Table
from lstchain.scripts.lstchain_create_run_summary import get_list_of_files, get_list_of_runs

from osa.nightsummary.database import get_run_info_from_TCU

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

parser.add_argument(
    "--tcu-db",
    type=str,
    help="Server of the TCU monitoring database",
    default="lst101-int",
)


def main():
    """
    Get run metadata information from TCU monitoring
    database and print out the run summary
    """
    args = parser.parse_args()

    tcu_db = args.tcu_db
    date_path = args.r0_path / args.date
    file_list = get_list_of_files(date_path)
    all_runs = get_list_of_runs(file_list)
    run_numbers = [x.run for x in all_runs]
    run_numbers_array = np.unique(run_numbers)
    run_numbers_array = run_numbers_array[run_numbers_array != 0]

    list_info = []

    for run in run_numbers_array:
        run_info = get_run_info_from_TCU(int(run), tcu_server=tcu_db)
        list_info.append(run_info)

    if list_info:
        table = Table(
            np.array(list_info).T.tolist(),
            names=("run", "type", "tstart", "tstop", "elapsed"),
            dtype=(int, str, datetime.datetime, datetime.datetime, float),
        )
        table["elapsed"].unit = u.min
        table["elapsed"].info.format = "3.1f"
        print("\n")
        table.pprint_all()

        # Sum elapsed times:
        obs_by_type = table.group_by("type")
        obs_by_type["number_of_runs"] = 1
        total_obs_time = obs_by_type["type", "number_of_runs", "elapsed"].groups.aggregate(np.sum)
        total_obs_time["elapsed"].info.format = "7.0f"

        print("\n")
        header = " Observation time per run type "
        print(f"{header.center(50, '*')}")
        total_obs_time.pprint_all()
        print("\n")

    else:
        print(f"No data found in {date_path}")


if __name__ == "__main__":
    main()
