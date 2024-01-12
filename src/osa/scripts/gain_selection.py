"""Script to run the gain selection over a list of dates."""
import logging
import re
import shutil
import glob
import pandas as pd
import subprocess as sp
from pathlib import Path
from textwrap import dedent
from io import StringIO

import click
from astropy.table import Table
from lstchain.paths import run_info_from_filename, parse_r0_filename

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.utils import wait_for_daytime
from osa.utils.logging import myLogger
from osa.job import get_sacct_output, FORMAT_SLURM

log = myLogger(logging.getLogger(__name__))

PATH = "PATH=/fefs/aswg/software/offline_dvr/bin:$PATH"


def get_sbatch_script(
    run_id, subrun, input_file, output_dir, log_dir, ref_time, ref_counter, module, ref_source
):
    """Build the sbatch job pilot script for running the gain selection."""
    return dedent(
        f"""\
    #!/bin/bash

    #SBATCH -D {log_dir}
    #SBATCH -o "gain_selection_{run_id:05d}_{subrun:04d}_%j.log"
    #SBATCH --job-name "gain_selection_{run_id:05d}"
    #SBATCH --export {PATH}

    lst_dvr {input_file} {output_dir} {ref_time} {ref_counter} {module} {ref_source}
    """
    )


def apply_gain_selection(date: str, start: int, end: int, output_basedir: Path = None):
    """
    Submit the jobs to apply the gain selection to the data for a given date
    on a subrun-by-subrun basis.
    """

    run_summary_dir = Path("/fefs/aswg/data/real/monitoring/RunSummary")
    run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
    summary_table = Table.read(run_summary_file)
    # Apply gain selection only to DATA runs
    data_runs = summary_table[summary_table["run_type"] == "DATA"]

    output_dir = output_basedir / date
    log_dir = output_basedir / "log" / date
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    r0_dir = Path(f"/fefs/aswg/data/real/R0/{date}")

    for run in data_runs:
        # Check slurm queue status and sleep for a while to avoid overwhelming the queue
        check_job_status_and_wait(max_jobs=1500)

        # Avoid running jobs while it is still night time
        wait_for_daytime(start, end)

        run_id = run["run_id"]
        ref_time = run["dragon_reference_time"]
        ref_counter = run["dragon_reference_counter"]
        module = run["dragon_reference_module_index"]
        ref_source = run["dragon_reference_source"].upper()

        files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.????.fits.fz")
        subrun_numbers = [int(file[-12:-8]) for file in files]
        input_files = []

        if ref_source in ["UCTS", "TIB"]:

            n_subruns = max(subrun_numbers)

            for subrun in range(n_subruns + 1):
                new_files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.{subrun:04d}.fits.fz")

                if len(new_files) != 4:
                    for file in new_files:
                        sp.run(["cp", file, output_dir])

                else:
                    new_files.sort()
                    input_files.append(new_files[0])

            for file in input_files:
                run_info = run_info_from_filename(file)
                job_file = log_dir / f"gain_selection_{run_info.run:05d}.{run_info.subrun:04d}.sh"
                with open(job_file, "w") as f:
                    f.write(
                        get_sbatch_script(
                            run_id,
                            run_info.subrun,
                            file,
                            output_dir,
                            log_dir,
                            ref_time,
                            ref_counter,
                            module,
                            ref_source,
                        )
                    )
                sp.run(["sbatch", job_file], check=True)

        else:

            input_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

            for file in input_files:
                sp.run(["cp", file, output_dir])

    calib_runs = summary_table[summary_table["run_type"] != "DATA"]

    for run in calib_runs:
        # Avoid copying files while it is still night time
        wait_for_daytime(start, end)

        run_id = run["run_id"]
        r0_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

        for file in r0_files:
            sp.run(["cp", file, output_dir])

def run_sacct_j(job) -> StringIO:
    """Run sacct to obtain the job information."""
    if shutil.which("sacct") is None:
        log.warning("No job info available since sacct command is not available")
        return StringIO()

    sacct_cmd = [
        "sacct",
        "-n",
        "--parsable2",
        "--delimiter=,",
        "--units=G",
        "-o",
        ",".join(FORMAT_SLURM),
        "-j",
        job,
    ]

    return StringIO(sp.check_output(sacct_cmd).decode())


def check_failed_jobs(date: str, output_basedir: Path = None):
    """Search for failed jobs in the log directory."""
    failed_jobs = []
    log_dir = output_basedir / "log" / date
    filenames = glob.glob(f"{log_dir}/gain_selection*.log")
    jobs = [re.search(r'(?<=_)(.[0-9.]+?)(?=.log)', i).group(0) for i in filenames]

    for job in jobs:
        output = run_sacct_j(job)
        df = get_sacct_output(output)

        if not df.iloc[0]["State"] == "COMPLETED":
            log.warning(f"Job {job} did not finish successfully")
            failed_jobs.append(job)

    if not failed_jobs:
        log.info(f"{date}: all jobs finished successfully")
    else:
        log.warning(f"{date}: some jobs did not finish successfully")


    run_summary_dir = Path("/fefs/aswg/data/real/monitoring/RunSummary")
    run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
    summary_table = Table.read(run_summary_file)
    runs = summary_table["run_id"]
    missing_runs = []

    r0_files = glob.glob(f"/fefs/aswg/data/real/R0/{date}/LST-1.?.Run?????.????.fits.fz")
    r0g_files = glob.glob(f"/fefs/aswg/data/real/R0G/{date}/LST-1.?.Run?????.????.fits.fz")
    all_r0_runs = [parse_r0_filename(i).run for i in r0_files]
    all_r0g_runs = [parse_r0_filename(i).run for i in r0g_files]

    for run in all_r0_runs:
        if run not in runs:
            if run not in all_r0g_runs:
                missing_runs.append(run)

    missing_runs.sort()
    if missing_runs:
        log.info(
            f"Some runs are missing. Copying R0 files of runs {pd.Series(missing_runs).unique()} "
            f"directly to /fefs/aswg/data/real/R0G/{date}"
        )

        for run in missing_runs:
            output_dir = Path(f"/fefs/aswg/data/real/R0G/{date}/")
            files = glob.glob(f"/fefs/aswg/data/real/R0/{date}/LST-1.?.Run{run:05d}.????.fits.fz")
            for file in files:
                sp.run(["cp", file, output_dir])



@click.command()
@click.option("--check", is_flag=True, default=False, help="Check for failed jobs.")
@click.argument("dates-file", type=click.Path(exists=True, path_type=Path))
@click.argument("output-basedir", type=click.Path(path_type=Path))
@click.option("-s", "--start-time", type=int, default=10, help="Time to (re)start gain selection in HH format.")
@click.option("-e", "--end-time", type=int, default=18, help="Time to stop gain selection in HH format.")
def main(
    dates_file: Path = None, 
    output_basedir: Path = None, 
    check: bool = False, 
    start_time: int = 10, 
    end_time: int = 18
):
    """
    Loop over the dates listed in the input file and launch the gain selection
    script for each of them. The input file should list the dates in the format
    YYYYMMDD one date per line.
    """
    log.setLevel(logging.INFO)

    list_of_dates = get_list_of_dates(dates_file)

    if check:
        for date in list_of_dates:
            check_failed_jobs(date, output_basedir)
    else:
        for date in list_of_dates:
            apply_gain_selection(date, start_time, end_time, output_basedir)
        log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
