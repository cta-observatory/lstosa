"""Script to run the gain selection over a list of dates."""
import fileinput
import logging
import re
import glob
import subprocess as sp
from pathlib import Path
from textwrap import dedent

import click
from astropy.table import Table
from lstchain.paths import run_info_from_filename

from osa.scripts.reprocessing import get_list_of_dates
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

PATH = "PATH=/fefs/aswg/software/gain_selection/bin:$PATH"


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

    lst_select_gain {input_file} {output_dir} {ref_time} {ref_counter} {module} {ref_source}
    """
    )


def apply_gain_selection(date: str, output_basedir: Path = None):
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
                        output_file = output_dir / file
                        file.link_to(output_file)

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
                output_file = output_dir / file.name
                file.link_to(output_file)

    calib_runs = summary_table[summary_table["run_type"] != "DATA"]

    for run in calib_runs:
        run_id = run["run_id"]
        r0_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

        for file in r0_files:
            output_file = output_dir / file.name
            file.link_to(output_file)


def check_failed_jobs(date: str, output_basedir: Path = None):
    """Search for failed jobs in the log directory."""
    failed_jobs = []
    log_dir = output_basedir / "log" / date
    filenames = log_dir.glob("gain_selection*.log")

    for line in fileinput.input(filenames):
        if re.search("FAILED", line) or re.search("Stream [1-4] not found", line):
            job_id = str(fileinput.filename())[-12:-4]
            run_id = str(fileinput.filename())[-23:-18]
            subrun_id = str(fileinput.filename())[-17:-13]
            failed_jobs.append(job_id)

            log.warning(f"Job {job_id} (corresponding to run {run_id}, subrun {subrun_id}) failed.")

    if not failed_jobs:
        log.info("All jobs finished successfully.")
    else:
        log.warning("Some jobs did not finish successfully.")


@click.command()
@click.option("--check", is_flag=True, default=False, help="Check for failed jobs.")
@click.argument("dates-file", type=click.Path(exists=True, path_type=Path))
@click.argument("output-basedir", type=click.Path(path_type=Path))
def main(dates_file: Path = None, output_basedir: Path = None, check: bool = False):
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
            apply_gain_selection(date, output_basedir)

        log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
