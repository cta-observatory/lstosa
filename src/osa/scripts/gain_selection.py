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
import argparse

from astropy.table import Table
from lstchain.paths import run_info_from_filename, parse_r0_filename

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.utils import wait_for_daytime
from osa.utils.logging import myLogger
from osa.job import get_sacct_output, FORMAT_SLURM
from osa.configs.config import cfg
from osa.paths import DEFAULT_CFG

log = myLogger(logging.getLogger(__name__))

PATH = "PATH=/fefs/aswg/software/offline_dvr/bin:$PATH"

parser = argparse.ArgumentParser()
parser.add_argument(
        "--check",                                                                                       
        action="store_true",
        default=False,
        help="Check if any job failed",
)
parser.add_argument(
        "--no-queue-check",
        action="store_true",
        default=False,
        help="Do not wait until the number of jobs in the slurm queue is < 1500",
)
parser.add_argument(
        "-c",                                                                                            
        "--config",                                                                                      
        action="store",
        type=Path,
        default=DEFAULT_CFG,
        help="Configuration file",
)
parser.add_argument(                                                                                     
        "-d",                                                                                            
        "--date",                                                                                        
        default=None,
        type=str,
        help="Night to apply the gain selection in YYYYMMDD format",
)                                                                                                        
parser.add_argument(                                                                                     
        "-l",                                                                                            
        "--dates-file",
        default=None,
        help="List of dates to apply the gain selection. The input file should list"
        "the dates in the format YYYYMMDD, one date per line.",
)
parser.add_argument(                                                                                     
        "-o",                                                                                            
        "--output-basedir",
        type=Path,
        default=Path("/fefs/aswg/data/real/R0G"),
        help="Output directory of the gain selected files. Default is /fefs/aswg/data/real/R0G."
)                                                                                                        
parser.add_argument(                                                                                     
        "-s",                                                                                            
        "--start-time",
        type=int,
        default=10,
        help="Time to (re)start gain selection in HH format. Default is 10.",
)                                                                                                       
parser.add_argument(                                                                                     
        "-e",                                                                                            
        "--end-time",
        type=int,
        default=18,
        help="Time to stop gain selection in HH format. Default is 18.",
)
parser.add_argument(                                                                                     
        "-t",                                                                                            
        "--tool",
        type=str,
        default=None,
        help="Choose tool to apply the gain selection regardless the date. Possible options are: lst_dvr (by default used for dates "
        "previous to 20231205) and lstchain_r0_to_r0g (by default used for dates later than 20231205).",
)

def get_sbatch_script(
    run_id, subrun, input_file, output_dir, log_dir, log_file, ref_time, ref_counter, module, ref_source, tool
):
    """Build the sbatch job pilot script for running the gain selection."""
    if tool == "lst_dvr":
        return dedent(
            f"""\
        #!/bin/bash

        #SBATCH -D {log_dir}
        #SBATCH -o "gain_selection_{run_id:05d}_{subrun:04d}_%j.log"
        #SBATCH --job-name "gain_selection_{run_id:05d}"
        #SBATCH --export {PATH}
        #SBATCH --partition=short,long

        lst_dvr {input_file} {output_dir} {ref_time} {ref_counter} {module} {ref_source}
        """
        )
    elif tool == "lstchain_r0_to_r0g":
        return dedent(
            f"""\
        #!/bin/bash

        #SBATCH -D {log_dir}
        #SBATCH -o "gain_selection_{run_id:05d}_{subrun:04d}_%j.log"
        #SBATCH --job-name "gain_selection_{run_id:05d}"
        #SBATCH --mem=40GB
        #SBATCH --partition=short,long

        lstchain_r0_to_r0g --R0-file={input_file} --output-dir={output_dir} --log={log_file} --no-flatfield-heuristic
        """
        )

def apply_gain_selection(date: str, start: int, end: int, output_basedir: Path = None, tool: str = None, no_queue_check: bool = False):
    """
    Submit the jobs to apply the gain selection to the data for a given date
    on a subrun-by-subrun basis.
    """

    if not tool:
        if date < "20231205":
            tool = "lst_dvr"
        else:
            tool = "lstchain_r0_to_r0g"

    run_summary_dir = Path("/fefs/aswg/data/real/monitoring/RunSummary")
    run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
    summary_table = Table.read(run_summary_file)
    # Apply gain selection only to DATA runs
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    log.info(f"Found {len(data_runs)} DATA runs to which apply the gain selection")

    output_dir = output_basedir / date
    log_dir = output_basedir / "log" / date
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"r0_to_r0g_{date}.log"
    r0_dir = Path(f"/fefs/aswg/data/real/R0/{date}")

    for run in data_runs:
        if not no_queue_check:
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

        if tool == "lst_dvr" and ref_source not in ["UCTS", "TIB"]:
            input_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")
            log.info(
                f"Run {run_id} does not have UCTS or TIB info, so gain selection cannot"
                f"be applied. Copying directly the R0 files to {output_dir}."
            )
            for file in input_files:
                sp.run(["cp", file, output_dir])

        else:
            n_subruns = max(subrun_numbers)

            for subrun in range(n_subruns + 1):
                new_files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.{subrun:04d}.fits.fz")

                if len(new_files) != 4:
                    log.info(f"Run {run_id}.{subrun:05d} does not have 4 streams of R0 files, so gain"
                        f"selection cannot be applied. Copying directly the R0 files to {output_dir}."
                    )
                    for file in new_files:
                        sp.run(["cp", file, output_dir])

                else:
                    new_files.sort()
                    input_files.append(new_files[0])

            log.info("Creating and launching the sbatch scripts for the rest of the runs to apply gain selection")
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
                            log_file,
                            ref_time,
                            ref_counter,
                            module,
                            ref_source,
                            tool,
                        )
                    )
                sp.run(["sbatch", job_file], check=True)

    calib_runs = summary_table[summary_table["run_type"] != "DATA"]
    log.info(f"Found {len(calib_runs)} NO-DATA runs")

    for run in calib_runs:
        run_id = run["run_id"]
        log.info(f"Copying R0 files corresponding to run {run_id} directly to {output_dir}")
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


def GainSel_flag_file(date: str) -> Path:
    filename = cfg.get("LSTOSA", "gain_selection_check")
    GainSel_dir = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
    flagfile = GainSel_dir / date / filename
    return flagfile.resolve()


def GainSel_finished(date: str) -> bool:
    """Check if gain selection finished successfully."""
    flagfile = GainSel_flag_file(date)
    return flagfile.exists()


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

    if failed_jobs:
        log.warning(f"{date}: some jobs did not finish successfully")

    else:
        log.info(f"{date}: all jobs finished successfully")


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

        GainSel_dir = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
        flagfile_dir = GainSel_dir / date
        flagfile_dir.mkdir(parents=True, exist_ok=True)
        
        flagfile = GainSel_flag_file(date)
        log.info(f"Gain selection finished successfully, creating flag file for date {date} ({flagfile})")
        flagfile.touch()


def main():
    """
    Loop over the dates listed in the input file and launch the gain selection
    script for each of them. The input file should list the dates in the format
    YYYYMMDD one date per line.
    """
    log.setLevel(logging.INFO)
    args = parser.parse_args()

    if args.date:
        if args.check:
            log.info(f"Checking gain selection status for date {args.date}")
            check_failed_jobs(args.date, args.output_basedir)
        else:
            log.info(f"Applying gain selection to date {args.date}")
            apply_gain_selection(
                args.date, 
                args.start_time, 
                args.end_time, 
                args.output_basedir,
                args.tool,
                no_queue_check=args.no_queue_check, 
            )


    elif args.dates_file:
        list_of_dates = get_list_of_dates(args.dates_file)
        log.info(f"Found {len(list_of_dates)} dates to apply or check gain selection")

        if args.check:
            for date in list_of_dates:
                log.info(f"Checking gain selection status for date {date}")
                check_failed_jobs(date, args.output_basedir)
        else:
            for date in list_of_dates:
                log.info(f"Applying gain selection to date {date}")
                apply_gain_selection(
                    date, 
                    args.start_time, 
                    args.end_time,
                    args.output_basedir,
                    args.tool,
                    no_queue_check=args.no_queue_check,
                )
            log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
