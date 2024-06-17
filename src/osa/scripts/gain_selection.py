"""Script to run the gain selection over a list of dates."""
import logging
import re
import glob
import pandas as pd
import subprocess as sp
from pathlib import Path
from textwrap import dedent
import argparse
import sys

from astropy.table import Table
from lstchain.paths import parse_r0_filename

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.utils import wait_for_daytime
from osa.utils.logging import myLogger
from osa.utils.iofile import append_to_file
from osa.job import get_sacct_output, run_sacct_j
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
parser.add_argument(                                                                                          
        "--simulate",
        type=str,
        default=False,
        help="Simulate launching of the gain selection script.",
)

def get_sbatch_script(
    run_id, subrun, input_file, output_dir, log_dir, log_file, ref_time, ref_counter, module, ref_source, tool
):
    """Build the sbatch job pilot script for running the gain selection."""
    
    sbatch_script = dedent(
            f"""\
        #!/bin/bash

        #SBATCH -D {log_dir}
        #SBATCH -o "gain_selection_{run_id:05d}_{subrun:04d}_%j.log"
        #SBATCH --job-name "gain_selection_{run_id:05d}"
        #SBATCH --partition=short,long
        """
        )
    
    if tool == "lst_dvr":
        sbatch_script += dedent(
            f"""   
        #SBATCH --export {PATH}

        lst_dvr {input_file} {output_dir} {ref_time} {ref_counter} {module} {ref_source}
        """
        )
        
    elif tool == "lstchain_r0_to_r0g":
        mem_per_job = cfg.get("SLURM", "MEMSIZE_GAINSEL")
        sbatch_script += dedent(
            f"""
        #SBATCH --mem={mem_per_job}

        lstchain_r0_to_r0g --R0-file={input_file} --output-dir={output_dir} --log={log_file} --no-flatfield-heuristic
        """
        )
        
    return sbatch_script


def launch_gainsel_for_data_run(run, output_dir, r0_dir, log_dir, log_file, tool, simulate=False):
    
    run_id = run["run_id"]
    ref_time = run["dragon_reference_time"]
    ref_counter = run["dragon_reference_counter"]
    module = run["dragon_reference_module_index"]
    ref_source = run["dragon_reference_source"].upper()

    files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.????.fits.fz")
    subrun_numbers = [int(file[-12:-8]) for file in files]
    
    if tool == "lst_dvr" and ref_source not in ["UCTS", "TIB"]:
        input_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")
        
        if not simulate:
            for file in input_files:
                log.info(
                    f"Run {run_id} does not have UCTS or TIB info, so gain selection cannot"
                    f"be applied. Copying directly the R0 files to {output_dir}."
                )
                sp.run(["cp", file, output_dir])

        else:
            log.info(
                f"Run {run_id} does not have UCTS or TIB info, so gain selection cannot"
                f"be applied. Simulate copy of the R0 files directly to {output_dir}."
            )

    else:
        n_subruns = max(subrun_numbers) 

        for subrun in range(n_subruns + 1):

            r0_files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.{subrun:04d}.fits.fz")

            if len(r0_files) != 4:
                if not simulate:
                    log.info(f"Run {run_id}.{subrun:05d} does not have 4 streams of R0 files, so gain"
                        f"selection cannot be applied. Copying directly the R0 files to {output_dir}."
                    )
                    for file in r0_files:
                        sp.run(["cp", file, output_dir])
                else:
                    log.info(f"Run {run_id}.{subrun:05d} does not have 4 streams of R0 files, so gain"
                        f"selection cannot be applied. Simulate copy of the R0 files directly to {output_dir}."
                    )

            else:
                history_file = log_dir / f"gain_selection_{run_id:05d}.{subrun:04d}.history"
                if history_file.exists():
                    if not simulate:
                        update_history_file(run, subrun, log_dir, history_file)

                    if history_file.read_text() == "":   # history_file is empty
                        log.info(f"Gain selection is still running for run {run_id:05d}.{subrun:04d}")
                        continue
                    else:
                        gainsel_rc = history_file.read_text().splitlines()[-1][-1]
                        if gainsel_rc == "1":
                            if job_finished_in_timeout(run, subrun, log_dir) and not simulate:
                                # Relaunch the job that finished in TIMEOUT
                                job_file = log_dir / f"gain_selection_{run_id:05d}.{subrun:04d}.sh"
                                sp.run(["sbatch", job_file], stdout=sp.PIPE, stderr=sp.STDOUT, check=True)
                            else:
                                log.warning(f"Gain selection failed for run {run_id:05d}.{subrun:04d}")
                        elif gainsel_rc == "0":
                            log.debug(f"Gain selection finished successfully for run {run_id:05d}.{subrun:04d},"
                                        "no additional jobs will be submitted for this subrun.") 
                else:
                    log.info("Creating and launching the sbatch scripts for the rest of the runs to apply gain selection")
                    if not simulate:
                        job_file = log_dir / f"gain_selection_{run_id:05d}.{subrun:04d}.sh"
                        r0_files.sort()
                        with open(job_file, "w") as f:
                            f.write(
                                get_sbatch_script(
                                    run_id,
                                    subrun,
                                    r0_files[0],
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

                        #submit job
                        history_file.touch()
                        sp.run(["sbatch", job_file], stdout=sp.PIPE, stderr=sp.STDOUT, check=True)


def apply_gain_selection(date: str, start: int, end: int, tool: str = None, no_queue_check: bool = False, simulate: bool = False):
    """
    Submit the jobs to apply the gain selection to the data for a given date
    on a subrun-by-subrun basis.
    """

    if not tool:
        if date < "20231205":
            tool = "lst_dvr"
        else:
            tool = "lstchain_r0_to_r0g"

    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
    summary_table = Table.read(run_summary_file)

    if len(summary_table) == 0:
        log.warning(f"No runs are found in the run summary of {date}. Nothing to do. Exiting.")
        sys.exit(0)

    # Apply gain selection only to DATA runs
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    log.info(f"Found {len(data_runs)} DATA runs to which apply the gain selection")

    base_dir = Path(cfg.get("LST1", "BASE"))
    output_dir = base_dir / f"R0G/{date}"
    log_dir = base_dir / f"R0G/log{date}"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"r0_to_r0g_{date}.log"
    r0_dir = base_dir / "R0" / date

    for run in data_runs:
        if not no_queue_check:
            # Check slurm queue status and sleep for a while to avoid overwhelming the queue
            check_job_status_and_wait(max_jobs=1500)

        # Avoid running jobs while it is still night time
        wait_for_daytime(start, end)

        launch_gainsel_for_data_run(run, output_dir, r0_dir, log_dir, log_file, tool, simulate)

    calib_runs = summary_table[summary_table["run_type"] != "DATA"]
    log.info(f"Found {len(calib_runs)} NO-DATA runs")

    for run in calib_runs:
        run_id = run["run_id"]
        log.info(f"Copying R0 files corresponding to run {run_id} directly to {output_dir}")

        if not simulate:
            # Avoid copying files while it is still night time
            wait_for_daytime(start, end)

            run_id = run["run_id"]
            r0_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

            for file in r0_files:
                sp.run(["cp", file, output_dir])


def get_last_job_id(run, subrun, log_dir):
    
    filenames = glob.glob(f"{log_dir}/gain_selection_{run:05d}_{subrun:04d}_*.log")
    match = re.search(f'gain_selection_{run:05d}_{subrun:04d}_(\d+).log', sorted(filenames)[-1])
    job_id = match.group(1)
    
    return job_id


def job_finished_in_timeout(run, subrun, log_dir):

    job_id = get_last_job_id(run, subrun, log_dir)
    job_status = get_sacct_output(run_sacct_j(job_id))["State"]
    if job_status == "TIMEOUT":
        return True
    else:
        return False

def update_history_file(run, subrun, log_dir, history_file):
    
    job_id = get_last_job_id(run, subrun, log_dir)
    job_status = get_sacct_output(run_sacct_j(job_id))["State"]
    if job_status in ["RUNNING", "PENDING"]:
        log.info(f"Job {job_id} is still running.")
        return
        
    elif job_status == "COMPLETED":
        log.info(f"Job {job_id} finished successfully, updating history file.")
        string_to_write = (
            f"{run:05d}.{subrun:04d} gain_selection 0\n"
        )
        append_to_file(history_file, string_to_write)
    
    else:
        log.info(f"Job {job_id} failed, updating history file.")
        string_to_write = (
            f"{run:05d}.{subrun:04d} gain_selection 1\n"
        )
        append_to_file(history_file, string_to_write)


def GainSel_flag_file(date: str) -> Path:
    filename = cfg.get("LSTOSA", "gain_selection_check")
    GainSel_dir = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
    flagfile = GainSel_dir / date / filename
    return flagfile.resolve()


def GainSel_finished(date: str) -> bool:
    """Check if gain selection finished successfully."""
    flagfile = GainSel_flag_file(date)
    return flagfile.exists()


def check_failed_jobs(date: str):
    """Search for failed jobs in the log directory."""
    base_dir = Path(cfg.get("LST1", "BASE"))
    log_dir = base_dir / f"R0G/log/{date}"
    history_files = glob.glob(f"{log_dir}/gain_selection_*.history")
    failed_subruns = []
    log.info(f"Checking history files of date {date}")

    for file in history_files:
        match = re.search("gain_selection_(\d+).(\d+).history", file)
        run = match.group(1)
        subrun = match.group(2)
        gainsel_rc = file.read_text().splitlines()[-1][-1]

        if gainsel_rc == "1":
            log.warning(f"Gain selection failed for run {run}.{subrun}")
            failed_subruns.append(file)

        elif gainsel_rc == "0":
            log.debug(f"Gain selection finished successfully for run {run}.{subrun}")
                
    if failed_subruns:
        log.warning(f"{date}: Some gain selection jobs did not finish successfully")
    else:
        log.info(f"{date}: All jobs finished successfully")

        run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
        run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
        summary_table = Table.read(run_summary_file)
        runs = summary_table["run_id"]
        missing_runs = []

        r0_files = glob.glob(base_dir / f"R0/{date}/LST-1.?.Run?????.????.fits.fz")
        r0g_files = glob.glob(base_dir f"R0G/{date}/LST-1.?.Run?????.????.fits.fz")
        all_r0_runs = [parse_r0_filename(i).run for i in r0_files]
        all_r0g_runs = [parse_r0_filename(i).run for i in r0g_files]

        for run in all_r0_runs:
            if run not in runs:
                if run not in all_r0g_runs:
                    missing_runs.append(run)

        missing_runs.sort()
        if missing_runs:
            output_dir = base_dir / f"R0G/{date}/"
            log.info(
                f"Some runs are missing. Copying R0 files of runs {pd.Series(missing_runs).unique()} "
                f"directly to {output_dir}"
            )

            for run in missing_runs:
                
                files = glob.glob(base_dir / f"R0/{date}/LST-1.?.Run{run:05d}.????.fits.fz")
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
            check_failed_jobs(args.date)
        else:
            log.info(f"Applying gain selection to date {args.date}")
            apply_gain_selection(
                args.date, 
                args.start_time, 
                args.end_time,
                args.tool,
                no_queue_check=args.no_queue_check, 
                simulate=args.simulate,
            )


    elif args.dates_file:
        list_of_dates = get_list_of_dates(args.dates_file)
        log.info(f"Found {len(list_of_dates)} dates to apply or check gain selection")

        if args.check:
            for date in list_of_dates:
                log.info(f"Checking gain selection status for date {date}")
                check_failed_jobs(date)
        else:
            for date in list_of_dates:
                log.info(f"Applying gain selection to date {date}")
                apply_gain_selection(
                    date, 
                    args.start_time, 
                    args.end_time,
                    args.tool,
                    no_queue_check=args.no_queue_check,
                    simulate=args.simulate,
                )
            log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
