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
from datetime import datetime

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.utils import wait_for_daytime, date_to_dir, date_to_iso
from osa.utils.logging import myLogger
from osa.utils.iofile import append_to_file
from osa.utils.cliopts import valid_date
from osa.job import get_sacct_output, run_sacct, job_finished_in_timeout
from osa.configs.config import cfg
from osa.paths import DEFAULT_CFG
from osa.nightsummary.nightsummary import run_summary_table


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
        type=valid_date,
        help="Night to apply the gain selection in YYYY-MM-DD format",
)                                                                                                        
parser.add_argument(                                                                                     
        "-l",                                                                                            
        "--dates-file",
        default=None,
        help="List of dates to apply the gain selection. The input file should list"
        "the dates in the format YYYY-MM-DD, one date per line.",
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
        "previous to 2023-12-05) and lstchain_r0_to_r0g (by default used for dates later than 2023-12-05).",
)
parser.add_argument(                                                                                          
        "--simulate",
        action="store_true",
        default=False,
        help="Simulate launching of the gain selection script. Dry run.",
)
parser.add_argument(
        "-v",                                                                                      
        "--verbose",
        action="store_true",
        default=False,
        help="Activate debugging mode.",
)

def get_sbatch_script(
    run_id: str,
    subrun: str,
    input_file: Path,
    output_dir: Path,
    log_dir: Path,
    log_file: Path,
    ref_time: int,
    ref_counter: int,
    module: int,
    ref_source: str,
    tool: str
):
    """Build the sbatch job pilot script for running the gain selection."""
    mem_per_job = cfg.get("SLURM", "MEMSIZE_GAINSEL")
    slurm_account = cfg.get("SLURM", "ACCOUNT")
    sbatch_script = dedent(
            f"""\
        #!/bin/bash

        #SBATCH -D {log_dir}
        #SBATCH -o "gain_selection_{run_id:05d}_{subrun:04d}_%j.log"
        #SBATCH --job-name "gain_selection_{run_id:05d}"
        #SBATCH --partition=short,long
        #SBATCH --mem={mem_per_job}
        #SBATCH --account={slurm_account}
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
        cmd = f"lstchain_r0_to_r0g --R0-file={input_file} --output-dir={output_dir} --log={log_file}"
        if not cfg.getboolean("lstchain", "use_ff_heuristic_gain_selection"): 
            cmd += " --no-flatfield-heuristic"
        sbatch_script += dedent(cmd)

    return sbatch_script


def launch_gainsel_for_data_run(
    date: datetime, run: Table, output_dir: Path, r0_dir: Path, log_dir: Path, tool: str, simulate: bool = False
    ):
    """
    Create the gain selection sbatch script and launch it for a given run.
    
    Runs from before 20231205 without UCTS or TIB info are directly copied to the final directory.
    Subruns that do not have four streams are also directly copied.
    """
    run_id = run["run_id"]
    ref_time = run["dragon_reference_time"]
    ref_counter = run["dragon_reference_counter"]
    module = run["dragon_reference_module_index"]
    ref_source = run["dragon_reference_source"].upper()

    files = glob.glob(f"{r0_dir}/LST-1.?.Run{run_id:05d}.????.fits.fz")
    subrun_numbers = [int(file[-12:-8]) for file in files]
    
    if tool == "lst_dvr" and ref_source not in ["UCTS", "TIB"]:
        input_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")
        
        if is_run_already_copied(date, run_id):
            log.info(f"The R0 files corresponding to run {run_id} have already been copied to the R0G directory.")
        else:
            if not simulate:
                for file in input_files:
                    log.debug(
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
                if not simulate and not is_run_already_copied(date, run_id):
                    log.debug(f"Run {run_id:05d}.{subrun:04d} does not have 4 streams of R0 files, so gain"
                        f"selection cannot be applied. Copying directly the R0 files to {output_dir}.")
                    for file in r0_files:
                        sp.run(["cp", file, output_dir])
                elif is_run_already_copied(date, run_id):
                    log.debug(f"Run {run_id:05d}.{subrun:04d} does not have 4 streams of R0 files. The R0 files"
                        f"have already been copied to {output_dir}.")
                elif simulate:
                    log.debug(f"Run {run_id:05d}.{subrun:04d} does not have 4 streams of R0 files, so gain"
                        f"selection cannot be applied. Simulate copy of the R0 files directly to {output_dir}.")

            else:
                history_file = log_dir / f"gain_selection_{run_id:05d}.{subrun:04d}.history"
                if history_file.exists():
                    if not simulate:
                        update_history_file(run_id, subrun, log_dir, history_file)

                    if history_file.read_text() == "":   # history_file is empty
                        log.debug(f"Gain selection is still running for run {run_id:05d}.{subrun:04d}")
                        continue
                    else:
                        gainsel_rc = history_file.read_text().splitlines()[-1][-1]
                        if gainsel_rc == "1":
                            job_id = get_last_job_id(run_id, subrun, log_dir)
                            if job_finished_in_timeout(job_id) and not simulate:
                                # Relaunch the job that finished in TIMEOUT
                                job_file = log_dir / f"gain_selection_{run_id:05d}.{subrun:04d}.sh"
                                sp.run(["sbatch", job_file], stdout=sp.PIPE, stderr=sp.STDOUT, check=True)
                            else:
                                log.warning(f"Gain selection failed for run {run_id:05d}.{subrun:04d}")
                        elif gainsel_rc == "0":
                            log.debug(f"Gain selection finished successfully for run {run_id:05d}.{subrun:04d},"
                                        "no additional jobs will be submitted for this subrun.") 
                else:
                    log.debug("Creating and launching the gain selection sbatch script for subrun {run_id:05d}.{subrun:04d}")
                    if not simulate:
                        log_file = log_dir / f"r0_to_r0g_{run_id:05d}.{subrun:04d}.log"
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


def apply_gain_selection(date: datetime, start: int, end: int, tool: str = None, no_queue_check: bool = False, simulate: bool = False):
    """
    Submit the jobs to apply the gain selection to the data for a given date
    on a subrun-by-subrun basis.
    """

    if not tool:
        if date_to_dir(date) < "20231205":
            tool = "lst_dvr"
        else:
            tool = "lstchain_r0_to_r0g"

    summary_table = run_summary_table(date)

    if len(summary_table) == 0:
        log.warning(f"No runs are found in the run summary of {date_to_iso(date)}. Nothing to do. Exiting.")
        sys.exit(0)

    # Apply gain selection only to DATA runs
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    log.info(f"Found {len(data_runs)} DATA runs to which apply the gain selection")

    date_str = date_to_dir(date)
    r0_dir = Path(cfg.get("LST1", "RAW_R0_DIR")) / date_str
    output_dir = Path(cfg.get("LST1", "R0_DIR")) / date_str
    log_dir = Path(cfg.get("LST1", "R0_DIR")) / f"log/{date_str}"
    if not simulate:
        output_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

    for run in data_runs:
        if not no_queue_check:
            # Check slurm queue status and sleep for a while to avoid overwhelming the queue
            check_job_status_and_wait(max_jobs=1500)

        # Avoid running jobs while it is still night time
        wait_for_daytime(start, end)

        if not is_closed(date, run["run_id"]):
            launch_gainsel_for_data_run(date, run, output_dir, r0_dir, log_dir, tool, simulate)

    calib_runs = summary_table[summary_table["run_type"] != "DATA"]
    log.info(f"Found {len(calib_runs)} NO-DATA runs")

    for run in calib_runs:
        run_id = run["run_id"]
        
        if is_run_already_copied(date, run_id):
            log.info(f"The R0 files corresponding to run {run_id:05d} have already been copied, nothing to do.")
        else:
            log.info(f"Copying R0 files corresponding to run {run_id} directly to {output_dir}")
       	    if not simulate:
            	# Avoid copying files while it is still night time
            	wait_for_daytime(start, end)

            	r0_files = r0_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

            	for file in r0_files:
                    sp.run(["cp", file, output_dir])


def get_last_job_id(run_id: str, subrun: str, log_dir: Path) -> int:
    """Get job id of the last gain selection job that was launched for a given subrun."""
    filenames = glob.glob(f"{log_dir}/gain_selection_{run_id:05d}_{subrun:04d}_*.log")
    if filenames:
        match = re.search(f'gain_selection_{run_id:05d}_{subrun:04d}_(\d+).log', sorted(filenames)[-1])
        job_id = match.group(1)
        return job_id


def update_history_file(run_id: str, subrun: str, log_dir: Path, history_file: Path):
    """
    Update the gain selection history file with the result 
    of the last job launched for a given subrun.
    """
    job_id = get_last_job_id(run_id, subrun, log_dir)
    if not job_id:
        log.debug(f"Cannot find a job_id for the run {run_id:05d}.{subrun:04d}")
    else:
        job_status = get_sacct_output(run_sacct(job_id=job_id))["State"]
        if job_status.item() in ["RUNNING", "PENDING"]:
            log.info(f"Job {job_id} is still running.")
            return
            
        elif job_status.item() == "COMPLETED":
            log.debug(f"Job {job_id} finished successfully, updating history file.")
            string_to_write = (
                f"{run_id:05d}.{subrun:04d} gain_selection 0\n"
            )
            append_to_file(history_file, string_to_write)
        
        else:
            log.info(f"Job {job_id} failed, updating history file.")
            string_to_write = (
                f"{run_id:05d}.{subrun:04d} gain_selection 1\n"
            )
            append_to_file(history_file, string_to_write)


def is_run_already_copied(date: datetime, run_id: int) -> bool:
    """Check if the R0 files of a given run have already been copied to the R0G directory."""
    r0_dir = Path(cfg.get("LST1", "RAW_R0_DIR"))
    r0g_dir = Path(cfg.get("LST1", "R0_DIR"))
    r0_files = glob.glob(f"{r0_dir}/{date_to_dir(date)}/LST-1.?.Run{run_id:05d}.????.fits.fz")
    r0g_files = glob.glob(f"{r0g_dir}/{date_to_dir(date)}/LST-1.?.Run{run_id:05d}.????.fits.fz")
    return len(r0_files)==len(r0g_files)


def is_closed(date: datetime, run_id: str) -> bool:
    """Check if run is already closed."""
    base_dir = Path(cfg.get("LST1", "BASE"))
    log_dir = base_dir / f"R0G/log/{date_to_dir(date)}"
    closed_run_file = log_dir / f"gain_selection_{run_id:05d}.closed"
    return closed_run_file.exists()


def GainSel_flag_file(date: datetime) -> Path:
    """Return the path to the file indicating the completion of the gain selection stage."""
    filename = cfg.get("LSTOSA", "gain_selection_check")
    GainSel_dir = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
    flagfile = GainSel_dir / date_to_dir(date) / filename
    return flagfile.resolve()


def GainSel_finished(date: datetime) -> bool:
    """Check if gain selection finished successfully."""
    flagfile = GainSel_flag_file(date)
    return flagfile.exists()
   

def check_gainsel_jobs_runwise(date: datetime, run_id: int) -> bool:
    """Search for failed jobs in the log directory."""
    base_dir = Path(cfg.get("LST1", "BASE"))
    log_dir = base_dir / f"R0G/log/{date_to_dir(date)}"
    history_files = list(log_dir.glob(f"gain_selection_{run_id:05d}.????.history"))
    summary_table = run_summary_table(date)
    n_subruns = summary_table[summary_table["run_id"] == run_id]["n_subruns"]
    
    if len(history_files) != n_subruns:
        log.debug(f"All history files of run {run_id} were not created yet")
        return False

    failed_subruns = []
    log.info(f"Checking all history files of run {run_id}")
    
    for file in history_files:
        match = re.search(f"gain_selection_{run_id:05d}.(\d+).history", str(file))
        subrun = match.group(1)
        if file.read_text() != "":
            gainsel_rc = file.read_text().splitlines()[-1][-1]

            if gainsel_rc == "1":
                log.warning(f"Gain selection failed for run {run_id}.{subrun}")
                failed_subruns.append(file)

            elif gainsel_rc == "0":
                log.debug(f"Gain selection finished successfully for run {run_id}.{subrun}")
        else:
            log.info(f"Gain selection is still running for run {run_id}.{subrun}")
            return False
            
    if failed_subruns:
        log.warning(f"{date_to_iso(date)}: Some gain selection jobs did not finish successfully for run {run_id}")
        return False
    else:
        log.info(f"{date_to_iso(date)}: All jobs finished successfully for run {run_id}, creating the corresponding .closed file")
        closed_run_file = log_dir / f"gain_selection_{run_id:05d}.closed"
        closed_run_file.touch()
        return True


def check_warnings_in_logs(date: datetime, run_id: int):
    """Look for warnings in the log files created by lstchain_r0_to_r0g."""
    base_dir = Path(cfg.get("LST1", "BASE"))
    log_dir = base_dir / f"R0G/log/{date_to_dir(date)}"
    log_files = log_dir.glob(f"r0_to_r0g_{run_id:05d}.*.log")
    for file in log_files:
        content = file.read_text().splitlines()
        for line in content:
            if "FlatField(FF)-like events are not tagged as FF" in line:
                log.warning(f"Warning for run {run_id}: {line}")


def check_failed_jobs(date: datetime):
    """Search for failed jobs in the log directory."""

    summary_table = run_summary_table(date)

    if len(summary_table) == 0:
        log.warning(f"No runs are found in the run summary of {date_to_iso(date)}. Nothing to do. Exiting.")
        sys.exit(0)
        
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    failed_runs = []

    for run in data_runs:
        run_id = run["run_id"]
        check_warnings_in_logs(date, run_id)
        if not is_closed(date, run_id):
            if not check_gainsel_jobs_runwise(date, run_id):
                log.warning(f"Gain selection did not finish successfully for run {run_id}.")
                failed_runs.append(run)

    if failed_runs:
        log.warning(f"Gain selection did not finish successfully for {date_to_iso(date)}, cannot create the flag file.")
        return

    runs = summary_table["run_id"]
    missing_runs = []

    date_str = date_to_dir(date)
    r0_dir = Path(cfg.get("LST1", "RAW_R0_DIR")) / date_str
    r0g_dir = Path(cfg.get("LST1", "R0_DIR")) / date_str
    r0_files = glob.glob(f"{r0_dir}/LST-1.?.Run?????.????.fits.fz")
    r0g_files = glob.glob(f"{r0g_dir}/LST-1.?.Run?????.????.fits.fz")
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
            f"directly to {r0g_dir}"
        )

        for run in missing_runs:
            files = r0_dir.glob(f"LST-1.?.Run{run:05d}.????.fits.fz")
            for file in files:
                sp.run(["cp", file, r0g_dir])

    GainSel_dir = Path(cfg.get("LST1", "GAIN_SELECTION_FLAG_DIR"))
    flagfile_dir = GainSel_dir / date_str
    flagfile_dir.mkdir(parents=True, exist_ok=True)
    
    flagfile = GainSel_flag_file(date)
    log.info(f"Gain selection finished successfully, creating flag file for date {date_to_iso(date)} ({flagfile})")
    flagfile.touch()


def main():
    """
    Loop over the dates listed in the input file and launch the gain selection
    script for each of them. The input file should list the dates in the format
    YYYYMMDD one date per line.
    """
    args = parser.parse_args()
    
    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    if args.date:
        if GainSel_finished(args.date):
            log.warning(f"Gain selection already done for date {date_to_iso(args.date)}. Exiting.")
            sys.exit(0)
        elif args.check:
            log.info(f"Checking gain selection status for date {date_to_iso(args.date)}")
            check_failed_jobs(args.date)
        else:
            log.info(f"\nApplying gain selection to date {date_to_iso(args.date)}")
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
