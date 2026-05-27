"""
Orchestrator script that submits run-wise SLURM jobs to produce Cat-B calibration
and tailcuts configs once the DL1A step has finished.
"""

import glob
import re
import argparse
import logging
from pathlib import Path
from astropy.table import Table
import subprocess as sp

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import valid_date, set_default_date_if_needed
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir, date_to_iso
from osa.job import run_sacct, get_sacct_output
from osa.paths import (
    catB_closed_file_exists,
    catB_calibration_file_exists,
    analysis_path,
)

log = myLogger(logging.getLogger())

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",                                                                                            
    "--config",                                                                                      
    action="store",
    type=Path,
    help="Configuration file",
)
parser.add_argument(                                                                                     
    "-d",                                                                                            
    "--date",
    type=valid_date,
    default=None,
)
parser.add_argument(
    "-v",                                                                                      
    "--verbose",
    action="store_true",
    default=False,
    help="Activate debugging mode.",
)
parser.add_argument(
    "-s",
    "--simulate",
    action="store_true",
    default=False,
    help="Simulate launching of the sequencer_catB_tailcuts script.",
)
parser.add_argument(
    "--overwrite-tailcuts",
    action="store_true",
    default=False,
    help="Overwrite the tailcuts config file if it already exists.",
)
parser.add_argument(
    "--overwrite-catB",
    action="store_true",
    default=False,
    help="Overwrite the Cat-B calibration files if they already exist.",
)
parser.add_argument(
    "tel_id",
    choices=["ST", "LST1", "LST2", "all"],
    help="telescope identifier LST1, LST2, ST or all.",
)

def tailcuts_config_file_exists(run_id: int) -> bool:
    """Check if the config file created by the tailcuts finder script already exists."""
    tailcuts_config_file = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
    return tailcuts_config_file.exists()


def pilot_job_is_active(run_id: int) -> bool:
    """
    Return True if the last submitted pilot job for this run is still RUNNING/PENDING.
    This avoids resubmitting the same work if the cron runs again.
    """
    jobid_path = Path(options.directory) / "log" / f"catb_tailcuts_{options.tel_id}_{run_id:05d}.jobid"
    if not jobid_path.exists():
        return False

    job_id = jobid_path.read_text().strip()
    if not job_id:
        return False

    state_series = get_sacct_output(run_sacct(job_id=job_id))["State"]
    state = str(state_series.iloc[0])
    return state in ("RUNNING", "PENDING")


def write_pilot_script(run_id: int) -> Path:
    """
    Create a pilot script analogous to sequencer's pilots:
    a python file with #SBATCH header that just calls a worker script.
    """
    log_dir = Path(options.directory) / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    job_name = f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
    account = cfg.get("SLURM", "ACCOUNT")

    # Worker will run the actual commands (no sbatch inside)
    worker_argv = [
        "catb_tailcuts_pipeline",
        f"--date={date_to_iso(options.date)}",
    ]

    if options.verbose:
        worker_argv.append("--verbose")
    if options.simulate:
        worker_argv.append("--simulate")
    if options.configfile:
        worker_argv.extend(["--config", str(Path(options.configfile).resolve())])
    if options.overwrite_catB:
        worker_argv.append("--overwrite-catB")
    if options.overwrite_tailcuts:
        worker_argv.append("--overwrite-tailcuts")

    # Positional args at the end (like datasequence)
    worker_argv.append(f"{run_id:d}")
    worker_argv.append(f"{options.tel_id}")

    content = ""
    content += "#!/usr/bin/env python\n\n"
    content += f"#SBATCH --job-name={job_name}\n"
    content += f"#SBATCH --chdir={options.directory}\n"
    content += f"#SBATCH --output=log/{job_name}_%j.out\n"
    content += f"#SBATCH --error=log/{job_name}_%j.err\n"
    content += f"#SBATCH --account={account}\n\n"
    content += "import subprocess\n"
    content += "import sys\n\n"

    content += "proc = subprocess.run([\n"
    for arg in worker_argv:
        content += f"    {arg!r},\n"
    content += "])\n"
    content += "sys.exit(proc.returncode)\n"

    path = Path(options.directory) / f"sequence_{options.tel_id}_{run_id:05d}_catb_tailcuts.py"
    path.write_text(content)
    return path


def submit_pilot_script(run_id: int) -> None:
    script = write_pilot_script(run_id)
    if options.simulate:
        log.info(f"Simulate: would submit {script}")
        return
    out = sp.check_output(["sbatch", "--parsable", str(script)], universal_newlines=True, shell=False).split()[0]
    jobid_path = Path(options.directory) / "log" / f"catb_tailcuts_{options.tel_id}_{run_id:05d}.jobid"
    jobid_path.write_text(out + "\n")
    log.info(f"Submitted {script.name}: jobid {out}")


def are_all_history_files_created(run_id: int) -> bool:
    """Check if all the history files (one per subrun) were created for a given run."""
    run_summary_dir = Path(cfg.get(options.tel_id, "RUN_SUMMARY_DIR"))
    run_summary_file = run_summary_dir / f"RunSummary_{date_to_dir(options.date)}.ecsv"
    run_summary = Table.read(run_summary_file)
    n_subruns = run_summary[run_summary["run_id"] == run_id]["n_subruns"]
    analysis_dir = Path(options.directory)
    history_files = glob.glob(f"{str(analysis_dir)}/sequence_LST1_{run_id:05d}.????.history")
    if len(history_files) == n_subruns:
        return True
    else:
        return False


def r0_to_dl1_step_finished_for_run(run_id: int) -> bool:
    """
    Check if the step r0_to_dl1 finished successfully 
    for a given run by looking the history files.
    """
    if not are_all_history_files_created(run_id):
        log.debug(f"All history files for run {run_id:05d} were not created yet.")
        return False
    analysis_dir = Path(options.directory)
    history_files = glob.glob(f"{str(analysis_dir)}/sequence_LST1_{run_id:05d}.????.history")
    for file in history_files:
        rc = Path(file).read_text().splitlines()[-1][-1]
        if rc != "0":
            print(f"r0_to_dl1 step did not finish successfully (check file {file})")
            return False
    return True


def get_catB_last_job_id(run_id: int) -> int:
    """Get job id of the last Cat-B calibration job that was launched for a given run."""
    log_dir = Path(options.directory) / "log"
    filenames = glob.glob(f"{log_dir}/catB_calibration_{run_id:05d}_*.err")
    if filenames:
        match = re.search(f"catB_calibration_{run_id:05d}_(\d+).err", sorted(filenames)[-1])
        job_id = match.group(1)
        return job_id


def main():
    """
    Main script to be called as cron job. It launches the Cat-B calibration script 
    and the tailcuts finder script for each run of the corresponding date, and creates
    the catB_{run}.closed files if Cat-B calibration has finished successfully.
    """ 
    opts = parser.parse_args()
    options.tel_id = opts.tel_id
    options.simulate = opts.simulate
    options.overwrite_tailcuts = opts.overwrite_tailcuts
    options.overwrite_catB = opts.overwrite_catB
    options.date = opts.date
    options.date = set_default_date_if_needed()
    options.configfile = opts.config.resolve()
    options.directory = analysis_path(options.tel_id)

    if opts.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    run_summary_dir = Path(cfg.get(options.tel_id, "RUN_SUMMARY_DIR"))
    run_summary = Table.read(run_summary_dir / f"RunSummary_{date_to_dir(options.date)}.ecsv")
    data_runs = run_summary[run_summary["run_type"]=="DATA"]
    for run_id in data_runs["run_id"]:
        # first check if the dl1a files are produced
        if not r0_to_dl1_step_finished_for_run(run_id):
            log.info(f"The r0_to_dl1 step did not finish yet for run {run_id:05d}. Please try again later.")
        else:
            # Avoid duplicate submissions if last pilot is still active
            if pilot_job_is_active(run_id):
                log.info(f"Pilot job already running/pending for run {run_id:05d}, skipping resubmission.")
                continue

            # If there is something to do (catB and/or tailcuts), submit a pilot script.
            # The worker will do the actual processing.
            need_catb = cfg.getboolean("lstchain", "apply_catB_calibration") and (
                options.overwrite_catB or (not catB_closed_file_exists(run_id) and not catB_calibration_file_exists(run_id))
            )
            need_tailcuts = (not cfg.getboolean("lstchain", "apply_standard_dl1b_config")) and (
                options.overwrite_tailcuts or (not tailcuts_config_file_exists(run_id))
            )

            if need_catb or need_tailcuts:
                submit_pilot_script(run_id)
            else:
                log.debug(f"Nothing to do for run {run_id:05d} (CatB/tailcuts already done).")


if __name__ == "__main__":
    main()
