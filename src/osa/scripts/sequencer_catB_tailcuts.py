import glob
import re
from argparse import ArgumentParser
import logging
from pathlib import Path
from astropy.table import Table
import subprocess as sp

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import get_last_pedcalib
from osa.utils.cliopts import common_parser, set_default_date_if_needed
from osa.utils.logging import myLogger
from osa.job import run_sacct, get_sacct_output
from osa.utils.utils import date_to_dir, get_calib_filters, get_lstchain_version
from osa.paths import (
    catB_closed_file_exists,
    catB_calibration_file_exists,
    analysis_path,
    get_major_version
)

import re
from pathlib import Path

# calibration table


TABLE_TEXT = """
20260522 1779438493227 20260512 (r24145) 20260617 (r24552) 20250911 (r21747) 20260521 (r24229) 20260521 (r24229) since 20260617 (r24552)
20260522 1779438493227 20260512 (r24145) 20260521 (r24229) 20250911 (r21747) 20260521 (r24229) 20260521 (r24229) since 20260521 (r24229)
20260513 1778660458817 20260512 (r24145) 20251111 (r22837) 20250911 (r21747) 20250326 (r20529)
20260416 1776350768431 20260415 (r24107) 20251111 (r22837) 20250911 (r21747) 20250326 (r20529)
"""

def parse_calibration_table(table_text):
    periods = []

    for line in table_text.splitlines():
        if "since" not in line:
            continue

        match = re.search(r"since\s+(\d{8})\s+\(r(\d+)\)", line)
        if not match:
            continue

        since_run = int(match.group(2))

        calib_matches = re.findall(r"(\d{8})\s+\(r(\d+)\)", line)

        if len(calib_matches) < 3:
            continue

        # calibration
        calib_date, calibration_run = calib_matches[1]

        # ffactor = penultimate column
        ffactor_date, ffactor_run = calib_matches[-2]

        periods.append({
            "since_run": since_run,
            "calib_date": calib_date,
            "calibration_run": int(calibration_run),
            "ffactor_date": ffactor_date,
            "ffactor_run": int(ffactor_run),
        })

    return sorted(periods, key=lambda x: x["since_run"], reverse=True)

def find_period_for_run(run_id, periods):
    for p in periods:
        if run_id >= p["since_run"]:
            return p

    print(f"[WARNING] Run {run_id} prior to the first period, using fallback")
    return periods[-1]



# Paths

BASE_SERVICE = Path("/fefs/onsite/data/lst-pipe/LSTN-01/service/PixelCalibration/Cat-A")


def find_catA_file(calib_date, calibration_run):
    path = BASE_SERVICE / "calibration" / calib_date / "pro"
    files = list(path.glob(f"*Run{calibration_run:05d}*.fits*"))

    if not files:
        raise RuntimeError(f"No Cat-A file para run {calibration_run} en {path}")

    return str(sorted(files)[0])


def find_systematics_file(calib_date):
    path = BASE_SERVICE / "ffactor_systematics" / calib_date / "v0.3.1"
    files = list(path.glob("scan_fit*.h5"))

    if not files:
        raise RuntimeError(f"No systematics for date {calib_date} in {path}")

    return str(sorted(files)[0])


def get_catA_and_systematics(run_id):
    periods = parse_calibration_table(TABLE_TEXT)
    period = find_period_for_run(run_id, periods)

    calib_date = period["calib_date"]
    calibration_run = period["calibration_run"]
    ffactor_date = period["ffactor_date"]

    catA_file = find_catA_file(calib_date, calibration_run)
    systematics_file = find_systematics_file(ffactor_date)


    return catA_file, systematics_file



log = myLogger(logging.getLogger())

parser = ArgumentParser(parents=[common_parser])
parser.add_argument(
    "--overwrite-tailcuts",
    action="store_true",
    default=False,
    help="Overwrite the tailcuts config file if it already exists.",
)
parser.add_argument(
    "--input-state",
    choices=["legacy_raw", "gain_selected", "catA_calibrated"],
    default="legacy_raw",
    help="Declared preprocessing state of input data",
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


def launch_catB_calibration(run_id: int):
    """
    Launch the Cat-B calibration script for a given run if the Cat-B calibration 
    file has not been created yet. If the Cat-B calibration script was launched
    before and it finished successfully, it creates a catB_{run}.closed file.
    """
    job_id = get_catB_last_job_id(run_id)
    if job_id and not options.overwrite_catB:
        job_status = get_sacct_output(run_sacct(job_id=job_id))["State"]
        if job_status.item() in ["RUNNING", "PENDING"]:
            log.debug(f"Job {job_id} (corresponding to run {run_id:05d}) is still running.")

        elif job_status.item() == "COMPLETED":
            catB_closed_file = Path(options.directory) / f"catB_{run_id:05d}.closed"
            catB_closed_file.touch()
            log.debug(
                f"Cat-B job {job_id} (corresponding to run {run_id:05d}) finished "
                f"successfully. Creating file {catB_closed_file}"
            )

        else: 
            log.warning(f"Cat-B job {job_id} (corresponding to run {run_id:05d}) failed.")

    else:
        if catB_calibration_file_exists(run_id):
            if not options.overwrite_catB:
                log.info(f"Cat-B calibration file already produced for run {run_id:05d}.")
                return 
            else:
                log.info(f"Cat-B calibration file already produced for run {run_id:05d}. Overwriting it.")

        command = cfg.get("lstchain", "catB_calibration")
        if cfg.getboolean("lstchain", "use_lstcam_env_for_CatB_calib"):
            env_command = f"conda run -n lstcam-env {command}"
        else:
            env_command = command
        options.filters = get_calib_filters(run_id) 
        base_dir = Path(cfg.get(options.tel_id, "BASE")).resolve()
        r0_dir = Path(cfg.get(options.tel_id, "R0_DIR")).resolve()
        log_dir = Path(options.directory) / "log"
        input_state = getattr(options, "input_state", "legacy_raw")
        if input_state == "catA_calibrated":
            catA_file, systematics_file = get_catA_and_systematics(run_id)
            log.info(f"[CatB] Using Cat-A file: {catA_file}")
            log.info(f"[CatB] Using systematics: {systematics_file}")
        else:
            catA_calib_run = get_last_pedcalib(options.date)
        slurm_account = cfg.get("SLURM", "ACCOUNT")
        lstchain_version = get_major_version(get_lstchain_version())
        analysis_dir = cfg.get("LST1", "ANALYSIS_DIR")
        cmd = ["sbatch", f"--account={slurm_account}", "--parsable",
            "-o", f"{log_dir}/catB_calibration_{run_id:05d}_%j.out",
            "-e", f"{log_dir}/catB_calibration_{run_id:05d}_%j.err",
            env_command,
            f"-r {run_id:05d}",
            "-b", base_dir,
            f"--r0-dir={r0_dir}",
            f"--filters={options.filters}",
        ]

        if input_state == "catA_calibrated":
            cmd.extend([
                f"--cat_A_calibration_file={catA_file}",
                f"--systematics_file={systematics_file}",
            ])
        else:
            cmd.append(f"--catA_calibration_run={catA_calib_run}")
        
        if command=="onsite_create_cat_B_calibration_file":
            cmd.append(f"--interleaved-dir={analysis_dir}")
        elif command=="lstcam_calib_onsite_create_cat_B_calibration_file":
            cmd.append(f"--dl1-dir={analysis_dir}")
            cmd.append(f"--lstchain-version={lstchain_version[1:]}")

        if options.overwrite_catB:
            cmd.append("--yes")

        if not options.simulate:
            job = sp.run(cmd, encoding="utf-8", capture_output=True, text=True, check=True)
            job_id = job.stdout.strip()
            log.debug(f"Launched Cat-B calibration job {job_id} for run {run_id}!")

        else: 
            log.info(f"Simulate launching of the {command} script.")
            

def launch_tailcuts_finder(run_id: int):
    """
    Launch the lstchain script to calculate the correct
    tailcuts to use for a given run. 
    """
    command = cfg.get("lstchain", "tailcuts_finder")
    slurm_account = cfg.get("SLURM", "ACCOUNT")
    input_dir = Path(options.directory)
    output_dir = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR"))
    log_dir = Path(options.directory) / "log"
    log_file = log_dir / f"tailcuts_finder_{run_id:05d}_%j.log"
    cmd = [
        "sbatch", "--parsable",
        f"--account={slurm_account}",
        "--mem-per-cpu=10GB",
        "-o", log_file,
        command,
        f"--input-dir={input_dir}",
        f"--run={run_id}",
        f"--output-dir={output_dir}",
    ]
    if not options.simulate:
        job = sp.run(cmd, encoding="utf-8", capture_output=True, text=True, check=True)
        job_id = job.stdout.strip()
        log.debug(f"Launched lstchain_find_tailcuts job {job_id} for run {run_id}!")

    else: 
        log.info(f"Simulate launching of the {command} script.")



def tailcuts_config_file_exists(run_id: int) -> bool:
    """Check if the config file created by the tailcuts finder script already exists."""
    tailcuts_config_file = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
    return tailcuts_config_file.exists()
    
        
def main():
    """
    Main script to be called as cron job. It launches the Cat-B calibration script 
    and the tailcuts finder script for each run of the corresponding date, and creates
    the catB_{run}.closed files if Cat-B calibration has finished successfully.
    """ 
    opts = parser.parse_args()
    options.input_state = opts.input_state
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
            # launch catB calibration and tailcut finder in parallel
            if cfg.getboolean("lstchain", "apply_catB_calibration") and not catB_closed_file_exists(run_id):
                launch_catB_calibration(run_id)
            if not cfg.getboolean("lstchain", "apply_standard_dl1b_config"):
                if tailcuts_config_file_exists(run_id) and not options.overwrite_tailcuts:
                    log.debug(
                        f"Tailcuts config file already exists for run {run_id:05d}. Use --overwrite-tailcuts to overwrite it."
                    )
                else:
                    launch_tailcuts_finder(run_id)


if __name__ == "__main__":
    main()
