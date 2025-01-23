import glob
import sys
import argparse
from pathlib import Path
from astropy.table import Table
import subprocess as sp

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import get_last_pedcalib
from osa.utils.cliopts import valid_date
from osa.job import run_sacct, get_sacct_output
from osa.utils.utils import date_to_dir, get_calib_filters
from osa.paths import catB_closed_file_exists

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
    "tel_id",
    choices=["ST", "LST1", "LST2", "all"],
    help="telescope identifier LST1, LST2, ST or all.",
)

def are_all_history_files_created(run_id: int) -> bool:
    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
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

    job_id = get_catB_last_job_id(run_id)
    if job_id:
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
        command = cfg.get("lstchain", "catB_calibration")
        options.filters = get_calib_filters(run_id) 
        base_dir = Path(cfg.get("LST1", "BASE")).resolve()
        r0_dir = Path(cfg.get("LST1", "R0_DIR")).resolve()
        interleaved_dir = Path(options.directory) / "interleaved"
        log_dir = Path(options.directory) / "log"
        catA_calib_run = get_last_pedcalib(options.date)
        slurm_account = cfg.get("SLURM", "ACCOUNT")
        cmd = ["sbatch", f"--account={slurm_account}", "--parsable",
            "-o", f"{log_dir}/catB_calibration_{run_id:05d}_%j.out",
            "-e", f"{log_dir}/catB_calibration_{run_id:05d}_%j.err",
            command,
            f"--run_number={run_id}",
            f"--catA_calibration_run={catA_calib_run}",
            f"--base_dir={base_dir}",
            f"--r0-dir={r0_dir}",
            f"--interleaved-dir={interleaved_dir}",
            f"--filters={options.filters}",
        ]
        job = sp.run(cmd, encoding="utf-8", capture_output=True, text=True, check=True)
        job_id = job.stdout.strip()
        log.debug(f"Launched Cat-B calibration job {job_id}!")


def launch_tailcuts_finder(run_id: int):
    command = "lstchain_find_tailcuts"  #cfg.get("lstchain", "tailcuts_finder")
    dl1ab_subdirectory = Path(options.directory) / options.dl1_prod_id
    output_dir = Path(options.directory)
    log_dir = Path(options.directory) / "log"
    log_file = log_dir / f"tailcuts_finder_{run_id:05d}_%j.log"
    cmd = [
        "sbatch", 
        f"--account={slurm_account}",
        command,
        f"--input-dir={dl1ab_subdirectory}",
        f"--run={run_id}",
        f"--output-dir={output_dir}",
        f"--log={log_file}",
    ]
    sp.run(cmd, check=True)


def tailcuts_config_file_exists(run_id: int) -> bool:
    tailcuts_config_file = Path(options.directory) / f"dl1ab_Run{run_id:05d}.json"
    return tailcuts_config_file.exists()
    
        
def main():

    opts = parser.parse_args()
    options.tel_id = opts.tel_id
    options.date = set_default_date_if_needed()
    options.configfile = opts.config.resolve()
    options.directory = analysis_path(options.tel_id)
    options.dl1_prod_id = get_dl1_prod_id()

    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    run_summary = Table.read(run_summary_dir / f"RunSummary_{date_to_dir(options.date)}.ecsv")
    data_runs = run_summary[run_summary["run_type"]=="DATA"]
    for run_id in data_runs["run_id"]:
        # first check if the dl1a files are produced
        if not r0_to_dl1_step_finished_for_run(run_id):
            log.info(f"The r0_to_dl1 step did not finish yet for run {run_id:05d}. Please try again later.")
        else:
            # launch catB calibration and tailcut finder in parallel
            if not catB_closed_file_exists(run_id):
                launch_catB_calibration(run_id)
            if not tailcuts_config_file_exists(run_id):
                launch_tailcuts_finder(run_id)


if __name__ == "__main__":
    main()