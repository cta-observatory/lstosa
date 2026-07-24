import glob
import re
from argparse import ArgumentParser
import logging
from pathlib import Path
from astropy.table import Table
import subprocess as sp
from datetime import datetime

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import get_last_pedcalib
from osa.utils.cliopts import common_parser, set_default_date_if_needed
from osa.utils.logging import myLogger
from osa.job import run_sacct, get_sacct_output
from osa.utils.utils import date_to_dir, get_calib_filters, get_lstchain_version
from osa.utils.utils import date_to_iso
from osa.paths import (
    catB_closed_file_exists,
    catB_calibration_file_exists,
    analysis_path,
    get_major_version
)

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

def create_run_history_file(run_id: int) -> Path:
    """Create the run-level history file if it does not exist."""

    history_file = (
        Path(options.directory)
        / f"{options.tel_id}_{run_id:05d}.history"
    )

    if history_file.exists():
        return history_file

    version = get_major_version(
        get_lstchain_version()
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M"
    )

    history_file.write_text(
        f"{run_id:05d} "
        f"lstchain_data_r0_to_dl1 "
        f"{version} "
        f"{timestamp} "
        f"None "
        f"all_subruns_finished "
        f"0\n"
    )

    return history_file


def tailcuts_config_file_exists(run_id: int) -> bool:
    """Check if the config file created by the tailcuts finder script already exists."""
    tailcuts_config_file = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
    return tailcuts_config_file.exists()


def write_pilot_script(run_id: int) -> Path:
    """
    Create a pilot script analogous to sequencer pilots.
    """

    log_dir = Path(options.directory) / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    job_name = (
        f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
    )

    account = cfg.get("SLURM", "ACCOUNT")

    worker_argv = [
        "catb_tailcuts_pipeline",
        f"--date={date_to_iso(options.date)}",
        f"--input-state={options.input_state}",
    ]

    if options.verbose:
        worker_argv.append("--verbose")

    if options.simulate:
        worker_argv.append("--simulate")

    if options.configfile:
        worker_argv.extend(
            [
                "--config",
                str(Path(options.configfile).resolve()),
            ]
        )

    if options.overwrite_catB:
        worker_argv.append("--overwrite-catB")

    if options.overwrite_tailcuts:
        worker_argv.append("--overwrite-tailcuts")

    worker_argv.append(str(run_id))
    worker_argv.append(options.tel_id)

    content = ""
    content += "#!/usr/bin/env python3\n\n"

    content += f"#SBATCH --job-name={job_name}\n"
    content += f"#SBATCH --chdir={options.directory}\n"
    content += (
        f"#SBATCH --output=log/{job_name}_%j.out\n"
    )
    content += (
        f"#SBATCH --error=log/{job_name}_%j.err\n"
    )
    content += (
        f"#SBATCH --account={account}\n\n"
    )
    content += "#SBATCH --mem=12G\n\n"

    content += "import subprocess\n"
    content += "import sys\n\n"

    content += "proc = subprocess.run([\n"

    for arg in worker_argv:
        content += f"    {arg!r},\n"

    content += "])\n"
    content += "sys.exit(proc.returncode)\n"

    pilot_script = (
        Path(options.directory)
        / (
            f"sequence_"
            f"{options.tel_id}_"
            f"{run_id:05d}_"
            f"catb_tailcuts.py"
        )
    )

    pilot_script.write_text(content)
    pilot_script.chmod(0o755)

    return pilot_script

def submit_pilot_script(run_id: int) -> str | None:

    pilot_script = write_pilot_script(run_id)

    cmd = [
        "sbatch",
        "--parsable",
        str(pilot_script),
    ]

    if options.simulate:

        log.info(
            f"Would submit {' '.join(cmd)}"
        )

        return None

    job = sp.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        text=True,
        check=True,
    )

    job_id = job.stdout.strip()

    log.info(
        f"Submitted CatB pipeline for run "
        f"{run_id:05d} ({job_id})"
    )

    return job_id


def pilot_job_is_active(run_id: int) -> bool:

    log_dir = Path(options.directory) / "log"

    pattern = rf"{options.tel_id}_catB_tailcuts_{run_id:05d}_(\d+)\.err$"
    files = sorted(
        glob.glob(str(log_dir / f"{options.tel_id}_catB_tailcuts_{run_id:05d}_*.err")),
        key=lambda p: int(re.search(pattern, p).group(1)) if re.search(pattern, p) else -1,
    )

    if not files:
        return False

    match = re.search(
        (
            rf"{options.tel_id}"
            rf"_catB_tailcuts_"
            rf"{run_id:05d}"
            rf"_(\d+)\.err"
        ),
        files[-1],
    )

    if match is None:
        return False

    job_id = match.group(1)

    try:

        state = get_sacct_output(
            run_sacct(job_id=job_id)
        )["State"].item()

    except Exception as e:
        log.warning(
            f"Could not query sacct for job {job_id} (run {run_id:05d}): {e}. "
            "Assuming job is active to avoid duplicate submissions."
        )
        return True

    return state in (
        "RUNNING",
        "PENDING",
    )
    
def main():
    """
    Main script to be called as cron job.

    It checks which runs are ready for CatB/tailcuts processing and
    submits a pilot job executing catb_tailcuts_pipeline.py.
    """

    opts = parser.parse_args()
    if opts.tel_id == "all":
        parser.error(
            "tel_id 'all' is not supported by sequencer_catB_tailcuts; run separately for ST, LST1, or LST2."
        )

    options.input_state = opts.input_state
    options.tel_id = opts.tel_id
    options.simulate = opts.simulate
    options.verbose = opts.verbose
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

    run_summary_dir = Path(
        cfg.get(options.tel_id, "RUN_SUMMARY_DIR")
    )

    run_summary = Table.read(
        run_summary_dir
        / f"RunSummary_{date_to_dir(options.date)}.ecsv"
    )

    data_runs = run_summary[
        run_summary["run_type"] == "DATA"
    ]

    for run_id in data_runs["run_id"]:

        # First check if DL1A has been produced
        if not r0_to_dl1_step_finished_for_run(run_id):

            log.info(
                f"The r0_to_dl1 step did not finish yet "
                f"for run {run_id:05d}. "
                f"Please try again later."
            )

            continue

        need_catb = (
            cfg.getboolean(
                "lstchain",
                "apply_catB_calibration",
            )
            and not catB_closed_file_exists(run_id)
        )

        need_tailcuts = (
            not cfg.getboolean(
                "lstchain",
                "apply_standard_dl1b_config",
            )
            and (
                not tailcuts_config_file_exists(run_id)
                or options.overwrite_tailcuts
            )
        )

        if not (need_catb or need_tailcuts):

            log.debug(
                f"Run {run_id:05d} already processed."
            )

            continue

        if pilot_job_is_active(run_id):

            log.debug(
                f"Pilot job already active for run "
                f"{run_id:05d}"
            )

            continue
            
        create_run_history_file(run_id)
        submit_pilot_script(run_id)


if __name__ == "__main__":
    main()
