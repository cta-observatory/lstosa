"""Functions to handle the interaction with the job scheduler."""

import datetime
import logging
import shutil
import subprocess as sp
import time
import re
import glob
import os
import errno
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import (
    pedestal_ids_file_exists,
    get_drive_file,
    get_summary_file,
    get_pedestal_ids_file,
    get_dl1_prod_id_and_config,
    catB_closed_file_exists,
)
from osa.utils.iofile import write_to_file
from osa.utils.logging import myLogger
from osa.utils.utils import (
    date_to_dir,
    time_to_seconds,
    stringify,
    date_to_iso,
    get_lstchain_version,
)
from osa.processing_plan import build_processing_plan

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "are_all_jobs_correctly_finished",
    "historylevel",
    "prepare_jobs",
    "sequence_filenames",
    "set_queue_values",
    "job_header_template",
    "plot_job_statistics",
    "scheduler_env_variables",
    "set_cache_dirs",
    "submit_jobs",
    "check_history_level",
    "get_sacct_output",
    "get_squeue_output",
    "filter_jobs",
    "run_sacct",
    "run_squeue",
    "calibration_sequence_job_template",
    "data_sequence_job_templates",
    "save_job_information",
    # catB helpers:
    "write_catb_pilot_script",
    "submit_catb_pilot_script",
    "pilot_job_is_active",
    # AUTOCLOSER helpers:
    "get_closer_sacct_output",
]

TAB = "\t".expandtabs(4)
FORMAT_SLURM = [
    "JobID",
    "JobName",
    "CPUTime",
    "CPUTimeRAW",
    "Elapsed",
    "TotalCPU",
    "MaxRSS",
    "State",
    "ExitCode",
]

PYTHON_IMPORTS = dedent(
    """\

    import os
    import subprocess
    import sys
    import tempfile

    """
)


def are_all_jobs_correctly_finished(sequence_list):
    """
    Check if all jobs are correctly finished by looking
    at the history file.
    """
    flag = True
    analysis_directory = Path(options.directory)
    for sequence in sequence_list:
        if sequence.type != "DATA":
            continue
        else:
            history_files_list = analysis_directory.rglob(f"*{sequence.run}.0*.history")

        if not options.test:
            try:
                next(history_files_list)
            except StopIteration:
                log.debug("No history files found.")
                flag = False

        for history_file in history_files_list:
            out, _ = historylevel(history_file, sequence.type)
            if out == 0:
                log.debug(f"Job {sequence.seq} ({sequence.type}) correctly finished")
                continue

            if out == 2 and options.no_dl1ab:
                log.debug(
                    f"Job {sequence.seq} ({sequence.type}) correctly "
                    f"finished up to DL1A, but --no-dl1ab option selected"
                )
                continue

            log.warning(
                f"Job {sequence.seq} (run {sequence.run}) not correctly finished [level {out}]"
            )
            flag = False
    return flag


def historylevel(history_file: Path, data_type: str):
    """
    Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file.
    """
    if data_type == "DATA":
        level = 3
    elif data_type == "PEDCALIB":
        level = 2
    else:
        raise ValueError(f"Type {data_type} not expected")

    exit_status = 0

    if history_file.exists():
        if data_type == "DATA":
            match = re.search(r"sequence_LST1_(\d+)\.\d+", str(history_file))
        elif data_type == "PEDCALIB":
            match = re.search(r"sequence_LST1_(\d+)\.history", str(history_file))
        run_id = int(match.group(1))
        for line in history_file.read_text().splitlines():
            words = line.split()
            try:
                program = words[1]
                prod_id = words[2]
                exit_status = int(words[-1])
                log.debug(f"{program}, finished with error {exit_status} and prod ID {prod_id}")
            except (IndexError, ValueError) as err:
                log.exception(f"Malformed history file {history_file}, {err}")
            else:
                # Calibration sequence
                if program == cfg.get("lstchain", "drs4_baseline"):
                    level = 1 if exit_status == 0 else 2
                elif program == cfg.get("lstchain", "charge_calibration"):
                    level = 0 if exit_status == 0 else 1
                # Data sequence
                elif program == cfg.get("lstchain", "r0_to_dl1"):
                    level = 2 if exit_status == 0 else 3
                elif program == cfg.get("lstchain", "dl1ab"):
                    dl1_prod_id = get_dl1_prod_id_and_config(run_id)[0]
                    if (exit_status == 0) and (prod_id == dl1_prod_id):
                        log.debug(f"DL1ab prod ID: {dl1_prod_id} already produced")
                        level = 1
                    else:
                        level = 2
                        log.debug(f"DL1ab prod ID: {dl1_prod_id} not produced yet")
                        break
                elif program == cfg.get("lstchain", "check_dl1"):
                    level = 0 if exit_status == 0 else 1

                else:
                    log.warning(f"Program name not identified: {program}")

    return level, exit_status


def prepare_jobs(sequence_list):
    """Prepare job file template for each sequence."""
    if not options.simulate:
        log.info("Building job scripts for each sequence.")

    for sequence in sequence_list:
        log.debug(f"Creating job scripts for sequence {sequence.seq}")
        if sequence.type == "PEDCALIB":
            calibration_sequence_job_template(sequence)
        elif sequence.type == "DATA":
            data_sequence_job_templates(sequence)
        else:
            raise ValueError(f"Type {sequence.type} not expected")


def sequence_filenames(sequence):
    """Build names of the script, veto and history files."""
    basename = f"sequence_{sequence.jobname}"
    sequence.script = Path(options.directory) / f"{basename}.py"
    sequence.veto = Path(options.directory) / f"{basename}.veto"
    sequence.history = Path(options.directory) / f"{basename}.history"


def save_job_information():
    """
    Write job information from sacct to a file.
    """
    log_directory = Path(options.directory) / "log"
    log_directory.mkdir(exist_ok=True, parents=True)
    file_path = log_directory / "job_information.csv"

    sacct_output = run_sacct()
    jobs_df = get_sacct_output(sacct_output)

    jobs_df_filtered = jobs_df.copy()
    jobs_df_filtered = jobs_df_filtered.dropna()

    jobs_df_filtered.to_csv(file_path, index=False, sep=",")


def plot_job_statistics(sacct_output: pd.DataFrame, directory: Path):
    """
    Produce a histogram plot of job stats.
    """
    sacct_output_filter = sacct_output.copy()
    sacct_output_filter = sacct_output_filter.dropna()
    sacct_output_filter["MaxRSS"] = sacct_output_filter["MaxRSS"].str.strip("G").astype(float)

    plt.figure()
    plt.hist2d(sacct_output_filter.MaxRSS, sacct_output_filter.CPUTimeRAW / 3600, bins=50)
    plt.xlabel("MaxRSS [GB]")
    plt.ylabel("Elapsed time [h]")
    directory.mkdir(exist_ok=True, parents=True)
    plot_path = directory / "job_statistics.pdf"
    plt.savefig(plot_path)


def scheduler_env_variables(sequence, scheduler="slurm"):
    """Return the SBATCH environment variables for a sequence."""
    if scheduler != "slurm":
        log.warning("No other schedulers are currently supported")
        return None

    sbatch_parameters = [
        f"--job-name={sequence.jobname}",
        f"--time={cfg.get('SLURM', 'WALLTIME')}",
        f"--chdir={options.directory}",
        f"--output=log/Run{sequence.run:05d}.%4a_jobid_%A.out",
        f"--error=log/Run{sequence.run:05d}.%4a_jobid_%A.err",
    ]

    subruns = sequence.subruns - 1

    if sequence.type == "DATA":
        sbatch_parameters.append(f"--array=0-{subruns}")

    sbatch_parameters.append(f"--partition={cfg.get('SLURM', f'PARTITION_{sequence.type}')}")
    sbatch_parameters.append(f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE_{sequence.type}')}")
    sbatch_parameters.append(f"--account={cfg.get('SLURM', 'ACCOUNT')}")

    return ["#SBATCH " + line for line in sbatch_parameters]


def job_header_template(sequence):
    """
    Returns a string with the job header template including SBATCH env vars.
    """
    python_shebang = "#!/bin/env python"
    if options.test:
        return python_shebang
    sbatch_parameters = "\n".join(scheduler_env_variables(sequence))
    return python_shebang + 2 * "\n" + sbatch_parameters


def set_cache_dirs():
    """
    Export cache directories for the jobs provided they are defined in the config file.
    """
    ctapipe_cache = cfg.get("CACHE", "CTAPIPE_CACHE")
    ctapipe_svc_path = cfg.get("CACHE", "CTAPIPE_SVC_PATH")
    mpl_config_path = cfg.get("CACHE", "MPLCONFIGDIR")

    content = []
    if ctapipe_cache:
        content.append(f"os.environ['CTAPIPE_CACHE'] = '{ctapipe_cache}'")

    if ctapipe_svc_path:
        content.append(f"os.environ['CTAPIPE_SVC_PATH'] = '{ctapipe_svc_path}'")

    if mpl_config_path:
        content.append(f"os.environ['MPLCONFIGDIR'] = '{mpl_config_path}'")

    return "\n".join(content)


def data_sequence_job_templates(sequence):
    """
    Create two job scripts per DATA sequence:
      - r0->dl1 (array) script
      - dl1ab (array) script
    """
    job_header = job_header_template(sequence)
    flat_date = date_to_dir(options.date)
    plan = build_processing_plan(options.input_state)

    base_commandargs = ["datasequence"]
    if options.verbose:
        base_commandargs.append("-v")
    if options.simulate:
        base_commandargs.append("-s")
    if options.configfile:
        base_commandargs.extend(("--config", f"{Path(options.configfile).resolve()}"))
    base_commandargs.append(f"--input-state={options.input_state}")
    if sequence.type == "DATA" and options.no_dl1ab:
        base_commandargs.append("--no-dl1ab")

    base_commandargs.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
            f"--drive-file={get_drive_file(flat_date)}",
            f"--run-summary={get_summary_file(flat_date)}",
        )
    )

    # Add calibration files only if needed
    if plan.needs_calibration:
        base_commandargs.extend(
            (
                f"--drs4-pedestal-file={sequence.drs4_file}",
                f"--time-calib-file={sequence.time_calibration_file}",
                f"--pedcal-file={sequence.calibration_file}",
                f"--systematic-correction-file={sequence.systematic_correction_file}",
            )
        )
    else:
        log.info(f"Skipping calibration inputs for run {sequence.run} (already calibrated)")

    if not options.no_dl1ab:
        dl1_prod_id, dl1b_config = get_dl1_prod_id_and_config(sequence.run)
        sequence.dl1_prod_id = dl1_prod_id
        sequence.dl1b_config = dl1b_config

        base_commandargs.append(f"--dl1b-config={sequence.dl1b_config}")
        base_commandargs.append(f"--dl1-prod-id={sequence.dl1_prod_id}")

    # r0->dl1 script
    header = job_header + "\n" + PYTHON_IMPORTS
    if not options.test:
        header += set_cache_dirs() + "\n"
        header += "subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    else:
        header += "subruns = 0\n"

    header += "\n"
    header += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    header += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    header += TAB + "proc = subprocess.run([\n"

    content_r0 = header
    for arg in base_commandargs:
        content_r0 += TAB * 2 + f"'{arg}',\n"

    if pedestal_ids_file_exists(sequence.run):
        pedestal_ids_file = get_pedestal_ids_file(sequence.run, flat_date)
        content_r0 += TAB * 2 + f"f'--pedestal-ids-file={pedestal_ids_file}',\n"

    content_r0 += TAB * 2 + f"f'{sequence.run:05d}.{{subruns:04d}}',\n"
    content_r0 += TAB * 2 + f"'{options.tel_id}'\n"
    content_r0 += TAB + "])\n\n"
    content_r0 += "sys.exit(proc.returncode)"

    # dl1ab script
    header_dl1ab = job_header + "\n" + PYTHON_IMPORTS
    if not options.test:
        header_dl1ab += set_cache_dirs() + "\n"
        header_dl1ab += "subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    else:
        header_dl1ab += "subruns = 0\n"

    header_dl1ab += "\n"
    header_dl1ab += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    header_dl1ab += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    header_dl1ab += TAB + "proc = subprocess.run([\n"

    content_dl1ab = header_dl1ab
    base_args_for_dl1ab = [a for a in base_commandargs if not a.startswith("--no-dl1ab")]
    for arg in base_args_for_dl1ab:
        content_dl1ab += TAB * 2 + f"'{arg}',\n"

    if pedestal_ids_file_exists(sequence.run):
        pedestal_ids_file = get_pedestal_ids_file(sequence.run, flat_date)
        content_dl1ab += TAB * 2 + f"f'--pedestal-ids-file={pedestal_ids_file}',\n"

    content_dl1ab += TAB * 2 + f"f'{sequence.run:05d}.{{subruns:04d}}',\n"
    content_dl1ab += TAB * 2 + f"'{options.tel_id}'\n"
    content_dl1ab += TAB + "])\n\n"
    content_dl1ab += "sys.exit(proc.returncode)"

    basename = f"sequence_{sequence.jobname}"
    script_r0 = Path(options.directory) / f"{basename}.py"
    script_dl1ab = Path(options.directory) / f"{basename}_dl1ab.py"

    if not options.simulate:
        write_to_file(script_r0, content_r0)
        write_to_file(script_dl1ab, content_dl1ab)

    sequence.script_r0 = script_r0
    sequence.script_dl1ab = script_dl1ab
    sequence.script = script_r0

    return content_r0


def calibration_sequence_job_template(sequence):
    """
    Create job script for calibration sequence (unchanged behavior).
    """
    job_header = job_header_template(sequence)

    if cfg.getboolean("lstchain", "use_lstcam_env_for_CatA_calib"):
        commandargs = ["conda", "run", "-n", "lstcam-env", "calibration_pipeline"]
    else:
        commandargs = ["calibration_pipeline"]

    if options.verbose:
        commandargs.append("-v")
    if options.simulate:
        commandargs.append("-s")
    if options.configfile:
        commandargs.extend(("--config", f"{Path(options.configfile).resolve()}"))
    commandargs.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--drs4-pedestal-run={sequence.drs4_run:05d}",
            f"--pedcal-run={sequence.run:05d}",
        )
    )

    content = job_header + "\n" + PYTHON_IMPORTS

    if not options.test:
        content += set_cache_dirs()
        content += "\n"
        content += "subruns = os.getenv('SLURM_ARRAY_TASK_ID')\n"
    else:
        content += "subruns = 0\n"

    content += "\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += TAB + "os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"

    content += TAB + "proc = subprocess.run([\n"

    for arg in commandargs:
        content += TAB * 2 + f"'{arg}',\n"

    content += TAB * 2 + f"'{options.tel_id}'\n"
    content += TAB + "])\n"
    content += "\n"
    content += "sys.exit(proc.returncode)"

    if not options.simulate:
        write_to_file(sequence.script, content)

    return content


#
# CatB pilot utilities: markers and submission helpers
#
def _catb_marker_path(run_id: int) -> Path:
    """Path of the marker file used to indicate a submitted CatB pilot for a run."""
    return Path(options.directory) / f"{options.tel_id}_{run_id:05d}.catb_submitted"


def mark_catb_submitted(run_id: int, jobid: str) -> None:
    """Atomically write a marker file with the jobid and timestamp to avoid duplicate submissions."""
    marker = _catb_marker_path(run_id)
    timestamp = datetime.datetime.utcnow().isoformat()
    content = f"{jobid}\n{timestamp}\n"
    marker.write_text(content)


def read_catb_marker(run_id: int) -> tuple[str, str] | None:
    """Read marker and return (jobid, timestamp) or None if missing/malformed."""
    marker = _catb_marker_path(run_id)
    if not marker.exists():
        return None
    try:
        lines = marker.read_text().splitlines()
        jobid = lines[0].strip()
        ts = lines[1].strip() if len(lines) > 1 else ""
        return jobid, ts
    except Exception:
        return None


def remove_catb_marker(run_id: int) -> None:
    """Remove the marker file if present."""
    marker = _catb_marker_path(run_id)
    try:
        if marker.exists():
            marker.unlink()
    except Exception as e:
        log.warning(f"Could not remove catB marker for run {run_id:05d}: {e}")


def pilot_job_is_active(run_id: int) -> bool:
    """
    Return True if a CatB pilot job for run_id is already pending/running.
    Strategy:
      - If a marker file exists, query sacct for that job id to determine state.
      - If no marker, scan log files for a recent matching job id and query sacct.
      - If sacct is unavailable, be conservative and return True to avoid duplicates.
    """
    marker = _catb_marker_path(run_id)
    if marker.exists():
        info = read_catb_marker(run_id)
        if info:
            jobid, ts = info
            try:
                state = get_sacct_output(run_sacct(job_id=jobid))["State"].item()
            except Exception:
                log.warning("Could not query sacct to check existing catB job; assuming active to avoid duplicate submission.")
                return True
            return state in ("RUNNING", "PENDING")
        else:
            try:
                marker.unlink()
            except Exception:
                pass
            return False

    # fallback: scan logs
    log_dir = Path(options.directory) / "log"
    pattern = rf"{options.tel_id}_catB_tailcuts_{run_id:05d}_(\d+)\.err$"
    try:
        files = sorted(
            glob.glob(str(log_dir / f"{options.tel_id}_catB_tailcuts_{run_id:05d}_*.err")),
            key=lambda p: int(re.search(pattern, p).group(1)) if re.search(pattern, p) else -1,
        )
    except Exception:
        files = []

    if not files:
        return False

    m = re.search(rf"{options.tel_id}_catB_tailcuts_{run_id:05d}_(\d+)\.err", files[-1])
    if not m:
        return False
    jobid = m.group(1)
    try:
        state = get_sacct_output(run_sacct(job_id=jobid))["State"].item()
    except Exception:
        log.warning("Could not query sacct to check existing catB job; assuming active to avoid duplicate submission.")
        return True
    return state in ("RUNNING", "PENDING")


def write_catb_pilot_script(run_id: int) -> Path:
    """
    Create a pilot script for CatB/tailcuts for a single run.
    """
    log_dir = Path(options.directory) / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    job_name = f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
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
        worker_argv.extend(["--config", str(Path(options.configfile).resolve())])

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
    content += f"#SBATCH --output=log/{job_name}_%j.out\n"
    content += f"#SBATCH --error=log/{job_name}_%j.err\n"
    content += f"#SBATCH --account={account}\n\n"
    content += "#SBATCH --mem=12G\n\n"

    content += "import subprocess\n"
    content += "import sys\n\n"

    content += "proc = subprocess.run([\n"
    for arg in worker_argv:
        content += f"    {arg!r},\n"
    content += "])\n"
    content += "sys.exit(proc.returncode)\n"

    pilot_script = Path(options.directory) / (
        f"sequence_{options.tel_id}_{run_id:05d}_catb_tailcuts.py"
    )

    pilot_script.write_text(content)
    pilot_script.chmod(0o755)

    return pilot_script


def submit_catb_pilot_script(run_id: int, dependency_jobid: str | None = None) -> str | None:
    """
    Submit the pilot script for CatB/tailcuts via sbatch. Optionally add dependency.
    Uses atomic marker creation to avoid races.
    """
    # If run already closed, skip
    if catB_closed_file_exists(run_id):
        log.info(f"CatB already closed for run {run_id:05d}; skipping pilot submission.")
        return None

    marker = _catb_marker_path(run_id)

    # Try to atomically create marker (reservation)
    try:
        fd = os.open(str(marker), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except OSError as e:
        if e.errno == errno.EEXIST:
            info = read_catb_marker(run_id)
            if info:
                jobid, ts = info
                try:
                    state = get_sacct_output(run_sacct(job_id=jobid))["State"].item()
                except Exception:
                    log.warning("Could not query sacct for existing marker; assuming active to avoid duplicate submission.")
                    return jobid
                if state in ("RUNNING", "PENDING"):
                    log.info(f"Another process already submitted pilot for run {run_id:05d} (job {jobid}); skipping.")
                    return jobid
                else:
                    try:
                        marker.unlink()
                    except Exception:
                        log.warning("Could not remove stale marker; skipping submission to be safe.")
                        return None
                    return submit_catb_pilot_script(run_id, dependency_jobid)
            else:
                log.info(f"Marker exists for run {run_id:05d}, skipping to avoid duplicates.")
                return None
        else:
            log.exception(f"Could not create marker file for run {run_id:05d}: {e}")
            return None
    else:
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write("PENDING\n")
                fh.write(datetime.datetime.utcnow().isoformat() + "\n")
        except Exception as e:
            log.warning(f"Could not initialize marker for run {run_id:05d}: {e}")
            try:
                marker.unlink()
            except Exception:
                pass
            return None

    # Compose sbatch cmd
    pilot_script = write_catb_pilot_script(run_id)
    cmd = ["sbatch", "--parsable", str(pilot_script)]
    if dependency_jobid:
        cmd = ["sbatch", "--parsable", f"--dependency=afterok:{dependency_jobid}", str(pilot_script)]

    if options.simulate:
        log.info(f"Would submit {' '.join(cmd)}")
        marker.write_text("SIMULATE\n" + datetime.datetime.utcnow().isoformat() + "\n")
        return None

    try:
        job = sp.run(cmd, encoding="utf-8", capture_output=True, text=True, check=True)
    except sp.CalledProcessError as error:
        log.exception(f"Failed to submit CatB pilot for run {run_id:05d}: {error}")
        try:
            marker.unlink()
        except Exception:
            log.warning(f"Could not remove marker after failed sbatch for run {run_id:05d}")
        return None

    job_id = job.stdout.strip()
    log.info(f"Submitted CatB pipeline for run {run_id:05d} ({job_id})")

    try:
        marker.write_text(f"{job_id}\n{datetime.datetime.utcnow().isoformat()}\n")
    except Exception as e:
        log.warning(f"Could not write jobid to marker for run {run_id:05d}: {e}")

    return job_id


def submit_jobs(sequence_list, batch_command="sbatch"):
    """
    Submit the jobs to the cluster in three-phases per sequence:
      - r0->dl1 array (if not already active) with PEDCALIB dependency if needed
      - catB/tailcuts per-run pilot dependent on r0 job (if needed)
      - dl1ab array dependent on catB pilot (or r0 if no catB)
    """
    job_list = []
    no_display_backend = "--export=ALL,MPLBACKEND=Agg"
    plan = build_processing_plan(options.input_state)
    parent_jobid = None  # Persist across loop iterations for PEDCALIB -> DATA dependency

    for sequence in sequence_list:
        # PEDCALIB sequence: optional if already calibrated
        if sequence.type == "PEDCALIB":
            if not plan.needs_calibration:
                log.info(
                    f"Skipping PEDCALIB for run {sequence.run} "
                    "(already calibrated)"
                )
                continue

            commandargs = [batch_command, "--parsable", no_display_backend]
            commandargs.append(str(sequence.script))
            if options.simulate or options.no_calib or options.test:
                log.debug("SIMULATE Launching scripts")
            else:
                try:
                    log.debug(f"Launching script {sequence.script}")
                    parent_jobid = sp.check_output(
                        commandargs, universal_newlines=True, shell=False
                    ).split()[0]
                    log.info(f"Submitted PEDCALIB for run {sequence.run:05d} -> job {parent_jobid}")
                except sp.CalledProcessError as error:
                    rc = error.returncode
                    log.exception(f"Command '{batch_command}' not found, error {rc}")
                    parent_jobid = None

            log.debug(stringify(commandargs))
            job_list.append(sequence.script)
            continue

        # DATA sequences: three-phase submit
        if sequence.type == "DATA":
            # Skip if there is already an active job for this sequence (avoid duplicates)
            if getattr(sequence, "state", None) in ("RUNNING", "PENDING", "COMPLETING"):
                log.info(f"Sequence {sequence.jobname} already active (state={sequence.state}), skipping submission.")
                continue

            # 1) submit r0->dl1 array (if not active). Use sequence.script_r0 created by prepare_jobs.
            cmd_r0 = [batch_command, "--parsable", no_display_backend]

            # Add dependency on PEDCALIB if calibration is needed
            if plan.needs_calibration and parent_jobid is not None:
                log.debug(f"Adding dependency on calibration job {parent_jobid}")
                cmd_r0.append(f"--dependency=afterok:{parent_jobid}")

            cmd_r0.append(str(sequence.script_r0))

            if options.simulate:
                log.info(f"SIMULATE would submit r0->dl1 array for run {sequence.run:05d}: {' '.join(cmd_r0)}")
                job_id_r0 = None
            elif options.test:
                log.info(f"TEST run of r0->dl1 script for run {sequence.run:05d}")
                sp.check_output(["python", str(sequence.script_r0)], shell=False)
                job_id_r0 = None
            else:
                try:
                    out = sp.check_output(cmd_r0, shell=False).decode()
                    job_id_r0 = out.split()[0]
                    log.info(f"Submitted r0->dl1 array for run {sequence.run:05d} -> job {job_id_r0}")
                except sp.CalledProcessError as error:
                    log.exception(f"Failed to submit r0->dl1 for run {sequence.run:05d}: {error}")
                    job_id_r0 = None

            job_list.append(sequence.script_r0)

            # 2) decide whether CatB/tailcuts are needed for this run
            need_catb = cfg.getboolean("lstchain", "apply_catB_calibration") and not catB_closed_file_exists(sequence.run)
            tailcuts_json = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{sequence.run:05d}.json"
            need_tailcuts = (not cfg.getboolean("lstchain", "apply_standard_dl1b_config")) and (not tailcuts_json.exists())

            job_id_catb = None
            if need_catb or need_tailcuts:
                # Safe: require job_id_r0 present (r0 array submitted in this invocation)
                if job_id_r0 is None and not options.force_submit:
                    log.info(f"No r0->dl1 jobid for run {sequence.run:05d} available; skipping CatB pilot for now.")
                else:
                    job_id_catb = submit_catb_pilot_script(sequence.run, dependency_jobid=job_id_r0)
                    if job_id_catb:
                        log.info(f"Submitted CatB pilot for run {sequence.run:05d} -> job {job_id_catb}")
                    else:
                        log.info(f"CatB pilot for run {sequence.run:05d} not submitted (simulate/skipped).")

            # 3) submit dl1ab array with dependency on catB (if created) or r0 job
            cmd_dl1ab = [batch_command, "--parsable", no_display_backend]
            dep_for_dl1 = job_id_catb if job_id_catb else job_id_r0
            if dep_for_dl1:
                cmd_dl1ab.insert(2, f"--dependency=afterok:{dep_for_dl1}")
            cmd_dl1ab.append(str(sequence.script_dl1ab))

            if options.simulate:
                log.info(f"SIMULATE would submit dl1ab array for run {sequence.run:05d}: {' '.join(cmd_dl1ab)}")
            elif options.test:
                log.info(f"TEST running dl1ab script for run {sequence.run:05d}")
                sp.check_output(["python", str(sequence.script_dl1ab)], shell=False)
            else:
                try:
                    sp.check_output(cmd_dl1ab, shell=False)
                    log.info(f"Submitted dl1ab array for run {sequence.run:05d} with dependency {dep_for_dl1}")
                except sp.CalledProcessError as error:
                    log.exception(f"Failed to submit dl1ab for run {sequence.run:05d}: {error}")

            job_list.append(sequence.script_dl1ab)
            continue

        # fallback
        job_list.append(sequence.script)

    return job_list


def run_squeue() -> StringIO:
    """Run squeue command to get the status of the jobs."""
    if shutil.which("squeue") is None:
        log.warning("No job info available since squeue command is not available")
        return StringIO()

    out_fmt = "%i;%j;%T;%M"  # JOBID, NAME, STATE, TIME
    return StringIO(sp.check_output(["squeue", "--me", "-o", out_fmt]).decode())


def get_squeue_output(squeue_output: StringIO) -> pd.DataFrame:
    """
    Obtain the current job information from squeue output and return a pandas dataframe.
    """
    df = pd.read_csv(squeue_output, delimiter=";")
    df.rename(
        inplace=True,
        columns={
            "STATE": "State",
            "JOBID": "JobID",
            "NAME": "JobName",
            "TIME": "CPUTime",
        },
    )

    df = df[df["JobName"].str.contains("LST1")]

    try:
        df["JobID"] = df["JobID"].apply(lambda x: x.split("_")[0]).astype("int")
    except Exception:
        log.debug("No job info could be obtained from squeue")

    df["CPUTimeRAW"] = df["CPUTime"].apply(time_to_seconds)

    return df


def run_sacct(job_id: str = None) -> StringIO:
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
    ]

    if job_id:
        sacct_cmd.append("--jobs")
        sacct_cmd.append(job_id)

    if cfg.get("SLURM", "STARTTIME_DAYS_SACCT"):
        days = int(cfg.get("SLURM", "STARTTIME_DAYS_SACCT"))
        start_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
        sacct_cmd.extend(["--starttime", start_date])

    return StringIO(sp.check_output(sacct_cmd).decode())


def get_sacct_output(sacct_output: StringIO) -> pd.DataFrame:
    """
    Fetch the information of jobs using sacct and store it in a pandas dataframe.
    """
    sacct_output = pd.read_csv(sacct_output, names=FORMAT_SLURM)

    sacct_output = sacct_output[
        (~sacct_output["JobID"].str.contains(r"\."))
        | (sacct_output["JobName"].str.contains("LST1"))
    ]

    try:
        sacct_output["JobID"] = sacct_output["JobID"].apply(lambda x: x.split("_")[0])
        sacct_output["JobID"] = sacct_output["JobID"].str.strip(".batch").astype(int)
    except Exception:
        log.debug("No job info could be obtained from sacct")

    return sacct_output


def get_closer_sacct_output(sacct_output) -> pd.DataFrame:
    """
    Fetch the information of jobs in the queue launched by AUTOCLOSER using the sacct 
    SLURM output and store it in a pandas dataframe.

    Parameters
    ----------
    sacct_output : StringIO or pd.DataFrame
        Output from run_sacct()

    Returns
    -------
    queue_list: pd.DataFrame
        Filtered dataframe with only AUTOCLOSER-related jobs
    """
    sacct_output = pd.read_csv(sacct_output, names=FORMAT_SLURM)

    # Keep only the jobs corresponding to AUTOCLOSER sequences 
    # Until the merging of muon files is fixed, check all jobs except "lstchain_merge_muon_files"
    sacct_output = sacct_output[
        (sacct_output["JobName"].str.contains("lstchain_merge_hdf5_files"))
        | (sacct_output["JobName"].str.contains("lstchain_check_dl1"))
        | (sacct_output["JobName"].str.contains("lstchain_longterm_dl1_check"))
        | (sacct_output["JobName"].str.contains("lstchain_cherenkov_transparency"))
        | (sacct_output["JobName"].str.contains("provproces"))
        | (sacct_output["JobName"].str.contains("lstchain_dl1_to_dl2"))
    ]

    try:
        sacct_output["JobID"] = sacct_output["JobID"].apply(lambda x: x.split("_")[0])
        sacct_output["JobID"] = sacct_output["JobID"].str.strip(".batch").astype(int)

    except AttributeError:
        log.debug("No job info could be obtained from sacct")

    return sacct_output


def filter_jobs(job_info: pd.DataFrame, sequence_list: Iterable):
    """Filter the job info list to get the values of the jobs in the current queue."""
    sequences_info = pd.DataFrame([vars(seq) for seq in sequence_list])
    return job_info[job_info["JobName"].isin(sequences_info["jobname"])]


def set_queue_values(
    sacct_info: pd.DataFrame, squeue_info: pd.DataFrame, sequence_list: Iterable
) -> None:
    """
    Extract job info and fetch them into the sequence objects.
    """
    if sacct_info.empty and squeue_info.empty or sequence_list is None:
        return

    job_info = pd.concat([sacct_info, squeue_info])
    job_info_filtered = filter_jobs(job_info, sequence_list)

    for sequence in sequence_list:
        df_jobname = job_info_filtered[job_info_filtered["JobName"] == sequence.jobname]
        sequence.tries = df_jobname["JobID"].nunique()
        sequence.action = "Check"

        if not df_jobname.empty:
            sequence.jobid = df_jobname["JobID"].max()
            df_jobid_filtered = df_jobname[df_jobname["JobID"] == sequence.jobid]
            try:
                sequence.cputime = time.strftime(
                    "%H:%M:%S",
                    time.gmtime(df_jobid_filtered["CPUTimeRAW"].median(skipna=False)),
                )
            except ValueError:
                sequence.cputime = None

            update_sequence_state(sequence, df_jobid_filtered)


def update_sequence_state(sequence, filtered_job_info: pd.DataFrame) -> None:
    """
    Update the state of the sequence based on the job info.
    """
    if (filtered_job_info.State.values == "COMPLETED").all():
        sequence.state = "COMPLETED"
        sequence.exit = filtered_job_info["ExitCode"].iloc[0]
    elif (filtered_job_info.State.values == "PENDING").all():
        sequence.state = "PENDING"
    elif any("FAILED" in job for job in filtered_job_info.State):
        sequence.state = "FAILED"
        sequence.exit = filtered_job_info[filtered_job_info.State.values == "FAILED"][
            "ExitCode"
        ].iloc[0]
    elif any("CANCELLED" in job for job in filtered_job_info.State):
        sequence.state = "CANCELLED"
        mask = ["CANCELLED" in job for job in filtered_job_info.State]
        sequence.exit = filtered_job_info[mask]["ExitCode"].iloc[0]
    elif any("TIMEOUT" in job for job in filtered_job_info.State):
        sequence.state = "TIMEOUT"
        sequence.exit = "0:15"
    elif any("RUNNING" in job for job in filtered_job_info.State):
        sequence.state = "RUNNING"


def job_finished_in_timeout(job_id: str) -> bool:
    """Return True if the input job_id finished in TIMEOUT state."""
    job_status = get_sacct_output(run_sacct(job_id=job_id))["State"]
    if job_id and job_status.item() == "TIMEOUT":
        return True
    else:
        return False
