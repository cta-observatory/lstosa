"""Functions to handle the interaction with the job scheduler.

This module owns everything related to SLURM:
  - SBATCH headers and job script generation (r0->dl1, CatB/tailcuts pilot, dl1ab, calibration)
  - job submission (`sbatch_submit`) and job state queries (squeue / sacct)
  - the per-run submission logic (`submit_jobs`)
"""

import datetime
import logging
import re
import shutil
import subprocess as sp
import time
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional

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
from osa.processing_plan import build_processing_plan
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir, date_to_iso, time_to_seconds

log = myLogger(logging.getLogger(__name__))

__all__ = [
    # naming helpers
    "r0_jobname",
    "catb_jobname",
    "dl1ab_jobname",
    # SBATCH header / script generation
    "scheduler_env_variables",
    "job_header_template",
    "set_cache_dirs",
    "write_r0_script",
    "write_dl1ab_script",
    "write_catb_pilot_script",
    "calibration_sequence_job_template",
    "sequence_filenames",
    "prepare_jobs",
    # submission
    "sbatch_submit",
    "submit_jobs",
    # job state queries
    "run_squeue",
    "get_squeue_output",
    "run_sacct",
    "get_sacct_output",
    "get_closer_sacct_output",
    "filter_jobs",
    "job_is_active",
    "get_active_jobid",
    "get_last_job_state",
    "array_job_status",
    "update_job_info",
    "set_queue_values",
    "update_sequence_state",
    "job_finished_in_timeout",
    # history helpers
    "historylevel",
    "r0_job_completed",
    "run_fully_processed",
    "CAT_A_DATACHECK_DIR",
    "are_all_jobs_correctly_finished",
    # misc
    "catb_tailcuts_needed",
    "plot_job_statistics",
    "save_job_information",
]

TAB = "\t".expandtabs(4)
SHEBANG = "#!/usr/bin/env python3"
PYTHON_IMPORTS = "import os\nimport subprocess\nimport sys\nimport tempfile\n"

# Subdirectory (inside options.directory) with the datacheck of the DL1a files (cat A),
# produced in the same job as r0->dl1.
CAT_A_DATACHECK_DIR = "datacheck_cat_a"

ACTIVE_STATES = {"RUNNING", "PENDING", "COMPLETING"}
BAD_STATES = {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY"}

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


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------
def r0_jobname(run_id: int) -> str:
    return f"{options.tel_id}_{run_id:05d}"


def catb_jobname(run_id: int) -> str:
    return f"{options.tel_id}_catB_tailcuts_{run_id:05d}"


def dl1ab_jobname(run_id: int) -> str:
    return f"{options.tel_id}_dl1ab_{run_id:05d}"


def _script_path(run_id: int, suffix: str = "") -> Path:
    return Path(options.directory) / f"sequence_{options.tel_id}_{run_id:05d}{suffix}.py"


def _array_spec(subruns: int) -> str:
    """SLURM array range for a run with `subruns` subruns."""
    return f"0-{max(subruns - 1, 0)}"


def _subrun_expression(run_id: int) -> str:
    """Python source of the run.subrun argument, resolved at job runtime."""
    return f"f'{run_id:05d}.{{subruns:04d}}'"


# ---------------------------------------------------------------------------
# Histories
# ---------------------------------------------------------------------------
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


def _read_history_entries(history_file: Path) -> list:
    """
    Parse a history file into a chronological list of (program, prod_id, exit_status).
    Malformed lines are skipped.
    """
    entries = []
    for line in history_file.read_text().splitlines():
        words = line.split()
        if not words:
            continue
        try:
            entries.append((words[1], words[2], int(words[-1])))
        except (IndexError, ValueError):
            log.warning(f"Malformed line in history file {history_file}: {line!r}")
    return entries


def historylevel(history_file: Path, data_type: str):
    """
    Returns the level from which the analysis should begin and
    the rc of the last executable given a certain history file.

    Levels of a DATA sequence:
        4: r0->dl1 pending
        3: datacheck of the DL1a file (cat A) pending
        2: dl1ab pending
        1: datacheck of the DL1b file pending
        0: everything done
    Levels of a PEDCALIB sequence: 2 (drs4 baseline), 1 (charge calibration), 0 (done).

    `lstchain_check_dl1` appears twice in the history of a DATA subrun (cat A first,
    DL1b after dl1ab). Both lines are identical, so they are told apart by order:
    a check_dl1 before any dl1ab line is the cat A datacheck.
    """
    if data_type == "DATA":
        level = 4
    elif data_type == "PEDCALIB":
        level = 2
    else:
        raise ValueError(f"Type {data_type} not expected")

    exit_status = 0

    if not history_file.exists():
        return level, exit_status

    run_id = None
    if data_type == "DATA":
        run_id = int(re.search(r"sequence_LST1_(\d+)\.\d+", str(history_file)).group(1))

    dl1ab_seen = False
    for program, prod_id, exit_status in _read_history_entries(history_file):
        log.debug(f"{program}, finished with error {exit_status} and prod ID {prod_id}")

        # Calibration sequence
        if program == cfg.get("lstchain", "drs4_baseline"):
            level = 1 if exit_status == 0 else 2
        elif program == cfg.get("lstchain", "charge_calibration"):
            level = 0 if exit_status == 0 else 1
        # Data sequence
        elif program == cfg.get("lstchain", "r0_to_dl1"):
            level = 3 if exit_status == 0 else 4
        elif program == cfg.get("lstchain", "dl1ab"):
            dl1ab_seen = True
            dl1_prod_id = get_dl1_prod_id_and_config(run_id)[0]
            if (exit_status == 0) and (prod_id == dl1_prod_id):
                log.debug(f"DL1ab prod ID: {dl1_prod_id} already produced")
                level = 1
            else:
                level = 2
                log.debug(f"DL1ab prod ID: {dl1_prod_id} not produced yet")
                break
        elif program == cfg.get("lstchain", "check_dl1"):
            if dl1ab_seen:
                level = 0 if exit_status == 0 else 1
            elif level == 3:
                # datacheck of the DL1a file (cat A), right after r0->dl1
                level = 2 if exit_status == 0 else 3
        else:
            log.warning(f"Program name not identified: {program}")

    return level, exit_status


def _r0_stage_ok(entries: list) -> bool:
    """
    The first job (r0->dl1 + cat A datacheck) is completed when r0->dl1 finished
    correctly and was followed by a correct check_dl1. Histories where dl1ab was
    already attempted after r0->dl1 (runs processed before the cat A datacheck
    existed) also count as completed.
    """
    r0_to_dl1 = cfg.get("lstchain", "r0_to_dl1")
    check_dl1 = cfg.get("lstchain", "check_dl1")
    dl1ab = cfg.get("lstchain", "dl1ab")

    r0_done = False
    for program, _, rc in entries:
        if program == r0_to_dl1 and rc == 0:
            r0_done = True
        elif r0_done and program == check_dl1 and rc == 0:
            return True
        elif r0_done and program == dl1ab:
            return True
    return False


def _fully_processed(entries: list) -> bool:
    """A run is fully processed when a correct check_dl1 follows a correct dl1ab."""
    dl1ab = cfg.get("lstchain", "dl1ab")
    check_dl1 = cfg.get("lstchain", "check_dl1")

    dl1ab_done = False
    for program, _, rc in entries:
        if program == dl1ab and rc == 0:
            dl1ab_done = True
        elif dl1ab_done and program == check_dl1 and rc == 0:
            return True
    return False


def _all_histories_satisfy(run_id: int, predicate) -> bool:
    """True if the run has history files and `predicate(entries)` holds for every subrun."""
    history_files = sorted(
        Path(options.directory).glob(f"sequence_{options.tel_id}_{run_id:05d}.*.history")
    )
    if not history_files:
        return False

    for history_file in history_files:
        try:
            entries = _read_history_entries(history_file)
        except OSError:
            return False
        if not predicate(entries):
            return False
    return True


def r0_job_completed(run_id: int) -> bool:
    """True if, for every subrun, the r0->dl1 job (r0->dl1 + cat A datacheck) finished correctly."""
    return _all_histories_satisfy(run_id, _r0_stage_ok)


def run_fully_processed(run_id: int) -> bool:
    """True if, for every subrun, dl1ab and the DL1b datacheck finished correctly."""
    return _all_histories_satisfy(run_id, _fully_processed)


def sequence_filenames(sequence):
    """Build names of the script, veto and history files."""
    basename = f"sequence_{sequence.jobname}"
    sequence.script = Path(options.directory) / f"{basename}.py"
    sequence.veto = Path(options.directory) / f"{basename}.veto"
    sequence.history = Path(options.directory) / f"{basename}.history"


# ---------------------------------------------------------------------------
# Job information / statistics
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# SBATCH header and script rendering
# ---------------------------------------------------------------------------
def scheduler_env_variables(
    job_name: str,
    job_type: str,
    array_spec: Optional[str] = None,
    scheduler: str = "slurm",
):
    """
    Return the list of `#SBATCH ...` lines for a job.

    Parameters
    ----------
    job_name : str
        SLURM job name.
    job_type : str
        "DATA" or "PEDCALIB": selects PARTITION_<type> and MEMSIZE_<type> in the config.
    array_spec : str, optional
        SLURM array range (e.g. "0-11"). If given, log files include the array
        task id (%4a) and array job id (%A); otherwise they use the job id (%j).
    """
    if scheduler != "slurm":
        log.warning("No other schedulers are currently supported")
        return None

    sbatch_parameters = [
        f"--job-name={job_name}",
        f"--time={cfg.get('SLURM', 'WALLTIME')}",
        f"--chdir={options.directory}",
        "--exclude=cp05",
    ]

    if array_spec:
        sbatch_parameters.extend(
            (
                f"--array={array_spec}",
                f"--output=log/{job_name}.%4a_jobid_%A.out",
                f"--error=log/{job_name}.%4a_jobid_%A.err",
            )
        )
    else:
        sbatch_parameters.extend(
            (
                f"--output=log/{job_name}_%j.out",
                f"--error=log/{job_name}_%j.err",
            )
        )

    sbatch_parameters.extend(
        (
            f"--partition={cfg.get('SLURM', f'PARTITION_{job_type}')}",
            f"--mem-per-cpu={cfg.get('SLURM', f'MEMSIZE_{job_type}')}",
            f"--account={cfg.get('SLURM', 'ACCOUNT')}",
        )
    )

    return ["#SBATCH " + line for line in sbatch_parameters]


def _sbatch_header(job_name: str, job_type: str, array_spec: Optional[str] = None) -> str:
    """Shebang + SBATCH block, ready to be prepended to a job script."""
    sbatch_lines = scheduler_env_variables(job_name, job_type, array_spec) or []
    return SHEBANG + "\n\n" + "\n".join(sbatch_lines) + "\n"


def job_header_template(sequence) -> str:
    """
    Returns a string with the job header template including SBATCH env vars.
    """
    array_spec = _array_spec(sequence.subruns) if sequence.type == "DATA" else None
    return _sbatch_header(sequence.jobname, sequence.type, array_spec)


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


def _cache_dirs_block() -> str:
    """Cache/env exports for the job scripts (none when running locally in test mode)."""
    return "" if options.test else set_cache_dirs()


def _render_script(header: str, expressions: list, prologue_lines: Iterable[str] = ()) -> str:
    """
    Render a job script that runs a command in a subprocess.

    `expressions` are Python *source* expressions (already repr'ed strings or
    f-strings to be evaluated at job runtime) forming the subprocess argv.
    """
    prologue = [line for line in prologue_lines]
    cache = _cache_dirs_block()
    if cache:
        prologue.insert(0, cache)

    parts = [header, "\n", PYTHON_IMPORTS, "\n"]
    if prologue:
        parts.append("\n".join(prologue) + "\n\n")
    parts.append("subruns = int(os.getenv('SLURM_ARRAY_TASK_ID', '0'))\n\n")
    parts.append("with tempfile.TemporaryDirectory() as tmpdirname:\n")
    parts.append(f"{TAB}os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n")
    parts.append(f"{TAB}proc = subprocess.run([\n")
    parts.extend(f"{TAB * 2}{expr},\n" for expr in expressions)
    parts.append(f"{TAB}])\n\n")
    parts.append("sys.exit(proc.returncode)\n")
    return "".join(parts)


def _write_script(path: Path, content: str) -> Path:
    """Write a job script (creating the directory) and make it executable."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    except OSError:
        log.exception(f"Failed to write script {path}")
        raise

    try:
        path.chmod(0o755)
    except OSError:
        log.warning(f"Could not chmod {path}")

    log.debug(f"Wrote script {path}")
    return path


def _datasequence_base_args() -> list:
    """Arguments shared by every `datasequence` invocation."""
    args = ["datasequence"]
    if options.verbose:
        args.append("-v")
    if options.simulate:
        args.append("-s")
    if options.configfile:
        args.extend(("--config", str(Path(options.configfile).resolve())))
    args.append(f"--input-state={options.input_state}")
    return args


# ---------------------------------------------------------------------------
# Script writers
# ---------------------------------------------------------------------------
def write_r0_script(sequence) -> Path:
    """Write the r0->dl1 array script of a DATA sequence."""
    run_id = sequence.run
    flat_date = date_to_dir(options.date)
    plan = build_processing_plan(options.input_state)

    args = _datasequence_base_args()
    args.append("--no-dl1ab")
    args.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
            f"--drive-file={get_drive_file(flat_date)}",
            f"--run-summary={get_summary_file(flat_date)}",
        )
    )

    if plan.needs_calibration:
        args.extend(
            (
                f"--drs4-pedestal-file={sequence.drs4_file}",
                f"--time-calib-file={sequence.time_calibration_file}",
                f"--pedcal-file={sequence.calibration_file}",
                f"--systematic-correction-file={sequence.systematic_correction_file}",
            )
        )
    else:
        log.info(f"Skipping calibration inputs for run {run_id} (already calibrated)")

    if pedestal_ids_file_exists(run_id):
        args.append(f"--pedestal-ids-file={get_pedestal_ids_file(run_id, flat_date)}")

    expressions = [repr(a) for a in args]
    expressions.append(_subrun_expression(run_id))
    expressions.append(repr(options.tel_id))

    header = _sbatch_header(r0_jobname(run_id), "DATA", _array_spec(sequence.subruns))
    return _write_script(_script_path(run_id), _render_script(header, expressions))


def write_dl1ab_script(sequence) -> Path:
    """
    Write the dl1ab array script of a DATA sequence.

    The dl1 prod-id and the dl1b config are resolved when the job *runs* (not
    when this script is written), because the CatB/tailcuts pilot may create
    the tailcuts config in between.
    """
    run_id = sequence.run

    prologue = [
        "from osa.configs import options",
        "from osa.configs.config import cfg",
        "from osa.paths import get_dl1_prod_id_and_config",
        "",
    ]
    if options.configfile:
        prologue.append(f"cfg.read({str(Path(options.configfile).resolve())!r})")
    prologue.extend(
        (
            f"options.tel_id = {options.tel_id!r}",
            f"options.prod_id = {options.prod_id!r}",
            f"options.input_state = {options.input_state!r}",
            "",
            "# Resolved at job runtime: the CatB/tailcuts pilot may have produced the config.",
            f"run_id = {run_id}",
            "dl1_prod_id, dl1b_config = get_dl1_prod_id_and_config(run_id)",
        )
    )

    args = _datasequence_base_args()
    args.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
        )
    )

    expressions = [repr(a) for a in args]
    expressions.extend(
        (
            "f'--dl1b-config={dl1b_config}'",
            "f'--dl1-prod-id={dl1_prod_id}'",
            _subrun_expression(run_id),
            repr(options.tel_id),
        )
    )

    header = _sbatch_header(dl1ab_jobname(run_id), "DATA", _array_spec(sequence.subruns))
    content = _render_script(header, expressions, prologue_lines=prologue)
    return _write_script(_script_path(run_id, "_dl1ab"), content)


def write_catb_pilot_script(run_id: int) -> Path:
    """Write the per-run CatB/tailcuts pilot script (not an array job)."""
    argv = [
        "catb_tailcuts_pipeline",
        f"--date={date_to_iso(options.date)}",
        f"--input-state={options.input_state}",
    ]
    if options.verbose:
        argv.append("--verbose")
    if options.simulate:
        argv.append("--simulate")
    if options.configfile:
        argv.extend(("--config", str(Path(options.configfile).resolve())))
    if getattr(options, "overwrite_catB", False):
        argv.append("--overwrite-catB")
    if getattr(options, "overwrite_tailcuts", False):
        argv.append("--overwrite-tailcuts")
    argv.extend((str(run_id), options.tel_id))

    header = _sbatch_header(catb_jobname(run_id), "DATA")

    # The pipeline writes its own history entries; nothing else is logged here.
    content = header + "\nimport os\nimport subprocess\nimport sys\n\n"
    cache = _cache_dirs_block()
    if cache:
        content += cache + "\n\n"
    content += "proc = subprocess.run([\n"
    content += "".join(f"{TAB}{a!r},\n" for a in argv)
    content += "])\nsys.exit(proc.returncode)\n"

    return _write_script(_script_path(run_id, "_catb_tailcuts"), content)


def calibration_sequence_job_template(sequence) -> str:
    """
    Create job script for the calibration (PEDCALIB) sequence.
    """
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
    commandargs.append(options.tel_id)

    header = job_header_template(sequence)
    content = _render_script(header, [repr(a) for a in commandargs])

    _write_script(Path(sequence.script), content)
    return content


def prepare_jobs(sequence_list):
    """
    Prepare the job scripts that must exist *before* submission.

    Only the calibration (PEDCALIB) scripts are prepared here. The DATA scripts
    (r0->dl1, CatB/tailcuts pilot and dl1ab) are written on demand by
    `submit_jobs`, only for the jobs that actually need to be submitted.
    """
    plan = build_processing_plan(options.input_state)

    for sequence in sequence_list:
        log.debug(f"Preparing job scripts for sequence {sequence.seq}")
        if sequence.type == "PEDCALIB":
            if plan.needs_calibration:
                calibration_sequence_job_template(sequence)
        elif sequence.type != "DATA":
            raise ValueError(f"Type {sequence.type} not expected")


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------
def sbatch_submit(
    script_path: Path,
    dependency: Optional[str] = None,
    batch_command: str = "sbatch",
) -> Optional[str]:
    """
    Submit a script and return its job id (None if simulated, run in test
    mode or if the submission failed).
    """
    cmd = [batch_command, "--parsable", "--export=ALL,MPLBACKEND=Agg"]
    if dependency:
        cmd.append(f"--dependency=afterok:{dependency}")
    cmd.append(str(script_path))

    if options.simulate:
        log.info(f"[SIMULATE] Would run: {' '.join(cmd)}")
        return None

    if options.test:
        log.info(f"[TEST] Running {script_path} locally")
        sp.check_output(["python", str(script_path)], shell=False)
        return None

    try:
        proc = sp.run(cmd, capture_output=True, text=True, check=True)
    except (sp.CalledProcessError, FileNotFoundError) as error:
        stderr = getattr(error, "stderr", "")
        log.error(f"Submission failed for {script_path}: {error}; stderr: {stderr}")
        return None

    job_id = proc.stdout.strip().split(";")[0]
    log.info(f"Submitted {Path(script_path).name} -> job {job_id}")
    return job_id


def catb_tailcuts_needed(run_id: int) -> tuple:
    """Return (need_catb, need_tailcuts) for a run."""
    need_catb = cfg.getboolean("lstchain", "apply_catB_calibration") and not catB_closed_file_exists(
        run_id
    )
    tailcuts_json = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
    need_tailcuts = (not cfg.getboolean("lstchain", "apply_standard_dl1b_config")) and (
        not tailcuts_json.exists()
    )
    return need_catb, need_tailcuts


def _outcome(jobid: Optional[str]) -> str:
    """Outcome of an sbatch call: 'submitted' (also when simulated/tested) or 'failed'."""
    return "submitted" if (jobid or options.simulate or options.test) else "failed"


def _describe(outcome: str, jobid: Optional[str] = None, dependency: Optional[str] = None) -> str:
    """Human-readable description of what happened to a job (for log summaries)."""
    if outcome == "active":
        return f"already queued/running (job {jobid}), NOT submitted again" if jobid else (
            "already queued/running, NOT submitted again"
        )
    if outcome == "submitted":
        text = "would be submitted (simulate)" if options.simulate else (
            "run locally (test)" if options.test else f"submitted (job {jobid})"
        )
        return text + (f", depends on job {dependency}" if dependency else "")
    return "submission FAILED"


def _submit_unless_active(
    jobname: str,
    write_script,
    dependency: Optional[str] = None,
    batch_command: str = "sbatch",
):
    """
    Submit the script produced by `write_script()` unless a job with that name
    is already queued/running.

    Returns (job_id, outcome) with outcome in {"active", "submitted", "failed"}.
    """
    if job_is_active(jobname):
        jobid = get_active_jobid(jobname)
        log.info(f"{jobname}: already queued/running (job {jobid}); not submitting again.")
        return jobid, "active"

    jobid = sbatch_submit(write_script(), dependency=dependency, batch_command=batch_command)
    return jobid, _outcome(jobid)


def _dl1ab_dependency(
    run_id: int,
    need_pilot: bool,
    jobid_pilot: Optional[str],
    jobid_r0: Optional[str],
    r0_done: bool,
) -> tuple:
    """
    Decide whether dl1ab can be submitted and on which job it depends.
    Returns (submit, dependency_jobid).
    """
    if need_pilot:
        # dl1ab reads the CatB/tailcuts products at runtime: it must wait for the pilot.
        if jobid_pilot:
            return True, jobid_pilot
        if options.force_submit and jobid_r0:
            log.warning(
                f"Force-submitting dl1ab for run {run_id:05d} with dependency on r0 "
                f"({jobid_r0}) even though the CatB/tailcuts pilot is not available."
            )
            return True, jobid_r0
        log.info(f"CatB/tailcuts required for run {run_id:05d} but no pilot job found; skipping dl1ab.")
        return False, None

    if jobid_r0:
        return True, jobid_r0
    if r0_done:
        log.debug(f"r0 already completed for run {run_id:05d}; submitting dl1ab without dependency.")
        return True, None
    if options.force_submit:
        log.warning(f"Force-submitting dl1ab for run {run_id:05d} without dependency.")
        return True, None
    log.info(f"No r0 job available and r0 not completed for run {run_id:05d}; skipping dl1ab.")
    return False, None


def _calibration_completed(sequence) -> bool:
    history = Path(options.directory) / f"sequence_{sequence.jobname}.history"
    return history.exists() and historylevel(history, "PEDCALIB")[0] == 0


def _submit_pedcalib(sequence, plan, batch_command: str) -> Optional[str]:
    """Submit the calibration job if needed. Returns its job id (or None)."""
    if not plan.needs_calibration:
        log.info(f"Skipping PEDCALIB for run {sequence.run} (already calibrated)")
        return None
    if options.no_calib:
        log.info(f"Skipping PEDCALIB for run {sequence.run} (--no-calib)")
        return None
    if _calibration_completed(sequence):
        log.info(f"PEDCALIB for run {sequence.run} already completed.")
        return None

    if job_is_active(sequence.jobname):
        log.info(f"PEDCALIB for run {sequence.run} already active, skipping submission.")
        return get_active_jobid(sequence.jobname)
    return sbatch_submit(sequence.script, batch_command=batch_command)


def _submit_data_sequence(
    sequence, plan, calib_jobid: Optional[str], batch_command: str = "sbatch"
) -> list:
    """
    Three-phase submission for a DATA run:
      1) r0->dl1 + cat A datacheck array (if not completed / not active)
      2) CatB/tailcuts pilot, dependent on r0 (if needed)
      3) dl1ab array, dependent on the pilot (or on r0 if no pilot is needed)
    Returns the list of job ids submitted (or found active) for this run.
    """
    run_id = sequence.run
    job_ids = []
    summary = {}  # step -> what happened, logged at the end of the run

    log.info(f"Run {run_id:05d} ({sequence.subruns} subruns): checking which jobs are needed.")

    # 1) r0 -> dl1
    r0_done = r0_job_completed(run_id)
    jobid_r0 = None
    if r0_done:
        summary["r0->dl1+DC-A"] = "already completed (r0->dl1 + cat A datacheck in history), not submitted"
    else:
        jobid_r0, outcome = _submit_unless_active(
            r0_jobname(run_id),
            lambda: write_r0_script(sequence),
            dependency=calib_jobid,
            batch_command=batch_command,
        )
        summary["r0->dl1+DC-A"] = _describe(
            outcome, jobid_r0, calib_jobid if outcome == "submitted" else None
        )
        job_ids.append(jobid_r0)

    # 2) CatB / tailcuts pilot
    need_catb, need_tailcuts = catb_tailcuts_needed(run_id)
    need_pilot = need_catb or need_tailcuts
    jobid_pilot = None
    if not need_pilot:
        summary["catB/tailcuts"] = "not needed"
    else:
        needed = ", ".join(n for n, flag in (("CatB", need_catb), ("tailcuts", need_tailcuts)) if flag)
        log.info(f"Run {run_id:05d}: CatB/tailcuts pilot needed ({needed}).")
        pilot_name = catb_jobname(run_id)
        if job_is_active(pilot_name):
            jobid_pilot = get_active_jobid(pilot_name)
            summary["catB/tailcuts"] = _describe("active", jobid_pilot)
        elif jobid_r0 is None and not r0_done and not options.force_submit:
            summary["catB/tailcuts"] = "NOT submitted: no r0 job available yet (use --force-submit)"
        else:
            jobid_pilot = sbatch_submit(
                write_catb_pilot_script(run_id), dependency=jobid_r0, batch_command=batch_command
            )
            outcome = _outcome(jobid_pilot)
            summary["catB/tailcuts"] = _describe(outcome, jobid_pilot, jobid_r0)
        job_ids.append(jobid_pilot)

    # 3) dl1ab
    dl1ab_name = dl1ab_jobname(run_id)
    if run_fully_processed(run_id):
        summary["dl1ab"] = "run already fully processed (check_dl1 ok), not submitted"
    elif job_is_active(dl1ab_name):
        summary["dl1ab"] = _describe("active", get_active_jobid(dl1ab_name))
    else:
        submit, dependency = _dl1ab_dependency(run_id, need_pilot, jobid_pilot, jobid_r0, r0_done)
        if submit:
            jobid_dl1ab = sbatch_submit(
                write_dl1ab_script(sequence), dependency=dependency, batch_command=batch_command
            )
            summary["dl1ab"] = _describe(_outcome(jobid_dl1ab), jobid_dl1ab, dependency)
            job_ids.append(jobid_dl1ab)
        else:
            summary["dl1ab"] = "NOT submitted: waiting for its dependencies (see messages above)"

    lines = [f"Run {run_id:05d} summary:"]
    lines.extend(f"    {step:<14} {text}" for step, text in summary.items())
    log.info("\n".join(lines))

    return [j for j in job_ids if j]


def submit_jobs(sequence_list, batch_command: str = "sbatch") -> list:
    """
    Submit the jobs of every sequence to the cluster.

      - PEDCALIB: only if the processing plan needs calibration
      - DATA: r0->dl1 array -> CatB/tailcuts pilot -> dl1ab array

    Honors --simulate, --test and --force-submit. Returns the list of job ids
    submitted (or found already active) during this call.
    """
    plan = build_processing_plan(options.input_state)
    calib_jobid = None  # persists across iterations for the PEDCALIB -> DATA dependency
    job_ids = []

    n_runs = sum(1 for seq in sequence_list if seq.type == "DATA")
    log.info(f"Checking {n_runs} DATA run(s) for job submission.")

    for sequence in sequence_list:
        if sequence.type == "PEDCALIB":
            calib_jobid = _submit_pedcalib(sequence, plan, batch_command)
            if calib_jobid:
                job_ids.append(calib_jobid)
        elif sequence.type == "DATA":
            job_ids.extend(_submit_data_sequence(sequence, plan, calib_jobid, batch_command))

    if job_ids:
        log.info(f"Jobs submitted or already active: {', '.join(job_ids)}")
    else:
        log.info("No jobs submitted in this call.")

    return job_ids


# ---------------------------------------------------------------------------
# squeue / sacct
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Job state queries by job name
# ---------------------------------------------------------------------------
def job_is_active(jobname: str) -> bool:
    """
    Return True if a job with this exact name is queued/running.

    Looks in squeue first and then in sacct. If sacct cannot be queried, be
    conservative and return True to avoid duplicate submissions. Without a
    scheduler (or in --test mode) nothing can be active.
    """
    if options.test or shutil.which("sacct") is None:
        return False

    try:
        squeue_info = get_squeue_output(run_squeue())
        if not squeue_info[squeue_info["JobName"] == jobname].empty:
            return True
    except Exception:
        log.debug("job_is_active: squeue not usable, falling back to sacct", exc_info=True)

    try:
        sacct_info = get_sacct_output(run_sacct())
    except Exception:
        log.warning(f"Could not query sacct for {jobname}; assuming it is active to avoid duplicates.")
        return True

    states = set(sacct_info.loc[sacct_info["JobName"] == jobname, "State"].astype(str))
    return bool(states & ACTIVE_STATES)


def get_active_jobid(jobname: str) -> Optional[str]:
    """Return the id of the latest queued/running job with this name (or None)."""
    try:
        sacct_info = get_sacct_output(run_sacct())
        jobs = sacct_info[
            (sacct_info["JobName"] == jobname)
            & (sacct_info["State"].astype(str).isin(ACTIVE_STATES))
        ]
        return str(int(jobs["JobID"].max())) if not jobs.empty else None
    except Exception:
        log.debug(f"get_active_jobid: could not determine job id for {jobname}", exc_info=True)
        return None


def get_last_job_state(jobname: str) -> Optional[str]:
    """Return the sacct state of the most recent job with this name (or None)."""
    try:
        sacct_info = get_sacct_output(run_sacct())
        jobs = sacct_info[sacct_info["JobName"] == jobname]
        if jobs.empty:
            return None
        last = jobs[jobs["JobID"] == jobs["JobID"].max()]
        return str(last.iloc[0]["State"])
    except Exception:
        log.debug(f"get_last_job_state: could not query state of {jobname}", exc_info=True)
        return None


def array_job_status(sacct_df, jobname: str) -> Optional[int]:
    """
    Overall status of an array job from a sacct DataFrame.

    Returns
    -------
    0 if all tasks are COMPLETED, 1 if any task is in a bad terminal state,
    None if some task is still RUNNING/PENDING/COMPLETING or no entry is found.
    """
    if sacct_df is None or sacct_df.empty:
        return None

    jobs = sacct_df[sacct_df["JobName"] == jobname]
    if jobs.empty:
        return None

    states = set(jobs["State"].astype(str))
    if states & BAD_STATES:
        return 1
    if states & ACTIVE_STATES:
        return None
    return 0


# ---------------------------------------------------------------------------
# Sequence objects <-> queue information
# ---------------------------------------------------------------------------
def update_job_info(sequence_list):
    """
    Update the SLURM information (jobid, state, cputime, exit, tries, action)
    of each sequence.
    """
    if options.test:
        return

    try:
        set_queue_values(
            sacct_info=get_sacct_output(run_sacct()),
            squeue_info=get_squeue_output(run_squeue()),
            sequence_list=sequence_list,
        )
    except Exception:
        log.exception("Failed to update SLURM job information")


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
