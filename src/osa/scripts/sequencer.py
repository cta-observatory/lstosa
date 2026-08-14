#!/usr/bin/env python3
"""
Sequencer: orchestrates r0->dl1 arrays, per-run CatB/tailcuts pilots and dl1ab arrays.

Behavior:
 - For each DATA run:
   * submit r0->dl1 array job (if not completed/active)
   * submit CatB/tailcuts pilot dependent on r0 (if needed)
   * submit dl1ab array dependent on CatB (if needed) or r0 (if no CatB)
 - Keeps per-subrun history entries as before (scripts append per-subrun lines).
 - Produces a textual sequencer table snapshot (sequencer_table.txt) in options.directory
   and a timestamped copy in options.log_directory.
 - Array stdout/stderr filenames include subrun (%a) and array job id (%A).
 - Honor --simulate, --test and --force-submit.
"""
import warnings
import logging
import os
import sys
import subprocess as sp
import datetime
from pathlib import Path
import re
from osa.processing_plan import build_processing_plan
from decimal import Decimal
from typing import Optional
from osa.paths import get_dl1_prod_id_and_config

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger
from osa.utils.cliopts import sequencer_cli_parsing
from osa.utils.utils import gettag, date_to_iso, date_to_dir, get_lstchain_version
from osa.nightsummary.nightsummary import run_summary_table
from osa.nightsummary.extract import build_sequences
from osa.veto import get_veto_list, get_closed_list
from osa.paths import (
    analysis_path,
    catB_closed_file_exists,
    get_drive_file,
    get_major_version,
    get_summary_file,
    destination_dir,
)
from osa.job import run_sacct, get_sacct_output, run_squeue, get_squeue_output, set_queue_values, prepare_jobs, submit_jobs, are_all_jobs_correctly_finished

warnings.filterwarnings("ignore", message="pkg_resources is deprecated as an API.*", category=UserWarning)

log = myLogger(logging.getLogger(__name__))


def _safe_write_text(path: Path, content: str, mode: str = "w", encoding: str = "utf-8"):
    """
    Ensure parent exists and write content to path. Log and raise on failure.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.exception(f"Could not create directory for {path.parent}: {e}")
        raise
    try:
        log.debug(f"Writing file {path} (parent exists: {path.parent.exists()})")
        with path.open(mode, encoding=encoding) as fh:
            fh.write(content)
    except Exception as e:
        log.exception(f"Failed to write file {path}: {e}")
        raise


def _sbatch_submit(script_path: Path, dependency: Optional[str] = None, simulate: bool = False) -> Optional[str]:
    cmd = ["sbatch", "--parsable"]
    if dependency:
        cmd.extend([f"--dependency=afterok:{dependency}"])
    cmd.append(str(script_path))
    if simulate:
        log.info(f"[SIMULATE] Would run: {' '.join(cmd)}")
        return None
    try:
        proc = sp.run(cmd, capture_output=True, text=True, check=True)
        jobid = proc.stdout.strip()
        log.info(f"sbatch submitted: {script_path.name} -> job {jobid}")
        return jobid
    except sp.CalledProcessError as e:
        log.exception(f"sbatch failed for {script_path}: {e}; stdout: {e.stdout}; stderr: {e.stderr}")
        return None


def _make_script_header(job_name: str, work_dir: Path, account: str, mem: str = "12G", array_spec: Optional[str] = None) -> str:
    """
    Build SBATCH header. If array_spec is provided we use %a (task id) and %A (array id)
    in output/error filenames to include subrun and array-job id.
    """
    header = "#!/usr/bin/env python3\n\n"
    header += f"#SBATCH --job-name={job_name}\n"
    header += f"#SBATCH --chdir={str(work_dir)}\n"
    if array_spec:
        header += f"#SBATCH --array={array_spec}\n"
        header += f"#SBATCH --output=log/{job_name}.%a_jobid_%A.out\n"
        header += f"#SBATCH --error=log/{job_name}.%a_jobid_%A.err\n"
    else:
        header += f"#SBATCH --output=log/{job_name}_%j.out\n"
        header += f"#SBATCH --error=log/{job_name}_%j.err\n"
    header += f"#SBATCH --account={account}\n"
    header += f"#SBATCH --mem={mem}\n\n"
    return header


def format_sequence_table(sequence_list) -> str:
    """
    Build the same table as report_sequences but return it as a formatted string.
    (Used to save a textual snapshot of the sequencer output.)
    """
    header = [
        "Tel",
        "Seq",
        "Parent",
        "Type",
        "Run",
        "Subruns",
        "Source",
        "Action",
        "Tries",
        "JobID",
        "State",
        "CPU_time",
        "Exit",
    ]
    if options.tel_id in ["LST1", "LST2"]:
        header.extend(("DL1%", "MUONS%", "CAT-B", "DL1AB%", "DATACHECK%", "DL2%"))
    matrix = [header]
    for sequence in sequence_list:
        row_list = [
            getattr(sequence, "telescope", None),
            getattr(sequence, "seq", None),
            getattr(sequence, "parent", None),
            getattr(sequence, "type", None),
            getattr(sequence, "run", None),
            getattr(sequence, "subruns", None),
            getattr(sequence, "source_name", None),
            getattr(sequence, "action", None),
            getattr(sequence, "tries", None),
            getattr(sequence, "jobid", None),
            getattr(sequence, "state", None),
            getattr(sequence, "cputime", None),
            getattr(sequence, "exit", None),
        ]
        if getattr(sequence, "type", None) in ["DRS4", "PEDCALIB"]:
            row_list.extend((None, None, None, None, None, None))
        elif getattr(sequence, "type", None) == "DATA":
            dl1s = getattr(sequence, "dl1status", None)
            muons = getattr(sequence, "muonstatus", None)
            catb = getattr(sequence, "catbstatus", None)
            dl1ab = getattr(sequence, "dl1abstatus", None)
            datacheck = getattr(sequence, "datacheckstatus", None)
            dl2 = getattr(sequence, "dl2status", None)
            row_list.extend((dl1s, muons, catb, dl1ab, datacheck, dl2))
        matrix.append(row_list)

    # build padded string; convert None->"" for display
    padding = int(cfg.get("OUTPUT", "PADDING"))
    max_field_length = []
    for row in matrix:
        for j, col in enumerate(row):
            col_str = "" if col is None else str(col)
            length = len(col_str)
            if len(max_field_length) <= j:
                max_field_length.append(length)
            elif length > max_field_length[j]:
                max_field_length[j] = length

    out_lines = []
    for row in matrix:
        stringrow = ""
        rpadding = padding * " "
        for j, col in enumerate(row):
            col_str = "" if col is None else str(col)
            lpad = (max_field_length[j] - len(col_str)) * " "
            # keep numeric alignment as before
            if isinstance(col, int):
                stringrow += f"{lpad}{col}{rpadding}"
            else:
                stringrow += f"{col_str}{lpad}{rpadding}"
        out_lines.append(stringrow)
    return "\n".join(out_lines) + "\n"


def _determine_array_job_status(sacct_df, jobname: str) -> Optional[int]:
    """
    Determine overall array job status using sacct DataFrame for a JobName.
    Returns:
      - 0 if all entries are COMPLETED
      - 1 if any terminal bad state seen (FAILED/CANCELLED/TIMEOUT/OUT_OF_MEMORY)
      - None if any entry is RUNNING/PENDING/COMPLETING or if no entries found
    """
    if sacct_df is None or sacct_df.empty:
        return None

    jobs = sacct_df[sacct_df["JobName"] == jobname]
    if jobs.empty:
        return None

    states = set(jobs["State"].astype(str))

    bad = {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY"}
    if any(s in bad for s in states):
        return 1

    running_like = {"RUNNING", "PENDING", "COMPLETING"}
    if any(s in running_like for s in states):
        return None

    # otherwise consider success
    return 0


def _write_r0_script(seq, work_dir: Path, account: str, simulate: bool) -> Path:
    """Write r0->dl1 array script."""

    run_id = seq.run
    job_name = f"{options.tel_id}_{run_id:05d}"
    script_path = work_dir / f"sequence_{options.tel_id}_{run_id:05d}.py"

    script_path.parent.mkdir(parents=True, exist_ok=True)

    subruns_count = max(0, seq.subruns - 1)
    array_spec = f"0-{subruns_count}" if subruns_count >= 0 else None

    flat_date = date_to_dir(options.date)

    args = ["datasequence"]

    if options.verbose:
        args.append("-v")

    if simulate:
        args.append("-s")

    if options.configfile:
        args.extend(
            ["--config", str(Path(options.configfile).resolve())]
        )

    args.append(f"--input-state={options.input_state}")
    args.append("--no-dl1ab")

    args.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
            f"--drive-file={get_drive_file(flat_date)}",
            f"--run-summary={get_summary_file(flat_date)}",
        )
    )

    plan = build_processing_plan(options.input_state)

    if plan.needs_calibration:
        args.extend(
            (
                f"--drs4-pedestal-file={seq.drs4_file}",
                f"--time-calib-file={seq.time_calibration_file}",
                f"--pedcal-file={seq.calibration_file}",
                f"--systematic-correction-file={seq.systematic_correction_file}",
            )
        )

    try:
        from osa.paths import (
            pedestal_ids_file_exists,
            get_pedestal_ids_file,
        )

        if pedestal_ids_file_exists(run_id):
            pedfile = get_pedestal_ids_file(run_id, flat_date)
            args.append(f"--pedestal-ids-file={pedfile}")

    except Exception:
        pass

    header = _make_script_header(
        job_name,
        work_dir,
        account,
        array_spec=array_spec,
    )

    content = header

    content += "import os\n"
    content += "import subprocess\n"
    content += "import sys\n"
    content += "import tempfile\n\n"

    content += "if 'SLURM_ARRAY_TASK_ID' in os.environ:\n"
    content += "    subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    content += "else:\n"
    content += "    subruns = 0\n\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += "    os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    content += "    proc = subprocess.run([\n"

    for a in args:
        content += f"        {a!r},\n"

    content += f"        f'{run_id:05d}.{{subruns:04d}}',\n"
    content += f"        {options.tel_id!r}\n"
    content += "    ])\n\n"

    content += "sys.exit(proc.returncode)\n"

    _safe_write_text(script_path, content)

    try:
        script_path.chmod(0o755)
    except Exception:
        log.warning(f"Could not chmod {script_path}")

    log.debug(f"Wrote r0 script {script_path}")

    return script_path


def _write_catb_pilot_script(run_id: int, work_dir: Path, account: str, simulate: bool) -> Path:
    """Write per-run CatB/tailcuts pilot script (non-array)."""
    job_name = f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
    script_path = work_dir / f"sequence_{options.tel_id}_{run_id:05d}_catb_tailcuts.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)

    header = _make_script_header(job_name, work_dir, account, array_spec=None)

    argv = [
        "catb_tailcuts_pipeline",
        f"--date={date_to_iso(options.date)}",
        f"--input-state={options.input_state}",
    ]
    if options.verbose:
        argv.append("--verbose")
    if simulate:
        argv.append("--simulate")
    if options.configfile:
        argv.extend(["--config", str(Path(options.configfile).resolve())])
    if getattr(options, "overwrite_catB", False):
        argv.append("--overwrite-catB")
    if getattr(options, "overwrite_tailcuts", False):
        argv.append("--overwrite-tailcuts")
    argv.append(str(run_id))
    argv.append(options.tel_id)

    content = header
    content += "import subprocess, sys\n\n"
    content += "proc = subprocess.run([\n"
    for a in argv:
        content += f"    {a!r},\n"
    content += "])\n"
    content += "rc = proc.returncode\n"
    # The catb pipeline itself writes detailed history entries; we don't duplicate here.
    content += "sys.exit(rc)\n"

    _safe_write_text(script_path, content)
    try:
        script_path.chmod(0o755)
    except Exception:
        log.warning(f"Could not chmod {script_path}")
    log.debug(f"Wrote CatB pilot script {script_path}")
    return script_path

def _write_dl1ab_wrapper_script(
    run_id: int,
    work_dir: Path,
    account: str,
    simulate: bool,
    subruns: int,
    dl1_prod_id: str,
    dl1b_config: str,
) -> Path:
    """Write dl1ab array script."""

    job_name = f"{options.tel_id}_dl1ab_{run_id:05d}"

    script_path = (
        work_dir
        / f"sequence_{options.tel_id}_{run_id:05d}_dl1ab.py"
    )

    script_path.parent.mkdir(parents=True, exist_ok=True)

    array_spec = (
        f"0-{max(0, subruns - 1)}"
        if subruns > 0
        else "0-0"
    )

    header = _make_script_header(
        job_name,
        work_dir,
        account,
        array_spec=array_spec,
    )

    content = header

    content += "import os\n"
    content += "import subprocess\n"
    content += "import sys\n"
    content += "import tempfile\n\n"

    content += "if 'SLURM_ARRAY_TASK_ID' in os.environ:\n"
    content += "    subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    content += "else:\n"
    content += "    subruns = 0\n\n"

    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += "    os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    content += "    proc = subprocess.run([\n"

    content += "        'datasequence',\n"

    if options.verbose:
        content += "        '-v',\n"

    if simulate:
        content += "        '-s',\n"

    if options.configfile:
        content += (
            f"        '--config', "
            f"{str(Path(options.configfile).resolve())!r},\n"
        )

    content += f"        '--input-state={options.input_state}',\n"
    content += f"        '--date={date_to_iso(options.date)}',\n"
    content += f"        '--prod-id={options.prod_id}',\n"
    content += f"        '--dl1b-config={dl1b_config}',\n"
    content += f"        '--dl1-prod-id={dl1_prod_id}',\n"
    content += f"        f'{run_id:05d}.{{subruns:04d}}',\n"
    content += f"        {options.tel_id!r}\n"

    content += "    ])\n\n"
    content += "sys.exit(proc.returncode)\n"

    _safe_write_text(script_path, content)

    try:
        script_path.chmod(0o755)
    except Exception:
        log.warning(f"Could not chmod {script_path}")

    log.debug(f"Wrote dl1ab wrapper script {script_path}")

    return script_path






def _job_active_in_sacct(jobname_pattern: str) -> bool:

    try:
        squeue_output = run_squeue()
        squeue_info = get_squeue_output(squeue_output)

        jobs = squeue_info[
            squeue_info["JobName"] == jobname_pattern
        ]

        if not jobs.empty:
            return True

    except Exception:
        pass

    try:
        sacct_output = run_sacct()
        sacct_info = get_sacct_output(sacct_output)

        jobs = sacct_info[
            sacct_info["JobName"] == jobname_pattern
        ]

        states = set(jobs["State"].astype(str))

        return any(
            s in ("RUNNING", "PENDING", "COMPLETING")
            for s in states
        )

    except Exception:
        return True

    return False



def update_job_info(sequence_list):
    """
    Update SLURM information associated with each sequence.

    Fills fields such as:
        jobid
        state
        cputime
        exit
        tries
        action
    """

    if options.test:
        return

    try:
        sacct_output = run_sacct()
        squeue_output = run_squeue()

        set_queue_values(
            sacct_info=get_sacct_output(sacct_output),
            squeue_info=get_squeue_output(squeue_output),
            sequence_list=sequence_list,
        )

    except Exception:
        log.exception("Failed to update SLURM job information")


def get_status_for_sequence(sequence, data_level) -> int:
    """
    Get number of files produced for a given sequence and data level.

    Parameters
    ----------
    sequence
    data_level : str
        Options: 'CALIB', 'DL1', 'DL1AB', 'DATACHECK', 'MUON' or 'DL2'

    Returns
    -------
    number_of_files : int
    """
    try:
        if data_level == "DL1AB":
            directory = options.directory / sequence.dl1_prod_id
            files = list(directory.glob(f"dl1_LST-1*{sequence.run}*.h5"))
        elif data_level == "DL2":
            directory = destination_dir(concept="DL2", create_dir=False, dl2_prod_id=sequence.dl2_prod_id)
            files = list(directory.glob(f"dl2_LST-1*{sequence.run}*.h5"))
        elif data_level == "DATACHECK":
            # try both options.directory/<dl1_prod_id> and DATACHECK destination_dir
            try:
                directory = options.directory / sequence.dl1_prod_id
                files = list(directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))
            except Exception:
                files = []
            try:
                alternative_directory = destination_dir(concept="DATACHECK", create_dir=False, dl1_prod_id=sequence.dl1_prod_id)
                files += list(alternative_directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))
            except Exception:
                pass
        else:
            prefix = cfg.get("PATTERN", f"{data_level}PREFIX")
            suffix = cfg.get("PATTERN", f"{data_level}SUFFIX")
            files = list(options.directory.glob(f"{prefix}*{sequence.run}*{suffix}"))
    except AttributeError:
        return 0
    except Exception:
        # On any unexpected error, log once and return 0
        log.debug(f"get_status_for_sequence: unexpected error for run {getattr(sequence,'run',None)} and level {data_level}", exc_info=True)
        return 0

    return len(files)


def check_catB_status(seq):
    """
    Determine catB status for a DATA sequence:
      - CLOSED if a catB*<run>*.closed file exists in options.directory
      - otherwise, if a catB log exists in options.log_directory, extract job id and query sacct to get job state.
    """
    catbstatus = "None"

    if seq.type == "DATA":
        directory = options.directory

        closed_files = list(directory.glob(f"catB*{seq.run}*.closed"))
        if closed_files:
            catbstatus = "CLOSED"
        else:
            log_files = list(options.log_directory.glob(f"catB_calibration_{seq.run}_*.err"))
            if log_files:
                filename = sorted(log_files)[-1].name
                match = re.search(f"catB_calibration_{seq.run}_(\\d+).err", filename)
                if match:
                    job_id = match.group(1)
                    try:
                        sacct_output = run_sacct(job_id)
                        sacct_info = get_sacct_output(sacct_output)
                        if not sacct_info.empty:
                            catbstatus = sacct_info.iloc[0]["State"]
                    except Exception:
                        log.debug(f"check_catB_status: could not query sacct for job {job_id}", exc_info=True)
    return catbstatus


def update_sequence_status(seq_list):
    """
    Update the percentage of files produced of each type (calibration, DL1,
    DATACHECK, MUON and DL2) for every run considering the total number of subruns.

    Parameters
    ----------
    seq_list
        List of sequences of a given night corresponding to each run.
    """
    for seq in seq_list:
        try:
            if seq.type == "PEDCALIB":
                denom = seq.subruns if seq.subruns else 1
                seq.calibstatus = int(Decimal(get_status_for_sequence(seq, "CALIB") * 100) / denom)
            elif seq.type == "DATA":
                denom = seq.subruns if seq.subruns else 1
                seq.dl1status = int(Decimal(get_status_for_sequence(seq, "DL1") * 100) / denom)
                seq.dl1abstatus = int(Decimal(get_status_for_sequence(seq, "DL1AB") * 100) / denom)
                seq.datacheckstatus = int(Decimal(get_status_for_sequence(seq, "DATACHECK") * 100) / denom)
                seq.muonstatus = int(Decimal(get_status_for_sequence(seq, "MUON") * 100) / denom)
                # For DL2 keep old behaviour: count files and multiply by 100 (no division by subruns)
                seq.dl2status = int(Decimal(get_status_for_sequence(seq, "DL2") * 100))
                seq.catbstatus = check_catB_status(seq)
        except Exception:
            log.exception(f"Could not update status for sequence run {getattr(seq,'run',None)}")


def _write_run_summary_line(run_dir: Path, tel: str, run: int, kind: str, status: int):
    """
    Helper to append a run summary line for array statuses (keeps compatibility
    with previous behaviour of writing single-line summaries for array results).
    """
    try:
        summary_file = run_dir / f"{kind.lower()}_{tel}_{run:05d}.status"
        with summary_file.open("a") as fh:
            fh.write(f"{status}\n")
    except Exception:
        log.debug(f"Could not write run summary line for {kind} {tel} {run}")




def single_process(telescope: str):
    sequencer_cli_parsing()  # ensure options set
    options.tel_id = telescope
    options.directory = analysis_path(options.tel_id)
    options.log_directory = options.directory / "log"

    # ensure base directories exist so script files can be written
    options.directory.mkdir(parents=True, exist_ok=True)
    if not options.simulate:
        options.log_directory.mkdir(parents=True, exist_ok=True)

    log.debug(f"options.directory = {options.directory} (exists={options.directory.exists()}, writable={os.access(str(options.directory), os.W_OK)})")
    log.info(f"Starting sequencer for {options.tel_id} on date {date_to_iso(options.date)} (input_state={options.input_state})")

    summary_table = run_summary_table(options.date)
    if len(summary_table) == 0:
        log.warning("No runs found for this date. Nothing to do.")
        return []

    sequence_list = build_sequences(options.date)
    get_veto_list(sequence_list)
    get_closed_list(sequence_list)

    try:
        update_job_info(sequence_list)
    except Exception:
        log.exception("Could not update job info")

    # Update statuses from disk products (DL1, MUON, DATACHECK, DL2) and Cat-B
    try:
        update_sequence_status(sequence_list)
    except Exception:
        log.exception("Could not update sequence status")

    # obtain sacct info once and use it for summaries / state decisions
    try:
        sacct_output = run_sacct()
        sacct_info = get_sacct_output(sacct_output)
    except Exception:
        sacct_info = None

    # Build run-level summaries for arrays when possible (do not duplicate)
    for seq in sequence_list:
        if seq.type != "DATA":
            continue
        run = seq.run
        tel = options.tel_id
        run_dir = options.directory

        # r0 array summary
        jobname_r0 = f"{tel}_{run:05d}"
        status_r0 = _determine_array_job_status(sacct_info, jobname_r0)
        if status_r0 is not None:
            _write_run_summary_line(run_dir, tel, run, "R0_ARRAY", status_r0)

        # dl1ab array summary
        jobname_dl1ab = f"{tel}_dl1ab_{run:05d}"
        status_dl1ab = _determine_array_job_status(sacct_info, jobname_dl1ab)
        if status_dl1ab is not None:
            _write_run_summary_line(run_dir, tel, run, "DL1AB_ARRAY", status_dl1ab)

    account = cfg.get("SLURM", "ACCOUNT")

    for seq in sequence_list:
        if seq.type != "DATA":
            continue

        run_id = seq.run
        jobname_r0 = f"{options.tel_id}_{run_id:05d}"
        jobname_catb = f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
        jobname_dl1ab = f"{options.tel_id}_dl1ab_{run_id:05d}"

        # check r0 completion via history (per-subrun history existence)
        history_files = sorted(options.directory.glob(f"sequence_{options.tel_id}_{run_id:05d}.*.history"))
        r0_completed = True
        if not history_files:
            r0_completed = False
        else:
            for hf in history_files:
                try:
                    lines = hf.read_text().splitlines()
                except Exception:
                    r0_completed = False
                    break
                found = any("lstchain_data_r0_to_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    r0_completed = False
                    break

        jobid_r0 = None
        if not r0_completed:
            if _job_active_in_sacct(jobname_r0):
                log.info(f"r0->dl1 already active for run {run_id:05d} (jobname {jobname_r0}), skipping r0 submit.")
                try:
                    sacct_output = run_sacct()
                    sacct_df = get_sacct_output(sacct_output)
                    jobs_run = sacct_df[sacct_df["JobName"] == jobname_r0]
                    jobid_r0 = str(int(jobs_run["JobID"].max())) if not jobs_run.empty else None
                except Exception:
                    jobid_r0 = None
            else:
                r0_script = _write_r0_script(seq, options.directory, account, options.simulate)
                jobid_r0 = _sbatch_submit(r0_script, dependency=None, simulate=options.simulate)
        else:
            log.debug(f"r0->dl1 already completed for run {run_id:05d}.")

        # decide CatB/tailcuts need
        need_catb = cfg.getboolean("lstchain", "apply_catB_calibration") and not catB_closed_file_exists(run_id)
        tailcuts_cfg = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
        need_tailcuts = (not cfg.getboolean("lstchain", "apply_standard_dl1b_config")) and (not tailcuts_cfg.exists())

        jobid_catb = None
        if need_catb or need_tailcuts:
            if _job_active_in_sacct(jobname_catb):
                log.info(f"CatB pilot already active for run {run_id:05d}, skipping CatB submit.")
                try:
                    sacct_output = run_sacct()
                    sacct_df = get_sacct_output(sacct_output)
                    jobs_run = sacct_df[sacct_df["JobName"] == jobname_catb]
                    jobid_catb = str(int(jobs_run["JobID"].max())) if not jobs_run.empty else None
                except Exception:
                    jobid_catb = None
            else:
                dep = jobid_r0
                if dep is None and not options.force_submit and not r0_completed:
                    log.info(f"No r0 job visible yet for run {run_id:05d}; skipping CatB submission until r0 is present (or use --force-submit).")
                else:
                    catb_script = _write_catb_pilot_script(run_id, options.directory, account, options.simulate)
                    jobid_catb = _sbatch_submit(catb_script, dependency=dep, simulate=options.simulate)
        else:
            log.debug(f"No CatB/tailcuts needed for run {run_id:05d}.")

        # check fully processed (check_dl1) using per-subrun history
        fully_processed = True
        if not history_files:
            fully_processed = False
        else:
            for hf in history_files:
                try:
                    lines = hf.read_text().splitlines()
                except Exception:
                    fully_processed = False
                    break
                found = any("lstchain_check_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    fully_processed = False
                    break

        if fully_processed:
            log.info(f"Run {run_id:05d} already fully processed, skipping dl1ab.")
            continue

        if _job_active_in_sacct(jobname_dl1ab):
            log.info(f"dl1ab already active for run {run_id:05d}, skipping dl1ab submit.")
            continue

        # DECIDE DEPENDENCY FOR DL1AB
        dep_for_dl1 = None
        if need_catb:
            if catB_closed_file_exists(run_id):
                dep_for_dl1 = None
                log.debug(f"CatB already closed for run {run_id:05d}; submitting dl1ab without dependency.")
            elif jobid_catb:
                dep_for_dl1 = jobid_catb
            else:
                if options.force_submit and jobid_r0:
                    dep_for_dl1 = jobid_r0
                    log.warning(f"Force-submitting dl1ab for run {run_id:05d} with dependency on r0 ({jobid_r0}) even though CatB is not yet closed/active.")
                else:
                    log.info(f"CatB required for run {run_id:05d} but no catB job or .closed found; skipping dl1ab.")
                    continue
        else:
            if jobid_r0:
                dep_for_dl1 = jobid_r0
            elif r0_completed:
                dep_for_dl1 = None
                log.debug(f"r0 already completed for run {run_id:05d}; submitting dl1ab without dependency.")
            else:
                if options.force_submit:
                    dep_for_dl1 = None
                    log.warning(f"Force-submitting dl1ab for run {run_id:05d} without dependency.")
                else:
                    log.info(f"No r0 job available and r0 not completed for run {run_id:05d}; skipping dl1ab.")
                    continue
                 
        dl1_prod_id, dl1b_config = get_dl1_prod_id_and_config(run_id)
        dl1ab_script = _write_dl1ab_wrapper_script(run_id, options.directory, account, options.simulate, seq.subruns, dl1_prod_id, dl1b_config)
        _sbatch_submit(dl1ab_script, dependency=dep_for_dl1, simulate=options.simulate)

    # At the end, save a textual snapshot of the sequencer table
    try:
        # Ensure statuses reflect disk products right before printing/saving
        try:
            update_sequence_status(sequence_list)
        except Exception:
            log.exception("Could not refresh sequence status before writing table")

        table_str = format_sequence_table(sequence_list)

        # ALWAYS print table to stdout so it's visible even with --simulate
        print(table_str)

        table_file = options.directory / "sequencer_table.txt"
        if not options.simulate:
            with open(table_file, "w") as fh:
                fh.write(table_str)
            stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            logfile = options.log_directory / f"sequencer_table_{stamp}.log"
            with open(logfile, "w") as fh:
                fh.write(table_str)
            log.info(f"Saved sequencer table to {table_file} and {logfile}")
        else:
            log.info("[SIMULATE] Would write sequencer table to disk")
    except Exception:
        log.exception("Could not write sequencer table to disk")

    return sequence_list


def main():
    sequencer_cli_parsing()  # parse CLI into options
    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    single_array = ["LST1", "LST2"]
    tag = gettag()
    log.info(f"=================================== Starting sequencer.py at {datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC for LST, Telescope: {options.tel_id}, Date: {date_to_iso(options.date)} ===================================")
    if options.tel_id in single_array:
        single_process(options.tel_id)
    else:
        log.error("Process mode not supported yet")


if __name__ == "__main__":
    main()
