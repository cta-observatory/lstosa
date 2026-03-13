import glob
import os
import re
import troubleshooting_utils as utils
from osa.configs.config import cfg
from pathlib import Path

# --- KNOWN ERROR DICTIONARY ---
KNOWN_ERRORS = {
    re.escape("Table /dl1/monitoring/telescope/catB/calibration already exists"): {
        "tag": "Table /dl1/monitoring/telescope/catB/calibration already exists",
        "msg": "Pending",
        "error_id": 1
    },
    re.escape("tables.exceptions.NoSuchNodeError: ... /pedestal"): {
        "tag": "NoSuchNodeError: missing pedestal child",
        "msg": "Discard last subrun if failing.",
        "error_id": 2
    },
    re.escape("x_new is above the interpolation range's maximum value"): {
        "tag": "ValueError: interpolation range exceeded",
        "msg": "Run must be discarded.",
        "error_id": 3
    },
    re.escape("No such file or directory: 'datasequence'"): {
        "tag": "Missing 'datasequence' file",
        "msg": "Command must be relaunched",
        "error_id": 4
    },
    r"lstcam_calib_onsite_create_drs4_pedestal_file.*Output file exists already": {
        "action": "Remove the DRS4 file",
        "description": "the DRS4 file already exists",
        "error_id": 5
    },
    re.escape("error message = 'Resource temporarily unavailable'"): {
        "tag": "error message = 'Resource temporarily unavailable'",
        "msg": "Command must be relaunched",
        "error_id": 6
    }
}

# --- HELPERS ---

def log_and_save(job_id, success, logger_func, success_msg, fail_msg):
    if success:
        logger_func(f"   |__ ✅ SUCCESS: {success_msg}")
        if utils.save_processed_job_id(job_id):
            logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
        return True
    logger_func(f"   |__ ⚠️ FAILURE: {fail_msg}")
    return False

def extract_ids_and_paths(job_id, job_name, log_path, error_path):
    match1 = re.search(r'^(\d+)_(\d+)$', job_id)
    match2 =  re.search(r'LST1_(\d+)(?:_(\d+))?', job_name)
    run_id, subrun_id = 0, 0
    subrun_id = "*" # Comodín por si falla el match

    if match1:
        run_id = match1.group(1)
        subrun_id = match1.group(2) if match1.group(2) else "*" # Usamos * si es None
    if match2:
        run_id = match2.group(1)
        subrun_id = match2.group(2) if match2.group(2) else "*" # Usamos * si es None

    if subrun_id == "*":
        search_pattern_err = error_path.replace(".%4a", ".*")
        search_pattern_log = log_path.replace(".%4a", ".*")
    else:
        subrun_fmt = f"{int(subrun_id):04d}" if len(subrun_id) < 5 else subrun_id
        search_pattern_err = error_path.replace("%4a", subrun_fmt)
        search_pattern_log = log_path.replace("%4a", subrun_fmt)

    found_err = glob.glob(search_pattern_err)
    found_log = glob.glob(search_pattern_log)
    final_error_path = found_err[0] if found_err else search_pattern_err
    final_log_path = found_log[0] if found_log else search_pattern_log

    return run_id, subrun_id, final_log_path, final_error_path

# --- ACTION HANDLERS ---

def perform_relaunch(job_id, command, logger_func, handler, msg="Job relaunched"):
    """Generic memory increase and relaunch logic."""
    if command in handler:
        return None
    success = utils.increase_memory_and_relaunch(command, 30)
    return command if log_and_save(job_id, success, logger_func, msg, "Relaunch failed") else None

def handle_case_actions(error_id, job_id, run_id, subrun_id, command, logger_func, handler, target_date):
    """Routes specific error IDs to their logic."""
    _, ecsv_path = utils.get_summary_info(target_date)
    if error_id == 2:  # Discard last subrun
        last_sr = utils.get_ecsv_column_value(ecsv_path, run_id, 'n_subruns')
        if int(subrun_id) == int(last_sr):
            success = utils.update_ecsv_cell(ecsv_path, run_id, "n_subruns", int(subrun_id) - 1)
            log_and_save(job_id, success, logger_func, "Subruns decremented", "ECSV update failed")
        else:
            logger_func(f"   |__ ⚠️ SKIP: {subrun_id} is not the last subrun ({last_sr})")
    elif error_id == 3:  # Discard Run
        success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA")
        log_and_save(job_id, success, logger_func, "Run marked as EDATA", "ECSV update failed")
    elif error_id in [4, 6]:  # Relaunch only
        return perform_relaunch(job_id, command, logger_func, handler)
    elif error_id == 5:  # Delete DRS4 and relaunch
        summary_date, _ = utils.get_summary_info(target_date)
        pedestal_dir = Path(cfg.get("LST1", "CAT_A_PEDESTAL_DIR"))
        drs4_glob = f"{pedestal_dir}/{summary_date}/v0.1.1/drs4_pedestal*.h5"
        if utils.delete_path(drs4_glob):
            return perform_relaunch(job_id, command, logger_func, handler, "DRS4 deleted & relaunched")

    return None

# --- MAIN ENTRY POINT ---

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date, handler):
    """Main orchestrator for sequencer errors."""
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    run_id, subrun_id, log_path, error_path = extract_ids_and_paths(job_id, job_name, log_path, error_path)
    logger_func(f"   |__ Log {log_path}")
    logger_func(f"   |__ Error {error_path}")

    if state == "TIMEOUT":
        return perform_relaunch(job_id, command, logger_func, handler, "Timeout: memory increased")

    if not os.path.exists(log_path):
        logger_func("Job skipped, can't find the log file")
        utils.save_skipped_job_id(job_id)
        return None

    try:
        with open(error_path, 'r', errors='ignore') as f:
            content = f.read()
            for pattern, details in KNOWN_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    logger_func(f"   |__ ❌ DETECTED: {details.get('tag', details.get('description'))}")
                    return handle_case_actions(details['error_id'], job_id, run_id, subrun_id, command, logger_func, handler, start_date)
    except Exception as e:
        logger_func(f"   |__ ❌ EXCEPTION: {str(e)}")
        return None
