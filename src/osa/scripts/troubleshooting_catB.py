import os
import re
import troubleshooting_utils as utils
from osa.configs.config import cfg
from pathlib import Path
from osa.utils import osa_utils

# --- KNOWN ERROR DICTIONARY ---
KNOWN_ERRORS = {
    re.escape("Could not find compatible EventSource for input_url"): {
        "tag": "ERROR: Could not find compatible EventSource",
        "msg": "Remove all logs from Cat-B and relaunch the job.",
        "error_id": 1
    },
    re.escape("!!! No calibration events in the output file !!!"): {
        "tag": "CRITICAL: No calibration events in output",
        "msg": "Discard if subruns < 20.",
        "error_id": 2
    },
    re.escape("os.symlink(target, self, target_is_directory)"): {
        "tag": "Symlink Error",
        "msg": "Pro link is not created",
        "error_id": 3
    },
    re.escape("Number of subruns with low statistics: 1 - removed from pedestal median calculation"): {
        "tag": "Low statistics warning",
        "msg": "Discard if subruns < 20",
        "error_id": 4
    },
    re.escape("Calibration file from run {calibration_run} not found"): {
        "tag": "Missing Calibration file",
        "msg": "Check Cat-A calibration and pro link",
        "error_id": 5
    },
    re.escape("error message = 'Resource temporarily unavailable'"): {
        "tag": "error message = 'Resource temporarily unavailable'",
        "msg": "Remove all logs from Cat-B and relaunch the job.",
        "error_id": 6
    }
}

def finalize_action(job_id, success, logger_func, success_msg, fail_msg):
    """Logs results and saves job ID on success."""
    if success:
        logger_func(f"   |__ ✅ SUCCESS: {success_msg}")
        if utils.save_processed_job_id(job_id):
            logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
        return True
    logger_func(f"   |__ ⚠️ FAILURE: {fail_msg}")
    return False

def handle_ecsv_type_update(job_id, review_path, logger_func, subruns_limit, target_date):
    """Updates run_type to EDATA in ECSV."""
    run_id = utils.get_run_id_from_path(review_path)
    _, ecsv_path = utils.get_summary_info(target_date)
    success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA", subruns_limit=subruns_limit)
    finalize_action(job_id, success, logger_func, f"Run {run_id} set to EDATA.", "Could not update ECSV.")

def handle_log_cleanup(job_id, log_path, error_path, logger_func):
    """Deletes log and error files."""
    s1 = utils.delete_path(error_path)
    s2 = utils.delete_path(log_path)
    finalize_action(job_id, (s1 and s2), logger_func, "Logs removed.", "Failed to remove some logs.")

def handle_pro_link(job_id, log_path, error_path, logger_func, target_date):
    """Creates pro link if missing and cleans logs."""
    date_str, _ = utils.get_summary_info(target_date)
    CAT_A_CALIB_DIR = Path(cfg.get("LST1", "CAT_A_CALIB_DIR"))
    base_path = f'{CAT_A_CALIB_DIR}/{date_str}/'
    if not utils.is_link(base_path + "pro"):
        lstcam_env = Path(cfg.get("LST1", "CALIB_ENV"))
        lstcam_calib_version = osa_utils.get_lstcam_calib_version(lstcam_env)
        success = utils.run_command(f'ln -s {base_path}v{lstcam_calib_version} {base_path}pro')
        if success:
            handle_log_cleanup(job_id, log_path, error_path, logger_func)
        else:
            logger_func("   |__ ⚠️ FAILURE: Could not create pro link.")
    else:
        logger_func("   |__ ⚠️ SKIP: Pro link already exists.")

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date, handler):
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    review_path = log_path if job_name == "lstchain_find_tailcuts" else error_path
    logger_func(f"   |__ Log {log_path}")
    logger_func(f"   |__ Error {error_path}")
    # --- Timeout Handling ---
    if state == "TIMEOUT":
        if command in handler:
            utils.save_skipped_job_id(job_id)
            return None
        success = utils.run_command(command)
        return command if finalize_action(job_id, success, logger_func, "Memory increased & relaunched.", "Relaunch failed.") else None

    if not review_path or not os.path.exists(review_path):
        logger_func("Job skipped, can't find the log file")
        utils.save_skipped_job_id(job_id)
        return False

    # --- Pattern Matching ---
    try:
        with open(review_path, 'r', errors='ignore') as f:
            content = f.read()
            for pattern, details in KNOWN_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    logger_func(f"   |__ ❌ DETECTED: {details['tag']}")
                    eid = details['error_id']
                    if eid == 2:
                        handle_ecsv_type_update(job_id, review_path, logger_func, subruns_limit=20)
                    elif eid == 1 or eid == 3 or eid == 6:
                        handle_log_cleanup(job_id, log_path, error_path, logger_func)
                        success = utils.run_command(command)
                        return command if finalize_action(job_id, success, logger_func, "Memory increased & relaunched.", "Relaunch failed.") else None
                    elif eid == 4:
                        handle_ecsv_type_update(job_id, review_path, logger_func, start_date)
                    elif eid == 5:
                        handle_pro_link(job_id, log_path, error_path, logger_func, start_date)
                    return None # Action taken
    except Exception as e:
        logger_func(f"   |__ ❌ EXCEPTION: {str(e)}")

    return None, None
