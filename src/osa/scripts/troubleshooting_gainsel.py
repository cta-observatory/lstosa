import os
import re
import troubleshooting_utils as utils
from datetime import datetime, timedelta

# --- KNOWN ERROR DICTIONARY ---
KNOWN_ERRORS = {
    re.escape("AttributeError: 'File' object has no attribute 'Events'"): {
        "tag": "AttributeError: 'File' object has no attribute 'Events'",
        "msg": "The run must be discarded if number of subruns < 20",
        "error_id": 1
    },
    re.escape("Error detail: Resource temporarily unavailable"): {
        "tag": "Error detail: Resource temporarily unavailable",
        "msg": "The job must be relaunched.",
        "error_id": 2
    },
    re.escape("fork: Cannot allocate memory"): {
        "tag": "MemoryError: Cannot allocate memory",
        "msg": "Increase --mem in sbatch and relaunch the job.",
        "error_id": 3
    },
    re.escape("RuntimeError: ERROR: unexpected offset value: 32768"): {
        "tag": "RuntimeError: ERROR: unexpected offset value: 32768",
        "msg": "Increase --mem in sbatch and relaunch the job.",
        "error_id": 4
    }
}

def process_memory_relaunch(job_id, command, review_path, logger_func, handler):
    """Helper to handle memory increases and job relaunches."""
    if command in handler:
        utils.save_skipped_job_id(job_id)
        return None

    run_id = utils.get_run_id_from_path(review_path)
    success = utils.increase_memory_and_relaunch(command, 20)

    if success:
        logger_func(f"   |__ ✅ SUCCESS: Run {run_id} memory updated.")
        if utils.save_processed_job_id(job_id):
            logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
        else:
            logger_func("   |__ ⚠️ ERROR: Could not write to job history.")
        return command
    logger_func("   |__ ⚠️ FUNCTIONAL FAILURE: Could not update the command.")
    return None

def process_ecsv_update(job_id, review_path, logger_func):
    """Helper to handle ECSV status updates for short runs."""
    run_id = utils.get_run_id_from_path(review_path)
    yesterday = datetime.now() - timedelta(days=1)
    summary_date = yesterday.strftime('%Y%m%d')
    ecsv_path = f'/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary/RunSummary_{summary_date}.ecsv'

    success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA", subruns_limit=20)

    if success:
        logger_func(f"   |__ ✅ SUCCESS: Run {run_id} updated to EDATA in ECSV.")
        if utils.save_processed_job_id(job_id):
            logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
        else:
            logger_func("   |__ ⚠️ ERROR: Could not write to job history.")
    else:
        logger_func("   |__ ⚠️ FUNCTIONAL FAILURE: Could not update ECSV.")

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date, handler):
    """Refactored handle_error to reduce cyclomatic complexity."""
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    review_path = log_path
    logger_func(f"   |__ Log {log_path}")
    logger_func(f"   |__ Error {error_path}")

    # Handle Timeouts immediately
    if state == "TIMEOUT":
        logger_func("   |__ ❌ DIAGNOSIS: TIMEOUT. 💡 ACTION: Increase --mem.")
        return process_memory_relaunch(job_id, command, review_path, logger_func, handler)

    if not review_path or not os.path.exists(review_path):
        logger_func("Job skipped, can't find the log file")
        utils.save_skipped_job_id(job_id)
        return None

    try:
        with open(review_path, 'r', errors='ignore') as f:
            content = f.read()
            for pattern, details in KNOWN_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    logger_func(f"   |__ ❌ DETECTED: {details['tag']}")
                    logger_func(f"   |__ 💡 SOLUTION: {details['msg']}")
                    err_id = details['error_id']
                    if err_id == 1:
                        process_ecsv_update(job_id, review_path, logger_func)
                        return None
                    if err_id in [2, 3, 4]:
                        return process_memory_relaunch(job_id, command, review_path, logger_func, handler)

    except Exception as e:
        logger_func(f"   |__ ❌ EXCEPTION: {str(e)}")

    return None
