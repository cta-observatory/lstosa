import os
import re
import troubleshooting_utils as utils
from datetime import datetime, timedelta

# --- KNOWN ERROR DICTIONARY ---
KNOWN_ERRORS = {
    r"LST1_\d{5,6}": {
        "tag": "ERROR [lstchain.CatBCalibrationHDF5Writer] (tool.run): Caught unexpected exception: Could not find compatible EventSource for input_url",
        "msg": "Remove all logs from Cat-B and relaunch the job.",
        "error_id": 1
    },
    re.escape("Table /dl1/monitoring/telescope/catB/calibration already exists in output file, use append or overwrite"): {
        "tag": "Table /dl1/monitoring/telescope/catB/calibration already exists in output file, use append or overwrite",
        "msg": "Pending",
        "error_id": 2
    },
    re.escape("tables.exceptions.NoSuchNodeError: group ``/`` does not have a child named ``/dl1/event/telescope/monitoring/pedestal``"): {
        "tag": "tables.exceptions.NoSuchNodeError: group ``/`` does not have a child named ``/dl1/event/telescope/monitoring/pedestal``",
        "msg": "Check if the last subrun is failing, if so, discard it.",
        "error_id": 3
    },
    re.escape("x_new is above the interpolation range's maximum value"): {
        "tag": "ValueError: x_new is above the interpolation range's maximum value",
        "msg": "Run must be discarded.",
        "error_id": 4
    },
    re.escape("No such file or directory: 'datasequence'"): {
        "tag": "No such file or directory: 'datasequence'",
        "msg": "Command must be relaunched",
        "error_id": 5
    },
    r"lstcam_calib_onsite_create_drs4_pedestal_file.*Output file exists already\. Stop": {
        "action": "Remove the DRS4 file",
        "description": "the DRS4 file already exists",
        "error_id": 6
    }
}

def get_summary_info():
    """Returns the date and path for the RunSummary ECSV."""
    yesterday = datetime.now() - timedelta(days=1)
    summary_date = yesterday.strftime('%Y%m%d')
    path = f'/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary/RunSummary_{summary_date}.ecsv'
    return summary_date, path

def log_and_save(job_id, success, logger_func, success_msg, fail_msg):
    """Helper to handle logging and job history registration."""
    if success:
        logger_func(f"   |__ ✅ SUCCESS: {success_msg}")
        if utils.save_processed_job_id(job_id):
            logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
        else:
            logger_func("   |__ ⚠️ ERROR: Could not write to job history.")
        return True
    logger_func(f"   |__ ⚠️ FAILURE: {fail_msg}")
    return False

def handle_subrun_discard(job_id, run_id, subrun_id, logger_func):
    """Case ID 3: Discard the last subrun if it fails."""
    _, ecsv_path = get_summary_info()
    last_subrun = utils.get_ecsv_column_value(ecsv_path, run_id, 'n_subruns')
    
    if int(subrun_id) == int(last_subrun):
        success = utils.update_ecsv_cell(ecsv_path, run_id, "n_subruns", int(subrun_id) - 1)
        log_and_save(job_id, success, logger_func, f"Run {run_id} subruns updated.", "ECSV update failed.")
    else:
        logger_func(f"   |__ ⚠️ SKIP: Subrun {subrun_id} is not the last one ({last_subrun}).")

def handle_drs4_error(job_id, command, logger_func, handler):
    """Case ID 6: Remove existing DRS4 and relaunch."""
    if command in handler: return None
    summary_date, _ = get_summary_info()
    drs4_path = f"/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/PixelCalibration/Cat-A/drs4_baseline/{summary_date}/v0.1.1/drs4_pedestal*.h5"
    
    if utils.delete_path(drs4_path):
        success = utils.increase_memory_and_relaunch(command, 30)
        if log_and_save(job_id, success, logger_func, "DRS4 removed & job relaunched", "Relaunch failed"):
            return command
    return None

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date, handler):
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    
    # --- Path Normalization ---
    run_id, subrun_id = 0, 0
    match = re.search(r'LST1_(\d{5,6})(?:_(\d+))?', job_name) or re.search(r'^(\d{8,9})_(\d{1,3})$', job_id)
    if match:
        run_id, subrun_id = match.groups()
        if subrun_id:
            error_path = error_path.replace("%4a", f"{int(subrun_id):04d}")
            log_path = log_path.replace("%4a", f"{int(subrun_id):04d}")

    # --- Immediate Actions ---
    if state == "TIMEOUT":
        if command in handler: return None
        success = utils.increase_memory_and_relaunch(command, 30)
        return command if log_and_save(job_id, success, logger_func, "Relaunched with more memory", "Update failed") else None

    if not os.path.exists(log_path):
        utils.save_skipped_job_id(job_id)
        return None

    # --- Log Analysis ---
    try:
        with open(error_path, 'r', errors='ignore') as f:
            content = f.read()
            for pattern, details in KNOWN_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    error_id = details['error_id']
                    logger_func(f"   |__ ❌ DETECTED: {details.get('tag', details.get('description'))}")

                    if error_id == 3:
                        handle_subrun_discard(job_id, run_id, subrun_id, logger_func)
                    elif error_id == 4:
                        _, ecsv_path = get_summary_info()
                        success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA")
                        log_and_save(job_id, success, logger_func, "Run updated to EDATA", "ECSV update failed")
                    elif error_id == 5:
                        if command in handler: return None
                        success = utils.increase_memory_and_relaunch(command, 30)
                        if log_and_save(job_id, success, logger_func, "Job relaunched", "Relaunch failed"): return command
                    elif error_id == 6:
                        return handle_drs4_error(job_id, command, logger_func, handler)
                    
                    return None
    except Exception as e:
        logger_func(f"   |__ ❌ EXCEPTION: {str(e)}")
        
    return None
