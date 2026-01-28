import os
import re
import troubleshooting_utils as utils
from datetime import datetime, timedelta

# --- KNOWN ERROR DICTIONARY (CASE-BY-CASE) ---
# Add new cases here as they appear in logs.

KNOWN_ERRORS = {
    re.escape("Could not find compatible EventSource for input_url"): {
        "tag": "ERROR [lstchain.CatBCalibrationHDF5Writer] (tool.run): Caught unexpected exception: Could not find compatible EventSource for input_url",
        "msg": "Remove all logs from Cat-B and relaunch the job.",
        "id": 1
    },
    re.escape("!!! No calibration events in the output file !!!"): {
        "tag": "CRITICAL [lstchain.CatBCalibrationHDF5Writer] (lstchain_create_cat_B_calibration_file.finish): !!! No calibration events in the output file !!! ",
        "msg": "No calibration events were found. Check the number of subruns; if it is less than 20, they can be discarded",
        "id": 2
    },
    re.escape("os.symlink(target, self, target_is_directory)"): {
        "tag": "os.symlink(target, self, target_is_directory)",
        "msg": "Pro link is not created",
        "id": 3
    },
    re.escape("Number of subruns with low statistics: 1 - removed from pedestal median calculation"): {
        "tag": "Number of subruns with low statistics: 1 - removed from pedestal median calculation",
        "msg": "The run must be discarded if number of subruns < 20",
        "id": 4
    },
    re.escape("Calibration file from run {calibration_run} not found"): {
        "tag": "Calibration file from run {calibration_run} not found", 
        "msg": "The calibration file is missing, it must be checked that the Cat-A calibration file exists and its pro link",
        "id": 5
    }
}

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date):
    """
    Checks if a specific pattern (string or regex) exists in a log file.
    Returns: True if found, False otherwise.
    """
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    logger_func(f"   |__ Log {log_path}")
    logger_func(f"   |__ Error {error_path}")

    review_path = error_path if job_name != "lstchain_find_tailcuts" else log_path
    
    if state == "TIMEOUT":
        logger_func(f"   |__ ❌ DIAGNOSIS: TIMEOUT (Walltime exceeded).")
        logger_func(f"   |__ 💡 ACTION: Increase --mem in sbatch.")
        try:    
            run_id = utils.get_run_id_from_path(review_path) # Ensure review_path is correct

            success = utils.increase_memory_and_relaunch(command, 30)

            # --- SUCCESS MANAGEMENT ---
            if success:
                logger_func(f"   |__ ✅ SUCCESS: Run {run_id} memory updated.")
                
                # Save the Job ID to avoid reprocessing
                saved = utils.save_processed_job_id(job_id)
                if saved:
                    logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
                else:
                    logger_func(f"   |__ ⚠️ ERROR: Could not write to job history.")
            
            else:
                # If update function returns False
                logger_func(f"   |__ ⚠️ FUNCTIONAL FAILURE: Could not update the command.")

        except Exception as e:
            # --- ERROR MANAGEMENT (Code Exception) ---
            logger_func(f"   |__ ❌ EXCEPTION: An unexpected error occurred managing Job {job_id}.")
            logger_func(f"   |__ 🔍 Detail: {str(e)}")
        return
    
    if not review_path or not os.path.exists(review_path):
        print(f"[UTILS ERROR] {review_path} not found, This job will be skipped")
        # Ensure review_path is correct (might be error_path or log_path)
        # run_id = utils.get_run_id_from_path(review_path) 

        utils.save_skipped_job_id(job_id)
        return False
    
    try:
        with open(review_path, 'r', errors='ignore') as f:
            content = f.read()
            for pattern, details in KNOWN_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    tag = details['tag']
                    msg = details['msg']
                    id = details['id']
                
                    if tag:
                        logger_func(f"   |__ ❌ DETECTED CAUSE: {tag}")
                        logger_func(f"   |__ 💡 SOLUTION: {msg}")
                    else:
                        logger_func(f"   |__ ❓ UNKNOWN CAUSE: No matching patterns found.")
                        logger_func(f"   |__ 👁  Check manually: {review_path}")

                    if id == 1:
                        # The run must be discarded
                        #utils.update_csv_cell(csv_path, search_col, search_val, target_col, new_val, delimiter=',')
                        return
                    
                    if id == 2:
                        try:
                            run_id = utils.get_run_id_from_path(review_path) 
                            yesterday = datetime.now() - timedelta(days=1)
                            summary_date = yesterday.strftime('%Y%m%d')
                            
                            ecsv_path = f'/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary/RunSummary_{summary_date}.ecsv'

                            # Attempt to update the ECSV
                            success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA", subruns_limit=20)

                            # --- SUCCESS MANAGEMENT ---
                            if success:
                                logger_func(f"   |__ ✅ SUCCESS: Run {run_id} updated to EDATA in ECSV.")
                                
                                # Save Job ID
                                saved = utils.save_processed_job_id(job_id)
                                if saved:
                                    logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
                                else:
                                    logger_func(f"   |__ ⚠️ ERROR: Could not write to job history.")
                            
                            else:
                                # If update returns False
                                logger_func(f"   |__ ⚠️ FUNCTIONAL FAILURE: Could not update ECSV (check paths or columns).")

                        except Exception as e:
                            # --- ERROR MANAGEMENT ---
                            logger_func(f"   |__ ❌ EXCEPTION: An unexpected error occurred managing Job {job_id}.")
                            logger_func(f"   |__ 🔍 Detail: {str(e)}")
                        return
                    
                    if id == 3:
                        # Pro link must be created
                        return
                    
                    if id == 4:
                        try:
                            run_id = utils.get_run_id_from_path(review_path) 
                            yesterday = datetime.now() - timedelta(days=1)
                            summary_date = yesterday.strftime('%Y%m%d')
                            
                            ecsv_path = f'/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary/RunSummary_{summary_date}.ecsv'

                            # Attempt to update the ECSV
                            success = utils.update_ecsv_cell(ecsv_path, run_id, "run_type", "EDATA")

                            # --- SUCCESS MANAGEMENT ---
                            if success:
                                logger_func(f"   |__ ✅ SUCCESS: Run {run_id} updated to EDATA in ECSV.")
                                
                                # Save Job ID
                                saved = utils.save_processed_job_id(job_id)
                                if saved:
                                    logger_func(f"   |__ 💾 SAVED: Job {job_id} registered in history.")
                                else:
                                    logger_func(f"   |__ ⚠️ ERROR: Could not write to job history.")
                            
                            else:
                                logger_func(f"   |__ ⚠️ FUNCTIONAL FAILURE: Could not update ECSV (check paths or columns).")

                        except Exception as e:
                            logger_func(f"   |__ ❌ EXCEPTION: An unexpected error occurred managing Job {job_id}.")
                            logger_func(f"   |__ 🔍 Detail: {str(e)}")
                        return

    except Exception as e:
        print(f"[UTILS ERROR] Failed to read log {error_path}: {e}")
    return None, None