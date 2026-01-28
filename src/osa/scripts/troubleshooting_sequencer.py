import os
import re
import troubleshooting_utils as utils
from datetime import datetime, timedelta

# --- KNOWN ERROR DICTIONARY (CASE-BY-CASE) ---
# Add new cases here as they appear in logs.

KNOWN_ERRORS = {
    r"LST1_\d{5,6}": {
        "tag": "ERROR [lstchain.CatBCalibrationHDF5Writer] (tool.run): Caught unexpected exception: Could not find compatible EventSource for input_url",
        "msg": "Remove all logs from Cat-B and relaunch the job.",
        "id": 1
    },
    re.escape("Table /dl1/monitoring/telescope/catB/calibration already exists in output file, use append or overwrite"): {
        "tag": "Table /dl1/monitoring/telescope/catB/calibration already exists in output file, use append or overwrite",
        "msg": "Pending",
        "id": 2
    },
    re.escape("tables.exceptions.NoSuchNodeError: group ``/`` does not have a child named ``/dl1/event/telescope/monitoring/pedestal``"): { 
        "tag": "tables.exceptions.NoSuchNodeError: group ``/`` does not have a child named ``/dl1/event/telescope/monitoring/pedestal``",
        "msg": "Check if the last subrun is failing, if so, discard it.",
        "id": 3
    },
    re.escape("x_new is above the interpolation range's maximum value"): {
        
        "tag": "ValueError: x_new is above the interpolation range's maximum value",
        "msg": "Run must be discarded.",
        "id": 4
    }
    # Poner error del Sabado, donde el log es /fefs/onsite/data/lst-pipe/LSTN-01/running_analysis/20260116/v0.11/log/Run23349.4294967294_jobid_51810134.err
# ver como sacar el valor largo 4...294967294

}

def handle_error(job_id, job_name, state, log_path, error_path, command, logger_func, start_date, end_date):
    """
    Checks if a specific pattern (string or regex) exists in a log file.
    Returns: True if found, False otherwise.
    """
    logger_func(f"   |__ Job {job_id} {job_name} {state}")
    run_id = 0
    subrun_id = 0

    if re.search(r'LST1_\d{5,6}', job_name):
        split_id = job_name.split('_')
        run_id = split_id[1]
        if len(split_id) > 2:
            subrun_id = split_id[2]
            if(subrun_id):
                error_path = error_path.replace("%4a", f"{int(subrun_id):04d}")
                log_path = log_path.replace("%4a", f"{int(subrun_id):04d}")
                logger_func(f"   |__ Log {log_path}")
                logger_func(f"   |__ Error {error_path}")


    if re.search(r'^\d{8,9}_(\d{1,3})$', job_id):
        split_job_id = job_id.split('_')
        subrun_id = split_job_id[1]
        error_path = error_path.replace("%4a", f"{int(subrun_id):04d}")
        log_path = log_path.replace("%4a", f"{int(subrun_id):04d}")

        logger_func(f"   |__ Log {log_path}")
        logger_func(f"   |__ Error {error_path}")


    review_path = error_path

    if state == "TIMEOUT":
        
        logger_func(f"   |__ ❌ DIAGNOSIS: TIMEOUT (Walltime exceeded).")
        logger_func(f"   |__ 💡 ACTION: Increase --mem in sbatch.")
        try:    
            success = utils.increase_memory_and_relaunch(command,30)

            # --- GESTIÓN DEL ÉXITO ---
            if success:
                logger_func(f"   |__ ✅ ÉXITO: Run {run_id} actualizada la memoria.")
                
                # Guardamos el Job ID para no volver a hacerlo
                saved = utils.save_processed_job_id(job_id)
                if saved:
                    logger_func(f"   |__ 💾 GUARDADO: Job {job_id} registrado en historial.")
                else:
                    logger_func(f"   |__ ⚠️ ERROR: No se pudo escribir en el historial de jobs.")
            
            else:
                # Si update_ecsv_cell devuelve False (fichero no encontrado, columna no existe, etc.)
                logger_func(f"   |__ ⚠️ FALLO FUNCIONAL: No se pudo actualizar el comando")

        except Exception as e:
            # --- GESTIÓN DEL ERROR (Excepción de código) ---
            logger_func(f"   |__ ❌ EXCEPCIÓN: Ocurrió un error inesperado gestionando Job {job_id}.")
            logger_func(f"   |__ 🔍 Detalle: {str(e)}")
        return
    
    if not log_path or not os.path.exists(log_path):
        print(f"[UTILS ERROR] {log_path} not found, This job will be skipped")
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
                        # The run must be discard
                        #utils.update_csv_cell(csv_path, search_col, search_val, target_col, new_val, delimiter=',')
                        return
                    if id == 2:
                        return
                    if id == 3:
                        #remove last subrun from run summary
                        try:
                            yesterday = datetime.now() - timedelta(days=1)
                            summary_date = yesterday.strftime('%Y%m%d')
                            
                            ecsv_path = f'/fefs/onsite/data/lst-pipe/LSTN-01/monitoring/RunSummary/RunSummary_{summary_date}.ecsv'

                            if subrun_id == utils.get_ecsv_column_value(ecsv_path, run_id, 'n_subruns'):

                                # Intentamos actualizar el ECSV
                                success = utils.update_ecsv_cell(ecsv_path, run_id, "n_subruns", subrun_id-1)

                                # --- GESTIÓN DEL ÉXITO ---
                                if success:
                                    logger_func(f"   |__ ✅ ÉXITO: Run {run_id} actualizado a EDATA en ECSV.")
                                    
                                    # Guardamos el Job ID para no volver a hacerlo
                                    saved = utils.save_processed_job_id(job_id)
                                    if saved:
                                        logger_func(f"   |__ 💾 GUARDADO: Job {job_id} registrado en historial.")
                                    else:
                                        logger_func(f"   |__ ⚠️ ERROR: No se pudo escribir en el historial de jobs.")
                                
                                else:
                                    # Si update_ecsv_cell devuelve False (fichero no encontrado, columna no existe, etc.)
                                    logger_func(f"   |__ ⚠️ FALLO FUNCIONAL: No se pudo actualizar el ECSV (verifica rutas o columnas).")

                            else:
                                logger_func(f"   |__ ⚠️ FALLO FUNCIONAL: Subrun {subrun_id} no es el último subrun de la Run {run_id}. No se actualiza el Run Summary.")
                        except Exception as e:
                            # --- GESTIÓN DEL ERROR (Excepción de código) ---
                            logger_func(f"   |__ ❌ EXCEPCIÓN: Ocurrió un error inesperado gestionando Job {job_id}.")
                            logger_func(f"   |__ 🔍 Detalle: {str(e)}")

                        return
                    if id == 4:
                        try:
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
                                # If update returns False
                                logger_func(f"   |__ ⚠️ FUNCTIONAL FAILURE: Could not update ECSV (check paths or columns).")

                        except Exception as e:
                            # --- ERROR MANAGEMENT ---
                            logger_func(f"   |__ ❌ EXCEPTION: An unexpected error occurred managing Job {job_id}.")
                            logger_func(f"   |__ 🔍 Detail: {str(e)}")
                        return
                    
    except Exception as e:
        print(f"[UTILS ERROR] Failed to read log {error_path}: {e}")
    return None, None




