import os
import shutil
import csv
import re
import subprocess
import sys
from datetime import datetime, timedelta

# ==========================================
# 1. LOGS & TEXT ANALYSIS
# ==========================================

def log_contains_pattern(log_path, pattern):
    """
    Checks if a specific pattern (string or regex) exists in a log file.
    Returns: True if found, False otherwise.
    """
    if not log_path or not os.path.exists(log_path):
        return False
    
    try:
        with open(log_path, 'r', errors='ignore') as f:
            content = f.read()
            if re.search(pattern, content, re.IGNORECASE):
                return True
    except Exception as e:
        print(f"[UTILS ERROR] Failed to read log {log_path}: {e}")
    return False

# ==========================================
# 2. FILE SYSTEM OPERATIONS (CRUD)
# ==========================================

def delete_path(path):
    """Safely deletes a file or an entire directory."""
    if not os.path.exists(path):
        return False
    
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        return True
    except Exception as e:
        print(f"[UTILS ERROR] Could not delete {path}: {e}")
        return False

def create_folder(path):
    """Creates a directory (and parents) if it doesn't exist."""
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as e:
        print(f"[UTILS ERROR] Could not create dir {path}: {e}")
        return False

def create_file(path, content=""):
    """Creates a file with optional initial content."""
    try:
        # Ensure parent directory exists
        parent_dir = os.path.dirname(path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
            
        with open(path, 'w') as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"[UTILS ERROR] Could not create file {path}: {e}")
        return False
    

def get_ecsv_column_value(file_path, target_id, target_col='n_subruns', id_col='run_id'):
    """
    Searches for a Run ID in the ECSV and returns the value of the specified column.
    Defaults to looking for 'n_subruns'.
    
    Returns:
        str: The found value (as a string).
        None: If the file, column, or Run ID is not found.
    """
    if not os.path.exists(file_path):
        print(f"[UTILS ERROR] File not found: {file_path}")
        return None

    try:
        with open(file_path, 'r') as f_in:
            header_parsed = False
            id_idx = -1
            target_idx = -1
            
            for line in f_in:
                # 1. Ignore Metadata
                if line.strip().startswith('#'):
                    continue
                
                # 2. Parse Header (first valid line)
                if not header_parsed:
                    headers = line.strip().split(',')
                    try:
                        id_idx = headers.index(id_col)
                        target_idx = headers.index(target_col)
                        header_parsed = True
                    except ValueError:
                        print(f"[UTILS ERROR] Column '{id_col}' or '{target_col}' not found in {file_path}")
                        return None
                    continue

                # 3. Find the row
                parts = line.strip().split(',')
                
                # Check if it is the correct row
                if len(parts) > id_idx and parts[id_idx].strip() == str(target_id):
                    value = parts[target_idx].strip()
                    # Return the raw value (string)
                    return value

        # If we reach here, we scanned the whole file and didn't find the ID
        print(f"[UTILS INFO] Run ID {target_id} not found in summary.")
        return None

    except Exception as e:
        print(f"[UTILS ERROR] Failed to read ECSV: {e}")
        return None

# ==========================================
# 3. CSV MANIPULATION
# ==========================================

def update_ecsv_cell(file_path, target_id, target_col, new_value, subruns_limit=0, id_col='run_id'):
    """
    Updates a column in an ECSV only if the run has fewer or equal 'n_subruns' than the limit.
    """
    if not os.path.exists(file_path):
        print(f"[UTILS ERROR] File not found: {file_path}")
        return False

    temp_path = file_path + ".tmp"
    modified = False
    try:
        with open(file_path, 'r') as f_in, open(temp_path, 'w') as f_out:
            header_parsed = False
            id_idx = -1
            target_idx = -1
            subruns_idx = -1  # Index for the subruns column
            
            for line in f_in:
                # 1. Preserve Metadata
                if line.strip().startswith('#'):
                    f_out.write(line)
                    continue
                
                # 2. Parse Header
                if not header_parsed:
                    headers = line.strip().split(',')
                    try:
                        id_idx = headers.index(id_col)
                        target_idx = headers.index(target_col)
                        # We look for the n_subruns column for the check
                        subruns_idx = headers.index('n_subruns') 
                        header_parsed = True
                        f_out.write(line)
                    except ValueError as e:
                        # If a critical column is missing, stop
                        print(f"[UTILS ERROR] Column missing in {file_path}: {e}")
                        f_out.close()
                        os.remove(temp_path)
                        return False
                    continue

                # 3. Process data rows
                parts = line.strip().split(',')
                # Verify if it is the correct row (ID matches)
                if len(parts) > id_idx and parts[id_idx].strip() == str(target_id):
                    
                    try:
                        if subruns_limit != 0:
                            print(f"[UTILS] Found target run_id: {target_id}")
                            # IMPORTANT: Convert to int to compare numbers
                            current_subruns = int(parts[subruns_idx])
                            if current_subruns <= subruns_limit:
                                parts[target_idx] = str(new_value)
                                modified = True
                                print(f"[UTILS] Run {target_id} updated: '{target_col}' -> '{new_value}' (Subruns: {current_subruns})")
                            else:
                                print(f"[UTILS] Run {target_id} SKIPPED: Subruns {current_subruns} > Limit {subruns_limit}")
                        else:
                            parts[target_idx] = str(new_value)
                            modified = True
                            print(f"[UTILS] Run {target_id} updated: '{target_col}' -> '{new_value}' (No subruns limit)")
                    except ValueError:
                        print(f"[UTILS WARNING] Could not parse n_subruns for Run {target_id}")

                # Reconstruct the line (modified or not)
                f_out.write(','.join(parts) + '\n')

        # 4. Finalize
        if modified:
            shutil.move(temp_path, file_path)
            return True
        else:
            if os.path.exists(temp_path): os.remove(temp_path)
            # Return False if nothing was modified (due to run not found or limit exceeded)
            return False

    except Exception as e:
        print(f"[UTILS ERROR] Failed to update ECSV: {e}")
        if os.path.exists(temp_path): os.remove(temp_path)
        return False

# ==========================================
# 4. SLURM & EXECUTION
# ==========================================

def run_command(command_str):
    """Executes a shell command and returns (exit_code, stdout)."""
    try:
        result = subprocess.run(
            command_str, 
            shell=True, 
            capture_output=True, 
            text=True
        )
        return result.returncode, result.stdout.strip()
    except Exception as e:
        return -1, str(e)

def increase_memory_and_relaunch(script_path, new_mem):
    """
    1. Reads a .sh/.slurm script.
    2. Finds the line #SBATCH --mem=XXG.
    3. Increases the memory value.
    4. Overwrites the script.
    5. Executes 'sbatch script_path'.
    """
    if not os.path.exists(script_path):
        print(f"[UTILS] Script not found: {script_path}")
        return False

    modified = False

    # Regex to find --mem=20G or --mem=20 (assuming G)
    # This matches format: #SBATCH --mem=20G
    mem_pattern_1 = re.compile(r'(#SBATCH\s+--mem=)(\d+)(G?)')
    mem_pattern_2 = re.compile(r'(#SBATCH\s+--mem-per-cpu=)(\d+)(G?)')
    try:
        with open(script_path, 'r') as f:
            lines = f.readlines()

        with open(script_path, 'w') as f:
            for line in lines:
                match = mem_pattern_1.search(line)
                if match:
                    prefix = match.group(1) # "#SBATCH --mem="
                    # old_val = match.group(2)
                    # suffix = match.group(3) # "G"
                    
                    # Force "G" unit format
                    new_line = f"{prefix}{new_mem}G\n"
                    f.write(new_line)
                    modified = True
                else:
                    match = mem_pattern_2.search(line)
                    if match:
                        prefix = match.group(1) # "#SBATCH --mem=-per-cpu"
                        # old_val = match.group(2)
                        # suffix = match.group(3) # "G"

                        # Force "G" unit format
                        new_line = f"{prefix}{new_mem}G\n"
                        f.write(new_line)
                        modified = True
                    else:
                        f.write(line)
        
        if modified:
            print(f"[UTILS] Memory updated to {new_mem}G in {script_path}")
            # Relaunch the job
            code, out = run_command(f"sbatch {script_path}")
            if code == 0:
                print(f"[UTILS] Job relaunched successfully: {out}")
                return True
            else:
                print(f"[UTILS] Failed to relaunch: {out}")
                return False
        else:
            print("[UTILS] No memory tag (#SBATCH --mem) found to update.")
            return False

    except Exception as e:
        print(f"[UTILS ERROR] Failed to modify/relaunch job: {e}")
        return False
    
def get_run_id_from_path(path):
    """
    Extracts the Run ID from a log file with the format '..._RUNID_JOBID.log'
    Example: /.../tailcuts_finder_23342_51712367.log -> Returns '23342'
    """
    if not path: 
        return None
    # 1. Keep only the filename (ignore folders)
    filename = os.path.basename(path) 
    
    # 2. Use Regex to find the final pattern: _(Digits)_(Digits).log
    # (\d+) captures the Run ID
    # \d+   matches the Job ID (but we don't capture it)
    match = re.search(r'(\d+)_(?:\d+_)?\d+\.[a-zA-Z]{3}$', filename)
    if match:
        return match.group(1) # Returns what is inside the first parenthesis (the Run ID)
    
    return None


def save_processed_job_id(job_id):
    """
    Saves the job_id to a text file to keep a record.
    Creates the file if it does not exist.
    """
    try:
        yesterday = datetime.now() - timedelta(days=1)
        main_date = yesterday.strftime('%Y%m%d')
        # Ensure the directory exists
        folder = os.path.dirname('/fefs/aswg/lstosa/troubleshooting/'+main_date+'/')
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        with open(folder+'/'+main_date+'_processed.txt', 'a') as f:
            f.write(f"{job_id}\n")
        return True
    except Exception as e:
        print(f"[UTILS ERROR] Could not save Job ID {job_id}: {e}")
        return False

def save_skipped_job_id(job_id):
    """
    Saves the job_id to a text file to keep a record of skipped jobs.
    Creates the file if it does not exist.
    """
    try:
        yesterday = datetime.now() - timedelta(days=1)
        main_date = yesterday.strftime('%Y%m%d')
        # Ensure the directory exists
        folder = os.path.dirname('/fefs/aswg/lstosa/troubleshooting/'+main_date+'/')
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        with open(folder+'/'+main_date+'_skipped.txt', 'a') as f:
            f.write(f"{job_id}\n")
        return True
    except Exception as e:
        print(f"[UTILS ERROR] Could not save Job ID {job_id}: {e}")
        return False
    
def is_job_already_processed_or_skipped(job_id):
    """
    Checks if a job_id is already in the record files.
    """
    yesterday = datetime.now() - timedelta(days=1)
    main_date = yesterday.strftime('%Y%m%d')
    file_path_processed ='/fefs/aswg/lstosa/troubleshooting/'+main_date+'/'+main_date+'_processed.txt'
    file_path_skipped ='/fefs/aswg/lstosa/troubleshooting/'+main_date+'/'+main_date+'_skipped.txt' 

    if not os.path.exists(file_path_processed):
        # Optional: Print informational message if file doesn't exist yet (first run)
        # print(f'{file_path_processed} folder or file not found')
        pass # Allow logic to continue to check the other file, or return False at end
    
    if not os.path.exists(file_path_skipped):
        # print(f'{file_path_skipped} folder or file not found')
        pass

    try:
        if os.path.exists(file_path_processed):
            with open(file_path_processed, 'r') as f:
                # Read all lines and strip whitespace
                processed_jobs = set(line.strip() for line in f)
                if str(job_id) in processed_jobs:
                    return 'PROCESSED'
        
        if os.path.exists(file_path_skipped):
            with open(file_path_skipped, 'r') as f:
                # Read all lines and strip whitespace
                skipped_jobs = set(line.strip() for line in f)
                if str(job_id) in skipped_jobs:
                    return 'SKIPPED'
                    
    except Exception:
        return False
        
    return False

import re
from datetime import datetime, timedelta

def is_yesterday_path(path):
    """
    Checks if a directory in the path represents 'yesterday's date' 
    in YYYYMMDD format, specifically looking for years starting with '20'.
    """
    # Regex breakdown:
    # /           -> Starts with a forward slash
    # (20\d{6})   -> Captures 8 digits starting with '20'
    # /           -> Ends with a forward slash
    pattern = r'/(20\d{6})/'
    
    match = re.search(pattern, path)
    
    if not match:
        return False
    
    date_str = match.group(1)
    
    try:
        # Convert string to date object
        path_date = datetime.strptime(date_str, "%Y%m%d").date()
        
        # Get yesterday's date
        yesterday = datetime.now().date() - timedelta(days=1)
        
        return path_date == yesterday
        
    except ValueError:
        # Returns False if the string is not a valid calendar date (e.g., 20261345)
        return False
    print("❌ This path does not belong to yesterday.")