import os
import shutil
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

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
# 3. CSV MANIPULATION (REFACTORED)
# ==========================================

def _get_header_indices(headers, id_col, target_col):
    """Helper to find column indices and handle errors."""
    try:
        return (
            headers.index(id_col),
            headers.index(target_col),
            headers.index('n_subruns')
        )
    except ValueError as e:
        print(f"[UTILS ERROR] Column missing: {e}")
        return None

def _should_update_row(parts, id_idx, subruns_idx, target_id, subruns_limit):
    """Helper to check if the current row matches the ID and satisfies subrun limits."""
    if len(parts) <= id_idx or parts[id_idx].strip() != str(target_id):
        return False
    if subruns_limit == 0:
        return True

    try:
        current_subruns = int(parts[subruns_idx])
        if current_subruns <= subruns_limit:
            return True
        print(f"[UTILS] Run {target_id} SKIPPED: Subruns {current_subruns} > Limit {subruns_limit}")
    except (ValueError, IndexError):
        print(f"[UTILS WARNING] Could not parse subruns for Run {target_id}")
    return False

def _process_ecsv_lines(f_in, f_out, target_id, target_col, new_value, subruns_limit, id_col):
    """Core loop to process lines and update target cell."""
    header_parsed = False
    modified = False
    id_idx, target_idx, subruns_idx = -1, -1, -1

    for line in f_in:
        # Preserve Metadata
        if line.strip().startswith('#'):
            f_out.write(line)
            continue

        # Parse Header
        if not header_parsed:
            headers = line.strip().split(',')
            indices = _get_header_indices(headers, id_col, target_col)
            if not indices: return False # Exit if columns missing
            id_idx, target_idx, subruns_idx = indices
            header_parsed = True
            f_out.write(line)
            continue

        # Process Row
        parts = line.strip().split(',')
        if _should_update_row(parts, id_idx, subruns_idx, target_id, subruns_limit):
            parts[target_idx] = str(new_value)
            modified = True
            print(f"[UTILS] Run {target_id} updated '{target_col}' -> '{new_value}'")

        f_out.write(','.join(parts) + '\n')
    return modified

def update_ecsv_cell(file_path, target_id, target_col, new_value, subruns_limit=0, id_col='run_id'):
    """
    Updates a column in an ECSV only if the run has fewer or equal 'n_subruns' than the limit.
    Refactored to meet MC0001 complexity standards.
    """
    if not os.path.exists(file_path):
        print(f"[UTILS ERROR] File not found: {file_path}")
        return False

    temp_path = file_path + ".tmp"
    success = False

    try:
        with open(file_path, 'r') as f_in, open(temp_path, 'w') as f_out:
            success = _process_ecsv_lines(
                f_in, f_out, target_id, target_col, new_value, subruns_limit, id_col
            )

        if success:
            shutil.move(temp_path, file_path)
        else:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    except Exception as e:
        print(f"[UTILS ERROR] Failed to update ECSV: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

    return success

# ==========================================
# 4. SLURM & EXECUTION
# ==========================================

def run_command(command_str):
    """
    Executes a command without shell=True by manually splitting the string.
    Note: This assumes arguments do not contain internal spaces.
    """
    try:
        # 1. Remove shell-specific operators (like ; or &) 
        # that are incompatible with shell=False
        clean_command = command_str.replace("osa_env;", "").replace("osa_env &", "").strip()
        # 2. Convert string to list by splitting on whitespace
        # "sbatch --mem=20G script.sh" -> ["sbatch", "--mem=20G", "script.sh"]
        command_args = [arg for arg in clean_command.split(" ") if arg]
        # 3. Execute safely
        result = subprocess.run(
            command_args,
            shell=False,
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
                    new_line = f"{prefix}{new_mem}G\n"
                    f.write(new_line)
                    modified = True
                else:
                    match = mem_pattern_2.search(line)
                    if match:
                        prefix = match.group(1) # "#SBATCH --mem-per-cpu="
                        new_line = f"{prefix}{new_mem}G\n"
                        f.write(new_line)
                        modified = True
                    else:
                        f.write(line)

        if modified:
            print(f"[UTILS] Memory updated to {new_mem}G in {script_path}")
            # Relaunch the job
            code, out = run_command(f"osa_env & sbatch {script_path}")
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
    match = re.search(r'(\d+)_(?:\d+_)?\d+\.[a-zA-Z]{3}$', filename)
    if match:
        return match.group(1)

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

    try:
        if os.path.exists(file_path_processed):
            with open(file_path_processed, 'r') as f:
                processed_jobs = set(line.strip() for line in f)
                if str(job_id) in processed_jobs:
                    return 'PROCESSED'

        if os.path.exists(file_path_skipped):
            with open(file_path_skipped, 'r') as f:
                skipped_jobs = set(line.strip() for line in f)
                if str(job_id) in skipped_jobs:
                    return 'SKIPPED'

    except Exception:
        return False

    return False

def is_yesterday_path(path):
    """
    Checks if a directory in the path represents 'yesterday's date'
    in YYYYMMDD format, specifically looking for years starting with '20'.
    """
    pattern = r'/(20\d{6})/'
    match = re.search(pattern, path)

    if not match:
        return False

    date_str = match.group(1)

    try:
        path_date = datetime.strptime(date_str, "%Y%m%d").date()
        yesterday = datetime.now().date() - timedelta(days=1)
        return path_date == yesterday
    except ValueError:
        return False

def file_exists(file_path: str) -> bool:
    """Checks if a file exists at the given path."""
    return Path(file_path).is_file()

def is_link(path: str) -> bool:
    """Checks if the path is a symbolic link."""
    return Path(path).is_symlink()
