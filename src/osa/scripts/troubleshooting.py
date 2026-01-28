#!/usr/bin/env python3
import subprocess
import os
import sys
import re
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
import troubleshooting_gainsel as handlers_gainsel
import troubleshooting_catB as handlers_catB
import troubleshooting_sequencer as handlers_sequencer
import troubleshooting_closer as handlers_closer

import troubleshooting_utils as utils

# --- CONFIGURATION ---
SACCT_CMD = "sacct"
SCONTROL_CMD = "scontrol"
SLURM_USER = "lstanalyzer"

# Category Map
JOB_CATEGORIES_MAP = {
    "gain_selection": "GAIN_SEL",
    "CatB": "CAT_B",
    "onsite_create_cat_B_calibration_file": "CAT_B",
    "lstchain_find_tailcuts": "CAT_B",
    "LST1_": "SEQUENCER",
    "lstchain_dl1_to_dl2": "CLOSER",
    "closer": "CLOSER"
}

# Report print order
REPORT_ORDER = ["GAIN_SEL", "SEQUENCER", "CAT_B", "CLOSER", "UNKNOWN"]

def log_msg(message):
    """Prints message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

# ---------------------------------------------------------
#       INTERNAL HANDLERS (For those not GainSel)
# ---------------------------------------------------------

def handle_closer_error(job, log_msg_func):
    log_msg_func(f"   |__ 🚨 [CLOSER] WARNING! Job {job['id']} has failed.")
    log_msg_func(f"   |__ 🛠  ACTION: Manual review required.")
    log_msg_func(f"   |__ 📂 Log: {job['log_path']}")

def handle_generic_error(job, log_msg_func):
    log_msg_func(f"   |__ ❌ [UNKNOWN] Job {job['id']} ({job['name']}) -> {job['state']}")
    log_msg_func(f"   |__ 📂 Log: {job['log_path']}")

def display_processed_jobs(jobs, log_msg_func):

    print(f"\n>>> CATEGORY REPORT: PROCESSED JOBS ({len(jobs)} failures) <<<")
    for job in jobs:
        log_msg_func(f"   |__ ✅ Job {job['id']} ({job['name']}) already processed.")
        log_msg_func(f"   |__ 📂 Log: {job['log_path']}")
        log_msg_func(f"   |__ 📂 Error: {job['log_error']}")

def display_skipped_jobs(jobs, log_msg_func):

    print(f"\n>>> CATEGORY REPORT: SKIPPED JOBS ({len(jobs)} failures) <<<")
    for job in jobs:
        log_msg_func(f"   |__ ❌ Job {job['id']} ({job['name']}) already processed.")
        log_msg_func(f"   |__ 📂 Log: {job['log_path']}")
        log_msg_func(f"   |__ 📂 Error: {job['log_error']}")

# ---------------------------------------------------------
#       DATA COLLECTION LOGIC
# ---------------------------------------------------------

def get_job_category(job_name):
    for pattern, label in JOB_CATEGORIES_MAP.items():
        if pattern in job_name:
            return label
    return "UNKNOWN"

def get_scontrol_details(job_id):
    cmd = [SCONTROL_CMD, "show", "job", job_id]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError:
        return {'stdout': 'Unknown (Purged)', 'command': 'Unknown', 'stderr': 'Unknown'}

    details = {'stdout': 'Unknown', 'command': 'Unknown', 'stderr': 'Unknown'}
    
    stdout_match = re.search(r'StdOut=([^\s]+)', output)
    if stdout_match: details['stdout'] = stdout_match.group(1)

    stderr_match = re.search(r'StdErr=([^\s]+)', output)
    if stderr_match: details['stderr'] = stderr_match.group(1)

    cmd_match = re.search(r'Command=(.+)', output)
    if cmd_match: details['command'] = cmd_match.group(1).split()[0]
    
    return details

def get_slurm_jobs(start_date, end_date):
    cmd = [
        SACCT_CMD, '-X', f'--user={SLURM_USER}',
        f'--starttime={start_date}', f'--endtime={end_date}',
        '--format=JobID,JobName,State,ExitCode', '--noconvert', '-n', '-P'
    ]
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError:
        return []

    jobs = []
    if not result: return jobs
    
    for line in result.strip().split('\n'):
        if not line: continue
        parts = line.split('|')
        if len(parts) < 3: continue
        jobs.append({'id': parts[0], 'name': parts[1], 'state_raw': parts[2]})
    return jobs

# ---------------------------------------------------------
#       PROCESSING AND REPORTING
# ---------------------------------------------------------

def process_jobs(start_date, end_date, args):
    log_msg(f"INFO: Searching failures for {SLURM_USER} from {start_date} to {end_date}")
    
    raw_jobs = get_slurm_jobs(start_date, end_date)
    
    # Structure to group jobs
    grouped_jobs = defaultdict(list)
    total_failures = 0
    processed_jobs = []
    skipped_jobs = []
    
    # 1. Collect data
    for job in raw_jobs:
        state = job['state_raw'].split()[0].replace('+', '').upper()
        skipped = False
        # Filter out what is NOT a failure
        if state in ['COMPLETED', 'RUNNING', 'PENDING', 'RESIZING', 'SUSPENDED','CANCELLED']:
            continue
        
        if utils.is_job_already_processed_or_skipped(job['id']) == 'PROCESSED':
            processed_jobs.append({'id': job['id'],    
                                  'name': job['name'],
                                  'log_path': get_scontrol_details(job['id'])['stdout'],
                                  'log_error': get_scontrol_details(job['id'])['stderr']})
            skipped = True

        if utils.is_job_already_processed_or_skipped(job['id']) == 'SKIPPED':
            skipped_jobs.append({'id': job['id'],    
                                 'name': job['name'],
                                 'log_path': get_scontrol_details(job['id'])['stdout'],
                                 'log_error': get_scontrol_details(job['id'])['stderr']})
            skipped = True

        if skipped == False:
            details = get_scontrol_details(job['id'])

            if args.more_days == False:
                if utils.is_yesterday_path(details['stdout']) == False:
                    continue

            # It is a failure
            total_failures += 1
            category = get_job_category(job['name'])
            
            job_data = {
                'id': job['id'],
                'name': job['name'],
                'state': state,
                'log_path': details['stdout'] if details['stdout'] != "/dev/null" else "No Log",
                'error_path': details['stderr'] if details['stderr'] != "/dev/null" else "No Error Path",
                'command': details['command']
            }
            
            grouped_jobs[category].append(job_data)
    # Debug print
    print(grouped_jobs)
    if total_failures == 0:
        log_msg("✅ INFO: No jobs found.")
    else:
        log_msg(f"⚠️  INFO: {total_failures} failed jobs found. Generating grouped report...\n")

        # 2. Print report ordered by groups
        for category in REPORT_ORDER:
            job_list = grouped_jobs.get(category, [])
            if not job_list:
                continue # If no failures of this type, skip

            print(f"\n>>> CATEGORY REPORT: {category} ({len(job_list)} failures) <<<")
            print("-" * 60)

            for job in job_list:
                # Routing to the appropriate handler
                if category == "GAIN_SEL":
                    # Use external module (performs deep analysis)
                    handlers_gainsel.handle_error(job['id'], job['name'], job['state'], job['log_path'], job['error_path'], job['command'], log_msg, start_date, end_date)
                
                elif category == "SEQUENCER":
                    handlers_sequencer.handle_error(job['id'], job['name'], job['state'], job['log_path'], job['error_path'], job['command'], log_msg, start_date, end_date)
                
                elif category == "CAT_B":
                    handlers_catB.handle_error(job['id'], job['name'], job['state'], job['log_path'], job['error_path'], job['command'],log_msg, start_date, end_date)

                elif category == "CLOSER":
                    handle_closer_error(job, log_msg)
                
                else:
                    handle_generic_error(job, log_msg)
                
                print("") # Space between jobs

    print("="*60)
    if args.no_show_processed == False:
        display_processed_jobs(processed_jobs, log_msg)
        display_skipped_jobs(skipped_jobs, log_msg)

        print("="*60)
    log_msg("🏁 INFO: End of report.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", help="YYYY-MM-DD")
    parser.add_argument("--no-show-processed", action="store_true", help="Do not show processed jobs in the report")
    parser.add_argument("--more-days", action="store_true", help="Only process today's jobs (default behavior)")

    args = parser.parse_args()

    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print("Date format error.")
            sys.exit(1)
    else:
        # --- IMPORTANT CHANGE: TODAY BY DEFAULT ---
        # Get current date, at 00:00:00
        target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    start_str = target_date.strftime('%Y-%m-%d')
    end_str = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

    process_jobs(start_str, end_str, args)

if __name__ == "__main__":
    main()