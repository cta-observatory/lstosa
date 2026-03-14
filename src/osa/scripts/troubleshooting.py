#!/usr/bin/env python3
import subprocess
import sys
import re
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

import troubleshooting_gainsel as handlers_gainsel
import troubleshooting_catB as handlers_catB
import troubleshooting_sequencer as handlers_sequencer
import troubleshooting_utils as utils
from osa.configs.config import cfg
from osa.paths import DEFAULT_CFG
from pathlib import Path

# --- CONFIGURATION ---
SACCT_CMD = "sacct"
SCONTROL_CMD = "scontrol"
SLURM_USER = "lstanalyzer"

JOB_CATEGORIES_MAP = {
    "gain_selection": "GAIN_SEL",
    "CatB": "CAT_B",
    "onsite_create_cat_B_calibration_file": "CAT_B",
    "lstchain_find_tailcuts": "CAT_B",
    "LST1_": "SEQUENCER",
    "lstchain_dl1_to_dl2": "CLOSER",
    "closer": "CLOSER"
}

REPORT_ORDER = ["GAIN_SEL", "SEQUENCER", "CAT_B", "CLOSER", "UNKNOWN"]

def log_msg(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

# ---------------------------------------------------------
#       HELPER UTILITIES
# ---------------------------------------------------------

def get_job_category(job_name):
    for pattern, label in JOB_CATEGORIES_MAP.items():
        if pattern in job_name:
            return label
    return "UNKNOWN"

def get_scontrol_details(job_id):
    """Retrieves StdOut, StdErr, and Command for a specific JobID."""
    cmd = [SCONTROL_CMD, "show", "job", job_id]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError:
        return {'stdout': 'Unknown', 'command': 'Unknown', 'stderr': 'Unknown'}

    details = {
        'stdout': (re.search(r'StdOut=([^\s]+)', output) or [None, "No Log"])[1],
        'stderr': (re.search(r'StdErr=([^\s]+)', output) or [None, "No Error Path"])[1],
        'command': (re.search(r'Command=(.+)', output) or [None, "Unknown"])[1].split()[0]
    }
    return details

def display_job_batch(title, jobs, icon="✅"):
    """Generic display for processed or skipped job lists."""
    if not jobs:
        return
    print(f"\n>>> {title} ({len(jobs)} jobs) <<<")
    for job in jobs:
        log_msg(f"   |__ {icon} Job {job['id']} ({job['name']})")
        log_msg(f"   |__ 📂 Log: {job['log_path']} | Error: {job['log_error']}")

# ---------------------------------------------------------
#       CORE PROCESSING LOGIC
# ---------------------------------------------------------

def get_failed_slurm_jobs(start_date):
    """Fetches jobs from sacct and filters for non-success states."""
    cmd = [
        SACCT_CMD, '-X', f'--user={SLURM_USER}',
        f'--starttime={start_date}',# f'--endtime={end_date}',
        '--format=JobID,JobName,State', '--noconvert', '-n', '-P'
    ]
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError:
        return []

    failures = []
    ignored_states = ['COMPLETED', 'RUNNING', 'PENDING', 'RESIZING', 'SUSPENDED', 'CANCELLED']

    for line in result.strip().split('\n'):
        if not line:
            continue
        parts = line.split('|')
        state = parts[2].split()[0].replace('+', '').upper()

        if state not in ignored_states:
            failures.append({'id': parts[0], 'name': parts[1], 'state': state})
    return failures

def run_handler_routing(category, job, start_date, end_date, relaunched_commands):
    """Routes a single job to its specific troubleshooting module."""
    args = (job['id'], job['name'], job['state'], job['log_path'],
            job['error_path'], job['command'], log_msg, start_date, end_date, relaunched_commands)

    if category == "GAIN_SEL":
        return handlers_gainsel.handle_error(*args)
    elif category == "SEQUENCER":
        return handlers_sequencer.handle_error(*args)
    elif category == "CAT_B":
        return handlers_catB.handle_error(*args)
    elif category == "CLOSER":
        log_msg(f"   |__ 🚨 [CLOSER] Manual review required for Job {job['id']}")
        return None
    else:
        log_msg(f"   |__ ❌ [UNKNOWN] No handler for {job['name']}")
        return None

def process_jobs(start_date, end_date, more_days, no_show_processed):
    log_msg(f"INFO: Searching failures for {SLURM_USER} ({start_date} to {end_date})")

    raw_failures = get_failed_slurm_jobs(end_date)
    grouped_jobs = defaultdict(list)
    processed_history = []
    skipped_history = []
    relaunched_commands = []

    for job in raw_failures:
        # Check history first
        history_status = utils.is_job_already_processed_or_skipped(job['id'])
        details = get_scontrol_details(job['id'])

        summary_info = {
            'id': job['id'], 'name': job['name'],
            'log_path': details['stdout'], 'log_error': details['stderr']
        }

        if history_status == 'PROCESSED':
            processed_history.append(summary_info)
            continue
        elif history_status == 'SKIPPED':
            skipped_history.append(summary_info)
            continue

        # Filter by path (Yesterday logic)
        if not more_days and not utils.is_yesterday_path(details['stdout']):
            continue

        # Add to processing queue
        category = get_job_category(job['name'])
        job.update({
            'log_path': details['stdout'],
            'error_path': details['stderr'],
            'command': details['command']
        })
        grouped_jobs[category].append(job)

    # Execution Phase
    for category in REPORT_ORDER:
        job_list = grouped_jobs.get(category, [])
        if not job_list:
            continue

        print(f"\n>>> CATEGORY REPORT: {category} ({len(job_list)} failures) <<<" + "\n" + "-"*60)
        for job in job_list:
            cmd = run_handler_routing(category, job, start_date, end_date, relaunched_commands)
            if cmd:
                relaunched_commands.append(cmd)
            print("")

    # Final Summary
    if not no_show_processed:
        display_job_batch("PROCESSED JOBS", processed_history, "✅")
        display_job_batch("SKIPPED JOBS", skipped_history, "❌")

    log_msg("🏁 INFO: End of report.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--date", nargs="?", help="YYYY-MM-DD")
    parser.add_argument("--no-show-processed", action="store_true")
    parser.add_argument("--more-days", action="store_true")
    parser.add_argument("-c", "--config", action="store", type=Path, default=DEFAULT_CFG, help="Configuration file")

    args = parser.parse_args()
    target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print("Date format error.")
            sys.exit(1)

    start_str = target_date.strftime('%Y-%m-%d')
    end_str = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

    process_jobs(start_str, end_str, args.more_days, args.no_show_processed)

if __name__ == "__main__":
    main()
