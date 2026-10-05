"""SLURM wrapper: sbatch --parsable, queue throttling, sacct-based waiting."""
import getpass
import logging
import subprocess
import time
from pathlib import Path

log = logging.getLogger("dvr")

RUNNING = {"PENDING", "RUNNING", "REQUEUED", "RESIZING", "SUSPENDED",
           "CONFIGURING", "COMPLETING"}


class Slurm:
    def __init__(self, cfg, dry_run=False):
        s = cfg.raw["slurm"]
        self.account = s["account"]
        self.poll = s.get("poll_seconds", 30)
        self.max_queued = s.get("max_queued_jobs", 1500)
        self.dry = dry_run
        self.user = getpass.getuser()
        self._n = 0

    def _queue_size(self) -> int:
        out = subprocess.run(["squeue", "-h", "-u", self.user, "-o", "%A"],
                             capture_output=True, text=True).stdout
        return len(out.split())

    def submit(self, script: Path, name: str, output: Path, workdir: Path,
               partition: str, extra=()) -> str:
        if self.dry:
            self._n += 1
            log.info("[dry-run] sbatch %s (%s)", script, name)
            return f"DRY{self._n}"
        while self._queue_size() >= self.max_queued:
            log.info("queue full (>= %d jobs), waiting...", self.max_queued)
            time.sleep(self.poll)
        cmd = ["sbatch", "--parsable", "-A", self.account, "-p", partition,
               "-J", name, "-o", str(output), "-D", str(workdir), *extra, str(script)]
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()
        return out.split(";")[0]

    @staticmethod
    def _states(job_ids) -> dict:
        out = subprocess.run(
            ["sacct", "-X", "-n", "-P", "-j", ",".join(job_ids), "-o", "JobID,State"],
            check=True, capture_output=True, text=True).stdout
        states = {}
        for line in out.splitlines():
            jid, state = line.split("|")[:2]
            states[jid] = state.split()[0]          # 'CANCELLED by 12' -> 'CANCELLED'
        return states

    def wait(self, job_ids) -> dict:
        """Block until all jobs finish; return {jobid: final state}."""
        ids = [j for j in job_ids if not j.startswith("DRY")]
        if not ids:
            return {}
        missing = {j: 0 for j in ids}
        while True:
            states = self._states(ids)
            pending = 0
            for j in ids:
                if j in states and states[j] not in RUNNING:
                    continue
                if j not in states:                 # accounting lag; give up after 10 polls
                    missing[j] += 1
                    if missing[j] > 10:
                        states[j] = "UNKNOWN"
                        continue
                pending += 1
            log.info("slurm: %d/%d jobs pending/running", pending, len(ids))
            if not pending:
                return states
            time.sleep(self.poll)
