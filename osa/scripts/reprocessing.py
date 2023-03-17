"""Script to reprocess a list of dates without overwhelming the job system manager."""

import logging
import subprocess as sp
import time
from pathlib import Path

import click

from osa.configs.config import DEFAULT_CFG
from osa.utils.logging import myLogger
from osa.utils.utils import wait_for_daytime

log = myLogger(logging.getLogger(__name__))


def number_of_pending_jobs():
    """Return the number of jobs in the slurm queue."""
    cmd = ["squeue", "-u", "lstanalyzer", "-h", "-t", "pending", "-r"]
    output = sp.check_output(cmd)
    return output.count(b"\n")


def run_script(
    script: str, date, config: Path, no_dl2: bool, no_calib: bool, simulate: bool, force: bool
):
    """Run the sequencer for a given date."""
    osa_config = Path(config).resolve()

    cmd = [script, "--config", str(osa_config), "--date", date]

    if no_dl2:
        cmd.append("--no-dl2")

    if no_calib:
        cmd.append("--no-calib")

    if simulate:
        cmd.append("--simulate")

    if force:
        cmd.append("--force")

    # Append the telescope to the command in the last place
    cmd.append("LST1")

    log.info(f"\nRunning {' '.join(cmd)}")
    sp.run(cmd)


def check_job_status_and_wait(max_jobs=2500):
    """Check the status of the jobs in the queue and wait for them to finish."""
    while number_of_pending_jobs() > max_jobs:
        log.info("Waiting 2 hours for slurm queue to decrease...")
        time.sleep(7200)


def get_list_of_dates(dates_file):
    """Read the files with the dates to be processed and build a list of dates."""
    with open(dates_file, "r") as file:
        list_of_dates = [line.strip() for line in file]
    return list_of_dates


@click.command()
@click.option("--no-dl2", is_flag=True, help="Do not run the DL2 step.")
@click.option("--no-calib", is_flag=True, help="Do not run the calibration step.")
@click.option("-s", "--simulate", is_flag=True, help="Activate simulation mode.")
@click.option("-f", "--force", is_flag=True, help="Force the autocloser to close the day.")
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True),
    default=DEFAULT_CFG,
    help="Path to the OSA config file.",
)
@click.argument(
    "script", type=click.Choice(["sequencer", "closer", "copy_datacheck", "autocloser"])
)
@click.argument("dates-file", type=click.Path(exists=True))
def main(
    script: str = None,
    dates_file: Path = None,
    config: Path = DEFAULT_CFG,
    no_dl2: bool = False,
    no_calib: bool = False,
    simulate: bool = False,
    force: bool = False,
):
    """
    Loop over the dates listed in the input file and launch the script for each of them.
    The input file should list the dates in the format YYYY-MM-DD one date per line.
    """
    logging.basicConfig(level=logging.INFO)

    list_of_dates = get_list_of_dates(dates_file)

    # Check slurm queue status
    check_job_status_and_wait()

    for date in list_of_dates:
        # Avoid running jobs while it is still night time
        wait_for_daytime()

        run_script(script, date, config, no_dl2, no_calib, simulate, force)
        log.info("Waiting 1 minute to launch the process for the next date...\n")
        time.sleep(60)

        # Check slurm queue status and sleep for a while to avoid overwhelming the queue
        check_job_status_and_wait()

    log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
