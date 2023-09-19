"""Script to reprocess all daily longterm files found under a given production."""

import logging
import subprocess as sp
from pathlib import Path

import click

from osa.utils.logging import myLogger

ANALYSIS_PATH = Path("/fefs/aswg/data/real")
LONGTERM_PATH = ANALYSIS_PATH / "OSA/DL1DataCheck_LongTerm"


log = myLogger(logging.getLogger())


def run_longterm(date: str, prod_id: str, new_prod_id: str, log_dir: Path):
    """
    Run the longterm script for a given date.

    Parameters
    ----------
    date : str
        Date to reprocess in YYYYMMDD format.
    prod_id : str
        Production ID to reprocess.
    new_prod_id : str
        New production ID to reprocess.
    log_dir : Path
        Path to the directory where job logs will be stored.
    """
    dl1_dir = ANALYSIS_PATH / "DL1" / date / prod_id / "tailcut84" / "datacheck"
    muons_dir = ANALYSIS_PATH / "DL1" / date / prod_id / "muons"
    new_longterm_dir = LONGTERM_PATH / new_prod_id / date
    longterm_output_file = new_longterm_dir / f"DL1_datacheck_{date}.h5"

    log_file = log_dir / f"daily_check_{date}_%j.log"

    cmd = [
        "sbatch",
        "-o",
        str(log_file),
        "lstchain_longterm_dl1_check",
        f"--input-dir={dl1_dir}",
        f"--output-file={longterm_output_file}",
        f"--muons-dir={muons_dir}",
        "--batch",
    ]

    sp.run(cmd)
    log.info(f"Executing {' '.join(cmd)}")


@click.command()
@click.option("--prod-id", help="Production ID to reprocess")
@click.option("--new-prod-id", help="New production ID")
@click.option("--log-dir", type=click.Path(path_type=Path), help="Path to log directory")
def main(prod_id: str = None, new_prod_id: str = None, log_dir: Path = None):
    """
    Reprocess the daily check files for all existing dates found on a given production.
    """
    log_dir.mkdir(exist_ok=True, parents=True)
    longterm_dir = LONGTERM_PATH / prod_id

    list_of_processed_dates = longterm_dir.glob("20*")

    for date_directory in list_of_processed_dates:
        run_longterm(date_directory.name, prod_id, new_prod_id, log_dir)


if __name__ == "__main__":
    main()
