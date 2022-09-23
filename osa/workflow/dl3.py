#!/usr/bin/env python

"""
Script to handle the production of DL3 files.

It uses the lstchain Tools:
 - lstchain_create_irf_files
 - lstchain_create_dl3_file
 - lstchain_create_dl3_index_files
"""

import logging
import subprocess as sp
from datetime import datetime
from pathlib import Path

import click
from astropy.utils import iers

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import build_sequences, get_source_list
from osa.paths import destination_dir, DEFAULT_CFG, create_source_directories, analysis_path
from osa.utils.cliopts import get_prod_id, get_dl2_prod_id
from osa.utils.logging import myLogger
from osa.utils.utils import stringify, YESTERDAY

iers.conf.auto_download = False

__all__ = [
    "batch_cmd_create_irf",
    "batch_cmd_create_dl3",
    "batch_cmd_create_index_dl3",
    "get_irf_file",
    "create_irf",
    "produce_dl3_files",
    "setup_global_options",
    "cuts_subdirectory",
    "create_obs_index",
]

log = myLogger(logging.getLogger(__name__))


def batch_cmd_create_irf(cwd, mc_gamma, mc_proton, mc_electron, output_irf_file, dl3_config):
    """Create batch command to create IRF file with sbatch."""
    return [
        "sbatch",
        "--parsable",
        "--mem=6GB",
        "--job-name=irf",
        "-D",
        cwd,
        "-o",
        "log/create_irf_%j.log",
        "lstchain_create_irf_files",
        "--point-like",
        f"--input-gamma-dl2={mc_gamma}",
        f"--input-proton-dl2={mc_proton}",
        f"--input-electron-dl2={mc_electron}",
        f"--output-irf-file={output_irf_file}",
        f"--config={dl3_config}",
        "--overwrite",
    ]


def batch_cmd_create_dl3(
    dl2_file, dl3_dir, run, source_name, source_ra, source_dec, irf, dl3_config, job_irf
):
    """Create batch command to create DL3 files with sbatch."""
    log_dir = dl3_dir / "log"
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir / f"dl2_to_dl3_Run{run:05d}_{source_name}_%j.log"
    sbatch_cmd = [
        "sbatch",
        "--mem=10GB",
        "--job-name=dl2dl3",
        "-D",
        dl3_dir,
        "-o",
        log_file,
        "--parsable",
    ]
    if job_irf is not None:
        sbatch_cmd.append(f"--dependency=afterok:{job_irf}")

    dl3_cmd = [
        "lstchain_create_dl3_file",
        f"-d={dl2_file}",
        f"-o={dl3_dir}",
        f"--input-irf={irf}",
        f"--source-name={source_name}",
        f"--source-ra={source_ra}deg",
        f"--source-dec={source_dec}deg",
        "--overwrite",
    ]
    if dl3_config:
        dl3_cmd.append(f"--config={dl3_config}")

    return sbatch_cmd + dl3_cmd


def batch_cmd_create_index_dl3(dl3_dir, parent_job_list):
    """Build batch command to create the observations index."""
    parent_job_list_str = ",".join(parent_job_list)
    log_dir = dl3_dir / "log"
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir / "create_index_dl3_%j.log"

    return [
        "sbatch",
        "--parsable",
        "--mem=8GB",
        "--job-name=dl3_index",
        f"--dependency=afterok:{parent_job_list_str}",
        "-D",
        dl3_dir,
        "-o",
        log_file,
        "lstchain_create_dl3_index_files",
        f"-d={dl3_dir}",
        f"-o={dl3_dir}",
        "-p=dl3*.fits.gz",
        "--overwrite",
    ]


def get_irf_file(directory: Path, simulate: bool = False):
    """Return the irf file. Create it if not existing."""
    if cfg.get("MC", "IRF_file") is not None:
        irf_file = Path(cfg.get("MC", "IRF_file"))
        log.info(f"Using existing IRF file:\n{irf_file}")
        dl3_config = None  # Uses default settings
        job_id_irf = None
    else:
        dl3_config = Path(cfg.get("lstchain", "DL3_CONFIG"))
        irf_file, job_id_irf = create_irf(directory, dl3_config, simulate)

    return irf_file, dl3_config, job_id_irf


def create_irf(directory: Path, config: Path, simulate: bool = False):
    """Create the IRF file for a given set of selection cuts."""
    log.info("Creating the IRFs.")

    mc_gamma = cfg.get("MC", "gamma")
    mc_proton = cfg.get("MC", "proton")
    mc_electron = cfg.get("MC", "electron")
    irf_file = directory / "irf.fits.gz"

    cmd1 = batch_cmd_create_irf(
        cwd=directory,
        mc_gamma=mc_gamma,
        mc_proton=mc_proton,
        mc_electron=mc_electron,
        output_irf_file=irf_file,
        dl3_config=config,
    )

    if simulate:
        return None

    log.info("Submitting the IRF job.")
    log.debug(stringify(cmd1))
    job_irf = sp.run(
        cmd1,
        encoding="utf-8",
        capture_output=True,
        text=True,
    )
    return irf_file, job_irf.stdout.strip()


def produce_dl3_files(
    sequence_list,
    irf_file: Path,
    dl2_dir: Path,
    cuts_dir: Path,
    dl3_config: Path,
    job_id_irf: int,
    simulate: bool = False,
):
    """Produce the DL3 files for a given list of sequences."""
    list_of_job_id = []

    log.info("Looping over the runs to produce the DL3 files.")
    for sequence in sequence_list:

        if sequence.type == "DATA":

            dl2_file = dl2_dir / f"dl2_LST-1.Run{sequence.run:05d}.h5"
            dl3_subdir = cuts_dir / f"{sequence.source_name}"

            cmd2 = batch_cmd_create_dl3(
                dl2_file=dl2_file,
                dl3_dir=dl3_subdir,
                run=sequence.run,
                source_name=sequence.source_name,
                source_ra=sequence.source_ra,
                source_dec=sequence.source_dec,
                irf=irf_file,
                dl3_config=dl3_config,
                job_irf=job_id_irf,
            )

            if not simulate:
                log.info(f"Producing DL3 file for run {sequence.run:05d}")
                job_id = sp.run(
                    cmd2,
                    encoding="utf-8",
                    capture_output=True,
                    text=True,
                )

                list_of_job_id.append(job_id.stdout.strip())

            else:
                log.debug("Simulate launching scripts")

            log.debug(f"Executing {stringify(cmd2)}")

    return list_of_job_id


def create_obs_index(source_list: list, cuts_dir: Path, parent_jobs: list, simulate: bool = False):
    """Creating observation index for each source."""
    log.info("Creating observation index for each source.")

    for source in source_list:
        dl3_subdir = cuts_dir / source

        cmd3 = batch_cmd_create_index_dl3(dl3_subdir, parent_jobs)

        if not simulate:
            log.info("Scheduling DL3 index job")
            sp.run(
                cmd3,
                encoding="utf-8",
                capture_output=True,
                text=True,
            )
        else:
            log.debug("Simulate creating DL3 index")

        log.debug(f"Executing {stringify(cmd3)}")


def setup_global_options(date_obs, telescope):
    """Set up the global options arguments."""
    options.date = date_obs
    options.tel_id = telescope
    options.prod_id = get_prod_id()
    options.dl2_prod_id = get_dl2_prod_id()
    options.directory = analysis_path(options.tel_id)


def cuts_subdirectory() -> Path:
    """Return the dl3 subdirectory for a given set of cuts."""
    log.debug("Creating DL3 directory")
    dl3_dir = destination_dir("DL3", create_dir=True)
    std_cuts_dir = dl3_dir / "std_cuts"
    log_dir = std_cuts_dir / "log"
    std_cuts_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(exist_ok=True)
    log.debug(f"DL3 directory: {dl3_dir}")
    return std_cuts_dir


@click.command()
@click.argument("telescope", type=click.Choice(["LST1", "LST2"]))
@click.option("-d", "--date-obs", type=click.DateTime(formats=["%Y-%m-%d"]), default=YESTERDAY)
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False),
    default=DEFAULT_CFG,
    help="Read option defaults from the specified cfg file",
)
@click.option("-v", "--verbose", is_flag=True)
@click.option("--local", is_flag=True)
@click.option("-s", "--simulate", is_flag=True)
def main(
    date_obs: datetime = YESTERDAY,
    telescope: str = "LST1",
    verbose: bool = False,
    simulate: bool = False,
    local: bool = False,
    config: Path = DEFAULT_CFG,
):
    """Produce the IRF and DL3 files tool in a run basis."""
    log.setLevel(logging.INFO)

    if verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"=== DL3 stage for {date_obs.strftime('%Y-%m-%d')} ===")
    log.debug(f"Config: {config.resolve()}")

    if local:
        options.test = True
        log.info("Local mode enabled: no interaction with the cluster.")

    if simulate:
        options.simulate = True
        log.info("Simulation mode enabled: no jobs will be submitted.")

    # Set up the global options
    setup_global_options(date_obs, telescope)
    dl2_dir = destination_dir("DL2", create_dir=False)

    # Build the sequences
    sequence_list = build_sequences(options.date)

    # Get the list of source names
    source_list = list(get_source_list(options.date))

    # Create a subdirectory inside the DL3 directory corresponding to the selection cuts
    std_cuts_dir = cuts_subdirectory()

    # Create a subdirectory for each source
    create_source_directories(source_list, std_cuts_dir)

    # Get IRF file
    irf_file, dl3_config, job_id_irf = get_irf_file(directory=std_cuts_dir, simulate=simulate)

    # Create the DL3 files
    list_of_job_id = produce_dl3_files(
        sequence_list=sequence_list,
        irf_file=irf_file,
        dl2_dir=dl2_dir,
        cuts_dir=std_cuts_dir,
        dl3_config=dl3_config,
        job_id_irf=job_id_irf,
        simulate=simulate,
    )

    # Creating an observation file index for each source
    create_obs_index(
        source_list=source_list,
        cuts_dir=std_cuts_dir,
        parent_jobs=list_of_job_id,
        simulate=simulate,
    )


if __name__ == "__main__":
    main()
