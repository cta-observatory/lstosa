#!/usr/bin/env python

import logging
import pathlib
import subprocess as sp
import sys
from datetime import date, timedelta

import click
from astropy.utils import iers

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import run_summary_table
from osa.utils.cliopts import set_default_directory_if_needed, get_prod_id, get_dl2_prod_id
from osa.utils.logging import myLogger
from osa.utils.utils import destination_dir, stringify

iers.conf.auto_download = False

__all__ = [
    "cmd_create_irf",
    "cmd_create_dl3",
    "cmd_create_index_dl3",
]

log = myLogger(logging.getLogger(__name__))

DEFAULT_CFG = pathlib.Path(__file__).parent / '../../cfg/sequencer.cfg'


def cmd_create_irf(cwd, mc_gamma, mc_proton, mc_electron, output_irf_file, dl3_config):
    return [
        "sbatch",
        "--parsable",
        "--mem=8GB",
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


def cmd_create_dl3(
        dl2_file,
        dl3_dir,
        run,
        source_name,
        source_ra,
        source_dec,
        irf,
        dl3_config,
        job_irf
):
    log_dir = dl3_dir / "log"
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir / f"dl2_to_dl3_Run{run:05d}_{source_name}_%j.log"
    return [
        "sbatch",
        "--mem=8GB",
        "--job-name=dl2dl3",
        f"--dependency=afterok:{job_irf}",
        "-D",
        dl3_dir,
        "-o",
        log_file,
        "--parsable",
        "lstchain_create_dl3_file",
        f"-d={dl2_file}",
        f"-o={dl3_dir}",
        f"--input-irf={irf}",
        f"--source-name={source_name}",
        f"--source-ra={source_ra}deg",
        f"--source-dec={source_dec}deg",
        f"--config={dl3_config}",
        "--overwrite",
    ]


def cmd_create_index_dl3(dl3_dir, parent_job_list):
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


@click.command()
@click.argument('telescope', type=click.Choice(['LST1', 'LST2']))
@click.option(
    '-d',
    '--date-obs',
    type=click.DateTime(formats=["%Y_%m_%d"]),
    default=(date.today() - timedelta(days=1)).strftime("%Y_%m_%d")
)
@click.option(
    '-c', '--config',
    type=click.Path(dir_okay=False),
    default=DEFAULT_CFG,
    help='Read option defaults from the specified cfg file',
)
@click.option('-v', '--verbose', is_flag=True)
@click.option('--local', is_flag=True)
@click.option('-s', '--simulate', is_flag=True)
def main(date_obs, telescope, verbose, simulate, config, local):
    """Produce the IRF and DL3 files tool in a run basis."""
    log.setLevel(logging.INFO)

    if verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"=== DL3 stage for {date_obs.strftime('%Y-%m-%d')} ===")

    if local:
        options.test = True
        log.info("Local mode enabled: no interaction with the cluster.")

    if simulate:
        options.simulate = True
        log.info("Simulation mode enabled: no jobs will be submitted.")

    options.date = date_obs.strftime('%Y_%m_%d')
    options.tel_id = telescope
    options.prod_id = get_prod_id()
    options.dl2_prod_id = get_dl2_prod_id()
    options.directory = set_default_directory_if_needed()

    # Build the sequences
    summary_table = run_summary_table(options.date)
    subrun_list = extractsubruns(summary_table)
    run_list = extractruns(subrun_list)
    sequence_list = extractsequences(run_list)

    # Get the list of source names
    source_list = []
    for sequence in sequence_list:
        if sequence.source_name is not None and sequence.source_name not in source_list:
            source_list.append(sequence.source_name)

    log.info(f"List of sources: {source_list}")

    if not source_list:
        sys.exit("No sources found. Check the access to database. Exiting.")

    # Create a subdirectory inside the DL3 directory corresponding to the selection cuts
    log.debug("Creating DL3 directory")
    dl2_dir = destination_dir("DL2", create_dir=False)
    dl3_dir = destination_dir("DL3", create_dir=True)
    std_cuts_dir = dl3_dir / "std_cuts"
    log_dir = std_cuts_dir / "log"
    std_cuts_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(exist_ok=True)
    log.debug(f"DL3 directory: {dl3_dir}")

    # Create a subdirectory for each source
    for source in source_list:
        if source is not None:
            source_dir = std_cuts_dir / source
            source_dir.mkdir(parents=True, exist_ok=True)

    log.info("Creating the IRFs.")

    mc_gamma = cfg.get("IRF", "mc_gamma")
    mc_proton = cfg.get("IRF", "mc_proton")
    mc_electron = cfg.get("IRF", "mc_electron")
    dl3_config = cfg.get("lstchain", "DL3_CONFIG")
    irf_file = std_cuts_dir / "irf.fits.gz"

    cmd1 = cmd_create_irf(
        cwd=std_cuts_dir,
        mc_gamma=mc_gamma,
        mc_proton=mc_proton,
        mc_electron=mc_electron,
        output_irf_file=irf_file,
        dl3_config=dl3_config,
    )

    if not simulate:
        log.info("Submitting the IRF job.")
        log.debug(stringify(cmd1))
        job_irf = sp.run(
            cmd1,
            encoding="utf-8",
            capture_output=True,
            text=True,
        )
        job_id_irf = job_irf.stdout.strip()

    else:
        job_id_irf = None

    list_of_job_id = []

    log.info("Looping over the runs to produce the DL3 files.")
    for sequence in sequence_list:

        if sequence.type == "DATA":

            dl2_file = dl2_dir / f"dl2_LST-1.Run{sequence.run:05d}.h5"
            dl3_subdir = std_cuts_dir / f"{sequence.source_name}"

            cmd2 = cmd_create_dl3(
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
                log.info("Simulate launching scripts")

            log.debug(f"Executing {stringify(cmd2)}")

    # Creating observation index for each source
    for source in source_list:
        dl3_subdir = std_cuts_dir / source

        cmd3 = cmd_create_index_dl3(dl3_subdir, list_of_job_id)

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


if __name__ == "__main__":
    main()
