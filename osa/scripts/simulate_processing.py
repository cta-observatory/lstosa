"""Simulate executions of data processing pipeline and produce provenance.

If it is not executed by tests, please run  pytest --basetemp=test_osa first.
It needs to have test_osa folder filled with test datasets.

python osa/scripts/simulate_processing.py"""

import logging
import multiprocessing as mp
import subprocess
from datetime import datetime
from pathlib import Path

import yaml

from osa.configs import options
from osa.configs.config import cfg
from osa.job import calibration_sequence_job_template, data_sequence_job_template
from osa.nightsummary.extract import build_sequences
from osa.provenance.utils import get_log_config
from osa.utils.cliopts import simprocparsing
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_dir

__all__ = [
    "parse_template",
    "do_setup",
    "simulate_processing",
    "simulate_subrun_processing",
    "tear_down",
]

CONFIG_FLAGS = {
    "Go": True,
    "TearDL1": False,
    "TearDL2": False,
    "TearSubDL1": False,
    "TearSubDL2": False,
    "TearAnalysis": False,
    "TearSubAnalysis": False,
}
provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]

log = myLogger(logging.getLogger())


def do_setup():
    """Set-up folder structure and check flags."""
    path_analysis = Path(cfg.get("LST1", "ANALYSIS_DIR")) / options.directory
    path_dl1 = Path(cfg.get("LST1", "DL1_DIR")) / options.directory
    path_dl2 = Path(cfg.get("LST1", "DL2_DIR")) / options.directory
    path_sub_analysis = path_analysis / options.prod_id
    path_dl1sub = path_dl1 / options.prod_id
    path_dl2_sub = path_dl2 / options.prod_id

    if Path(LOG_FILENAME).exists() and not options.append:
        CONFIG_FLAGS["Go"] = False
        log.info(f"File {LOG_FILENAME} already exists.")
        log.info(f"You must rename/remove {LOG_FILENAME} to produce a clean provenance.")
        log.info("You can also set --append flag to append captured provenance.")
        return

    CONFIG_FLAGS["TearSubAnalysis"] = (
        False if path_sub_analysis.exists() or options.provenance else path_sub_analysis
    )
    CONFIG_FLAGS["TearAnalysis"] = (
        False if path_analysis.exists() or options.provenance else path_analysis
    )
    CONFIG_FLAGS["TearSubDL1"] = (
        False if path_dl1sub.exists() or options.provenance else path_dl1sub
    )
    CONFIG_FLAGS["TearSubDL2"] = (
        False if path_dl2_sub.exists() or options.provenance else path_dl2_sub
    )
    CONFIG_FLAGS["TearDL1"] = False if path_dl1.exists() or options.provenance else path_dl1
    CONFIG_FLAGS["TearDL2"] = False if path_dl2.exists() or options.provenance else path_dl2

    if options.provenance and not options.force:
        if path_sub_analysis.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {path_sub_analysis} already exist.")
        if path_dl1sub.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {path_dl1sub} already exist.")
        if path_dl2_sub.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {path_dl2_sub} already exist.")
        if not CONFIG_FLAGS["Go"]:
            log.info("You must enforce provenance files overwrite with --force flag.")
            return

    path_sub_analysis.mkdir(parents=True, exist_ok=True)
    path_dl1sub.mkdir(parents=True, exist_ok=True)
    path_dl2_sub.mkdir(parents=True, exist_ok=True)


def tear_down():
    """Tear down created temporal folders."""
    if isinstance(CONFIG_FLAGS["TearSubAnalysis"], Path):
        CONFIG_FLAGS["TearSubAnalysis"].rmdir()
    if isinstance(CONFIG_FLAGS["TearAnalysis"], Path):
        CONFIG_FLAGS["TearAnalysis"].rmdir()
    if isinstance(CONFIG_FLAGS["TearSubDL1"], Path):
        CONFIG_FLAGS["TearSubDL1"].rmdir()
    if isinstance(CONFIG_FLAGS["TearSubDL2"], Path):
        CONFIG_FLAGS["TearSubDL2"].rmdir()
    if isinstance(CONFIG_FLAGS["TearDL1"], Path):
        CONFIG_FLAGS["TearDL1"].rmdir()
    if isinstance(CONFIG_FLAGS["TearDL2"], Path):
        CONFIG_FLAGS["TearDL2"].rmdir()


def parse_template(template, idx):
    """Parse batch templates."""
    args = []
    keep = False
    for line in template.splitlines():
        if keep:
            line = line.replace("f'", "")
            line = line.replace("'", "")
            line = line.replace(",", "")
            line = line.replace(r"{subruns:04d}", str(idx).zfill(4))
            args.append(line.strip())
        if "subprocess.run" in line:
            keep = True
    # remove last three elements
    return args[:-3]


def simulate_calibration(args):
    """Simulate calibration."""
    log.info("Simulating calibration call")
    subprocess.run(args)


def simulate_subrun_processing(args):
    """Simulate subrun processing."""
    run_str, subrun_idx = args[-2].split(".")
    log.info(f"Simulating process call for run {run_str} subrun {subrun_idx}")
    subprocess.run(args)


def simulate_processing():
    """Simulate daily processing and capture provenance."""
    options.simulate = True
    options.test = True

    sequence_list = build_sequences(options.date)

    # simulate data calibration and reduction
    for sequence in sequence_list:
        processed = False
        if sequence.type == "PEDCALIB":
            args_cal = parse_template(calibration_sequence_job_template(sequence), 0)
            simulate_calibration(args_cal)
        elif sequence.type == "DATA":
            with mp.Pool() as poolproc:
                args_proc = [
                    parse_template(data_sequence_job_template(sequence), subrun_idx)
                    for subrun_idx in range(sequence.subruns)
                ]
                processed = poolproc.map(simulate_subrun_processing, args_proc)
        # produce prov if overwrite prov arg
        if processed and options.provenance:
            command = "provprocess"
            drs4_pedestal_run_id = f"{sequence.drs4_run:05d}"
            pedcal_run_id = f"{sequence.pedcal_run:05d}"

            args_pp = [
                command,
                "-c",
                f"{options.configfile}",
                drs4_pedestal_run_id,
                pedcal_run_id,
                sequence.run_str,
                options.directory,
                options.prod_id,
            ]
            log.info(f"Processing provenance for run {sequence.run_str}")
            subprocess.run(args_pp, check=True)


def main():
    """Dry-run of the entire processing chain to produce provenance."""
    log.setLevel(logging.INFO)

    simprocparsing()

    # date and tel_id hardcoded for the moment
    options.date = datetime.fromisoformat("2020-01-17")
    options.tel_id = "LST1"
    options.directory = date_to_dir(options.date)

    log.info("Running simulate processing")

    do_setup()
    if CONFIG_FLAGS["Go"]:
        simulate_processing()
    tear_down()


if __name__ == "__main__":
    main()
