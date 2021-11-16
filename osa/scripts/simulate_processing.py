"""
Simulate executions of data processing pipeline and produce provenance
"""
import logging
import multiprocessing as mp
import subprocess
from pathlib import Path

import yaml

from osa.configs import options
from osa.configs.config import cfg
from osa.configs.datamodel import SequenceData
from osa.jobs.job import createjobtemplate
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import run_summary_table
from osa.provenance.utils import get_log_config
from osa.utils.cliopts import simprocparsing
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_number

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

    pathAnalysis = Path(cfg.get("LST1", "ANALYSISDIR")) / options.directory
    pathDL1 = Path(cfg.get("LST1", "DL1DIR")) / options.directory
    pathDL2 = Path(cfg.get("LST1", "DL2DIR")) / options.directory
    pathSubAnalysis = pathAnalysis / options.prod_id
    pathDL1sub = pathDL1 / options.prod_id
    pathDL2sub = pathDL2 / options.prod_id

    if Path(LOG_FILENAME).exists() and not options.append:
        CONFIG_FLAGS["Go"] = False
        log.info(f"File {LOG_FILENAME} already exists.")
        log.info(f"You must rename/remove {LOG_FILENAME} to produce a clean provenance.")
        log.info(f"You can also set --append flag to append captured provenance.")
        return

    CONFIG_FLAGS["TearSubAnalysis"] = False if pathSubAnalysis.exists() or options.provenance else pathSubAnalysis
    CONFIG_FLAGS["TearAnalysis"] = False if pathAnalysis.exists() or options.provenance else pathAnalysis
    CONFIG_FLAGS["TearSubDL1"] = False if pathDL1sub.exists() or options.provenance else pathDL1sub
    CONFIG_FLAGS["TearSubDL2"] = False if pathDL2sub.exists() or options.provenance else pathDL2sub
    CONFIG_FLAGS["TearDL1"] = False if pathDL1.exists() or options.provenance else pathDL1
    CONFIG_FLAGS["TearDL2"] = False if pathDL2.exists() or options.provenance else pathDL2

    if options.provenance and not options.force:
        if pathSubAnalysis.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {pathSubAnalysis} already exist.")
        if pathDL1sub.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {pathDL1sub} already exist.")
        if pathDL2sub.exists():
            CONFIG_FLAGS["Go"] = False
            log.info(f"Folder {pathDL2sub} already exist.")
        if not CONFIG_FLAGS["Go"]:
            log.info(f"You must enforce provenance files overwrite with --force flag.")
            return

    pathSubAnalysis.mkdir(parents=True, exist_ok=True)
    pathDL1sub.mkdir(parents=True, exist_ok=True)
    pathDL2sub.mkdir(parents=True, exist_ok=True)


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
            line = line.replace("'", "")
            line = line.replace(",", "")
            line = line.replace(r"{0}.format(str(subruns).zfill(4))", str(idx).zfill(4))
            if "--stdout=" in line or "--stderr" in line or "srun" in line:
                continue
            if "--prod-id" in line:
                args.append("-s")
            args.append(line.strip())
        if "subprocess.run" in line:
            keep = True
    # Remove last three elements
    return args[0:-3]


def simulate_subrun_processing(args):
    """Simulate subrun processing."""
    run_str, subrun_idx = args[14].split(".")
    log.info(f"Simulating process call for run {run_str} subrun {subrun_idx}")
    subprocess.run(args)


def simulate_processing():
    """Simulate daily processing and capture provenance."""

    options.mode = "P"
    options.simulate = True
    summary_table = run_summary_table(options.date)

    sub_run_list = extractsubruns(summary_table)
    run_list = extractruns(sub_run_list)
    sequence_list = extractsequences(run_list)

    # skip drs4 and calibration
    for s in sequence_list:
        processed = False
        if not isinstance(s, SequenceData):
            continue
        for sl in s.subrun_list:
            if sl.runobj.type != "DATA":
                continue
            with mp.Pool() as pool:
                args_ds = [
                    parse_template(createjobtemplate(s, get_content=True), subrun_idx)
                    for subrun_idx in range(sl.subrun)
                ]
                processed = pool.map(simulate_subrun_processing, args_ds)

        # produce prov if overwrite prov arg
        if processed and options.provenance:
            command = Path(cfg.get("LSTOSA", "SCRIPTSDIR")) / "provprocess.py"
            args_pp = [
                "python",
                command,
                "-c",
                options.configfile,
                s.run_str,
                options.directory,
                options.prod_id,
            ]
            log.info(f"Processing provenance for run {s.run_str}")
            subprocess.run(args_pp)


if __name__ == "__main__":

    log.setLevel(logging.INFO)

    simprocparsing()

    # date and tel_id hardcoded for the moment
    #
    options.date = "2020_01_17"
    options.tel_id = "LST1"
    #

    options.directory = lstdate_to_number(options.date)

    log.info(f"Running simulate processing")

    do_setup()
    if CONFIG_FLAGS["Go"]:
        simulate_processing()
    tear_down()
