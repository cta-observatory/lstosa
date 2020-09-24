"""
Simulate executions of data processing pipeline and produce provenance
"""
import logging
import multiprocessing as mp
import subprocess
from pathlib import Path

import yaml

from datamodel import SequenceData
from osa.configs.config import cfg
from osa.jobs.job import createjobtemplate
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import readnightsummary
from osa.provenance.utils import get_log_config
from osa.utils import options
from osa.utils.cliopts import simprocparsing
from osa.utils.utils import lstdate_to_number
from osa.utils.standardhandle import gettag

CONFIG_FLAGS = {"Go": True, "TearDL1": False, "TearDL2": False, "TearSubDL1": False, "TearSubDL2": False}
provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]


def do_setup():
    """Set-up folder structure and check flags."""

    pathDL1 = Path(cfg.get("LST1", "DL1DIR")) / options.directory
    pathDL2 = Path(cfg.get("LST1", "DL2DIR")) / options.directory
    pathDL1sub = pathDL1 / options.prod_id
    pathDL2sub = pathDL2 / options.prod_id

    if Path(LOG_FILENAME).exists() and not options.append:
        CONFIG_FLAGS["Go"] = False
        logging.info(f"File {LOG_FILENAME} already exists.")
        logging.info(f"You must rename/remove {LOG_FILENAME} to produce a clean provenance.")
        logging.info(f"You can also set --append flag to append captured provenance.")
        return

    CONFIG_FLAGS["TearSubDL1"] = False if pathDL1sub.exists() or options.provenance else pathDL1sub
    CONFIG_FLAGS["TearSubDL2"] = False if pathDL2sub.exists() or options.provenance else pathDL2sub
    CONFIG_FLAGS["TearDL1"] = False if pathDL1.exists() or options.provenance else pathDL1
    CONFIG_FLAGS["TearDL2"] = False if pathDL2.exists() or options.provenance else pathDL2

    if options.provenance and not options.force:
        if pathDL1sub.exists():
            CONFIG_FLAGS["Go"] = False
            logging.info(f"Folder {pathDL1sub} already exist.")
        if pathDL2sub.exists():
            CONFIG_FLAGS["Go"] = False
            logging.info(f"Folder {pathDL2sub} already exist.")
        if not CONFIG_FLAGS["Go"]:
            logging.info(f"You must enforce provenance files overwrite with --force flag.")
            return

    pathDL1sub.mkdir(parents=True, exist_ok=True)
    pathDL2sub.mkdir(parents=True, exist_ok=True)


def tear_down():
    """Tear down created temporal folders."""
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
            if "--prod_id" in line:
                args.append("-s")
            args.append(line.strip())
        if line.startswith("subprocess.call"):
            keep = True
    args.pop()
    return args


def simulate_subrun_processing(args):
    """Simulate subrun processing."""
    run_str, subrun_idx = args[17].split(".")
    logging.info(f"Simulating process call for run {run_str} subrun {subrun_idx}")
    subprocess.run(args)


def simulate_processing():
    """Simulate daily processing and capture provenance."""

    options.mode = "P"
    options.simulate = True
    night_content = readnightsummary()
    logging.info(f"Night summary file content\n{night_content}")

    sub_run_list = extractsubruns(night_content)
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
            args_pp = [
                "python",
                "provprocess.py",
                "-c",
                options.configfile,
                s.run_str,
                options.directory,
                options.prod_id,
            ]
            logging.info(f"Processing provenance for run {s.run_str}")
            subprocess.run(args_pp)


if __name__ == "__main__":
    tag = gettag()
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)

    simprocparsing()
    options.directory = lstdate_to_number(options.date)

    logging.info(f"Running simulate processing")

    do_setup()
    if CONFIG_FLAGS["Go"]:
        simulate_processing()
    tear_down()
