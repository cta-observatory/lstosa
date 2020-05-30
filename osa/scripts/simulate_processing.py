"""
Simulate executions of data processing pipeline and produce provenance
"""
import logging
from pathlib import Path
from osa.utils import options, cliopts
from osa.jobs.job import createjobtemplate
from osa.nightsummary.nightsummary import readnightsummary
from osa.nightsummary import extract
from lstchain.version import get_version

BASE_PATH = Path(__file__).parent


def parse_template(template):
    """Parse batch templates."""
    return template


def run_simulate_processing():
    """Build batch templates."""

    from osa.configs import config

    options.simulate = True
    options.directory = BASE_PATH
    night_content = readnightsummary()
    logging.info(f"Night summary file content\n{night_content}")

    sub_run_list = extract.extractsubruns(night_content)
    run_list = extract.extractruns(sub_run_list)
    sequence_list = extract.extractsequences(run_list)
    options.lstchain_version = 'v' + get_version()
    options.prod_id = options.lstchain_version + '_' + config.cfg.get('LST1', 'VERSION')

    # TODO
    # loop running sequentially
    # datasequence.py with args and option simulate to capture prov
    # provprocess.py with args producing prov in temp/physical folder
    for s in sequence_list:
        parsed_template = parse_template(createjobtemplate(s, get_content=True))
        print(parsed_template)


if __name__ == "__main__":

    format = "%(filename)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)
    cliopts.sequencercliparsing()

    logging.info(f"Running simulate processing")

    run_simulate_processing()
