"""
Simulate executions of data processing pipeline and produce provenance
"""
import logging
from osa.utils import options, cliopts
from osa.nightsummary.nightsummary import readnightsummary
from osa.nightsummary import extract


def run_simulate():

    # TODO
    # parse night summary
    night = readnightsummary()
    logging.info(f"Night summary file content\n{night}")
    subrun_list = extract.extractsubruns(night)
    run_list = extract.extractruns(subrun_list)

    # TODO
    # loop running sequentially
    # datasequence.py with args and option simulate to capture prov
    # provprocess.py with args producing prov in temp/physical folder


if __name__ == "__main__":

    format = "%(filename)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)
    cliopts.sequencercliparsing()
    logging.info(f"Running simulate processing")

    run_simulate()
