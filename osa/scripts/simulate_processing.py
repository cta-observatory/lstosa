"""
Simulate executions of data processing pipeline and produce provenance
"""

import logging
import subprocess
from osa.utils import options, cliopts
from osa.utils.utils import lstdate_to_number
from osa.jobs.job import createjobtemplate
from osa.nightsummary.nightsummary import readnightsummary
from osa.nightsummary import extract
from lstchain.version import get_version


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
            # if "calibrationsequence.py" in line:
            #     break
            if "--prod_id" in line:
                args.append("-s")
            args.append(line.strip())
        if line.startswith("subprocess.call"):
            keep = True
    args.pop()
    return args


def run_simulate_processing():
    """Build batch templates."""

    from osa.configs import config

    options.simulate = True
    night_content = readnightsummary()
    logging.info(f"Night summary file content\n{night_content}")

    options.directory = lstdate_to_number(options.date)
    sub_run_list = extract.extractsubruns(night_content)
    run_list = extract.extractruns(sub_run_list)
    sequence_list = extract.extractsequences(run_list)
    options.lstchain_version = 'v' + get_version()
    options.prod_id = options.lstchain_version + '_' + config.cfg.get('LST1', 'VERSION')

    # skip drs4 and calibration
    start_run_idx = 1
    start_subrun_idx = 2
    for run_idx, s in enumerate(sequence_list[start_run_idx:]):
        for subrun_idx in range(sub_run_list[run_idx + start_subrun_idx].subrun):
            args_ds = parse_template(createjobtemplate(s, get_content=True), subrun_idx)
            subprocess.run(args_ds)
        args_pp = [
            'python',
            'provprocess.py',
            s.run_str,
            options.directory,
            options.prod_id,
        ]
        # TODO: produce prov if overwrite arg
        # subprocess.run(args_pp)
        # print(sub_run_list[run_idx + start_subrun_idx].subrun)
        # print(args_pp)


if __name__ == "__main__":
    format = "%(filename)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)

    # TODO: make specific cli parsing
    cliopts.sequencercliparsing()

    logging.info(f"Running simulate processing")
    run_simulate_processing()
