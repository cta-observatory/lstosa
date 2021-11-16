import logging
import os
import shutil
from filecmp import cmp
from os.path import basename, exists, join
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import destination_dir

__all__ = [
    "register_files",
    "register_run_concept_files"
]

log = logging.getLogger(__name__)


def register_files(run_str, input_dir, prefix, suffix, output_dir):
    """
    Copy files into final data directory destination and register
    them into the DB (to be implemented).

    Parameters
    ----------
    run_str: str
        Run number
    input_dir: analysis directory
    suffix: suffix of the data file
    output_dir: final data directory
    prefix: prefix of the data file
    """
    file_list = Path(input_dir).rglob(f"{prefix}*{run_str}*{suffix}")
    log.debug(f"File list is {file_list}")
    # hostname = gethostname()
    # the default subrun index for most of the files
    # run = int(run_str.lstrip("0"))
    # subrun = 1
    for inputf in file_list:
        # next is the way of searching 3 digits after a dot in filenames
        # subrunsearch = re.search("(?<=\.)\d\d\d_", basename(inputf))
        # if subrunsearch:
        # and strip the zeroes after getting the first 3 character
        # subrun = subrunsearch.group(0)[0:3].lstrip("0")
        outputf = join(output_dir, basename(inputf))
        if exists(outputf) and cmp(inputf, outputf):
            # do nothing than acknowledging
            log.debug(
                f"Nothing to do. Destination file {outputf} exists and "
                f"it is identical to input"
            )
        else:
            # there is no output file or it is different
            log.debug(f"Moving file {outputf}")
            shutil.move(inputf, outputf)

            # Keeping DL1 and muons symlink in running_analysis
            if prefix == "dl1_LST-1" and suffix == ".h5":
                file_basename = os.path.basename(inputf)
                dl1_filepath = os.path.join(options.directory, file_basename)
                # Remove the original DL1 files pre DL1ab stage and keep only symlinks
                if os.path.isfile(dl1_filepath) and not os.path.islink(dl1_filepath):
                    os.remove(dl1_filepath)
                if not os.path.islink(dl1_filepath):
                    os.symlink(outputf, dl1_filepath)
            if prefix == "muons_LST-1" and suffix == ".fits":
                os.symlink(outputf, inputf)


def register_run_concept_files(run_string, concept):
    """
    Prepare files to be moved to final destination directories
    from the running_analysis original directory. DL1ab, datacheck
    and DL2 are firstly stored in the corresponding subdirectory.

    Parameters
    ----------
    run_string
    concept
    """

    if concept in ["MUON", "PEDESTAL", "CALIB", "TIMECALIB"]:
        inputdir = options.directory

    elif concept == "DL2":
        inputdir = join(options.directory, options.dl2_prod_id)

    elif concept in ["DL1AB", "DATACHECK"]:
        inputdir = join(options.directory, options.dl1_prod_id)

    outputdir = destination_dir(concept, create_dir=False)
    type = cfg.get("LSTOSA", concept + "TYPE")
    prefix = cfg.get("LSTOSA", concept + "PREFIX")
    suffix = cfg.get("LSTOSA", concept + "SUFFIX")
    log.debug(f"Registering {type} file for {prefix}*{run_string}*{suffix}")

    if concept in [
        "DL1AB", "DATACHECK", "PEDESTAL", "CALIB", "TIMECALIB", "MUON", "DL2"
    ]:
        register_files(type, run_string, inputdir, prefix, suffix, outputdir)
    else:
        log.warning(f"Concept {concept} not known")
