"""Identify files to be moved to their final destination directories"""

import logging
import re
import shutil
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import destination_dir
from osa.utils.logging import myLogger
from osa.veto import set_closed_sequence

__all__ = [
    "register_files",
    "register_run_concept_files",
    "register_found_pattern",
    "register_non_existing_file",
]

log = myLogger(logging.getLogger(__name__))

ANALYSIS_PRODUCTS = [
    "DL1AB",
    "DATACHECK",
    "PEDESTAL",
    "CALIB",
    "TIMECALIB",
    "MUON",
    "DL2",
    "INTERLEAVED"
]


def register_files(run_str, analysis_dir, prefix, suffix, output_dir) -> None:
    """
    Copy files into final data directory destination and register
    them into the DB (to be implemented).

    Parameters
    ----------
    run_str: str
        Run number
    analysis_dir: pathlib.Path
        analysis directory
    suffix: str
        suffix of the data file
    output_dir: pathlib.Path
        final data directory
    prefix: str
        prefix of the data file
    """

    file_list = analysis_dir.rglob(f"{prefix}*{run_str}*{suffix}")

    for input_file in file_list:
        output_file = output_dir / input_file.name
        if not output_file.exists():
            log.debug(f"Moving file {input_file} to {output_dir}")
            shutil.move(input_file, output_file)
            # Keep DL1 and muons symlink in running_analysis
            create_symlinks(input_file, output_file, prefix, suffix)


def create_symlinks(input_file, output_file, prefix, suffix):
    """
    Keep DL1 and muons symlink in running_analysis for possible future re-use.
    DL1 symlink is also kept in the DL1ab subdirectory to be able to process
    up to DL2 later on.
    """

    analysis_dir = Path(options.directory)
    dl1ab_dir = analysis_dir / options.dl1_prod_id

    if prefix == "dl1_LST-1" and suffix == ".h5":
        dl1_filepath_analysis_dir = analysis_dir / input_file.name
        dl1_filepath_dl1_dir = dl1ab_dir / input_file.name
        # Remove the original DL1 files pre DL1ab stage and keep only symlinks
        if dl1_filepath_analysis_dir.is_file() and not dl1_filepath_analysis_dir.is_symlink():
            dl1_filepath_analysis_dir.unlink()

        if not dl1_filepath_analysis_dir.is_symlink():
            dl1_filepath_analysis_dir.symlink_to(output_file.resolve())

        # Also set the symlink in the DL1ab subdirectory
        if not dl1_filepath_dl1_dir.is_symlink():
            dl1_filepath_dl1_dir.symlink_to(output_file.resolve())

    if prefix == "muons_LST-1" and suffix == ".fits":
        input_file.symlink_to(output_file.resolve())

    if prefix == "interleaved_LST-1" and suffix == ".h5":
        input_file.symlink_to(output_file.resolve())


def register_run_concept_files(run_string, concept):
    """
    Prepare files to be moved to final destination directories
    from the running_analysis original directory.

    Parameters
    ----------
    run_string: str
    concept: str
    """

    initial_dir = Path(options.directory)  # running_analysis

    # For MUON and INTERLEAVED data products, the initial directory is running_analysis

    if concept == "DL2":
        initial_dir = initial_dir / options.dl2_prod_id

    elif concept == "DL1AB":
        initial_dir = initial_dir / options.dl1_prod_id

    elif concept == "DATACHECK":
        initial_dir = initial_dir / options.dl1_prod_id

    output_dir = destination_dir(concept, create_dir=False)
    data_level = cfg.get("PATTERN", f"{concept}TYPE")
    prefix = cfg.get("PATTERN", f"{concept}PREFIX")
    suffix = cfg.get("PATTERN", f"{concept}SUFFIX")

    log.debug(f"Registering {data_level} file for {prefix}*{run_string}*{suffix}")
    if concept in ANALYSIS_PRODUCTS:
        register_files(run_string, initial_dir, prefix, suffix, output_dir)
    else:
        log.warning(f"Concept {concept} not known")


def register_found_pattern(file_path: Path, seq_list: list, concept: str, destination_path: Path):
    """

    Parameters
    ----------
    file_path: pathlib.Path
    seq_list: list
    concept: str
    destination_path: pathlib.Path
    """
    new_dst = destination_path / file_path.name
    log.debug(f"New file path {new_dst}")
    if not options.simulate:
        if new_dst.exists():
            log.debug("Destination file already exists")
        else:
            log.debug(f"Destination file {new_dst} does not exists")
            register_non_existing_file(file_path, concept, seq_list)

    # Return filepath already registered to be deleted from the set of all files
    return file_path


def register_non_existing_file(file_path, concept, seq_list):
    """

    Parameters
    ----------
    file_path: pathlib.Path
    concept: str
    seq_list: list
    """
    for sequence in seq_list:
        if sequence.type == "DATA":
            run_str_found = re.search(sequence.run_str, str(file_path))

            if run_str_found is not None:
                log.debug(f"Registering file {run_str_found}")
                register_run_concept_files(sequence.run_str, concept)
                if options.seqtoclose is None and not file_path.exists():
                    log.debug("File does not exists")

        elif sequence.type in ["PEDCALIB", "DRS4"]:
            calib_run_str_found = re.search(str(sequence.run), str(file_path))
            drs4_run_str_found = re.search(str(sequence.previousrun), str(file_path))

            if calib_run_str_found is not None:
                log.debug(f"Registering file {calib_run_str_found}")
                register_run_concept_files(str(sequence.run), concept)
                if options.seqtoclose is None and not file_path.exists():
                    log.debug("File does not exists")

            if drs4_run_str_found is not None:
                log.debug(f"Registering file {drs4_run_str_found}")
                register_run_concept_files(str(sequence.previousrun), concept)
                if options.seqtoclose is None and not file_path.exists():
                    log.debug("File does not exists")

        set_closed_sequence(sequence)
