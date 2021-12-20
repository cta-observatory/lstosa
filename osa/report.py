import logging
from datetime import datetime
from fnmatch import fnmatchcase
from glob import glob
from os.path import basename, getsize, join

from osa.configs import config, options
from osa.configs.config import cfg
from osa.raw import get_raw_dir
from osa.utils.iofile import append_to_file

log = logging.getLogger(__name__)

__all__ = ["history", "start", "rule", "finished_assignments"]


def start(parent_tag: str):
    """
    Print out the header of the script (sequencer, closer, etc).

    Parameters
    ----------
    parent_tag: str
    """
    now = datetime.utcnow()
    simple_parent_tag = parent_tag.rsplit("(")[0]
    header(
        f"Starting {simple_parent_tag} at {now.strftime('%Y-%m-%d %H:%M')} "
        f"UTC for LST, Telescope: {options.tel_id}, Night: {options.date}"
    )


def header(message):
    """Print out a header of a given length."""
    framesize = int(config.cfg.get("OUTPUT", "REPORTWIDTH"))
    if len(message) < framesize - 2:
        prettyframe = int((framesize - 2 - len(message)) / 2) * "="
    else:
        prettyframe = ""
    log.info(f"{prettyframe} {message} {prettyframe}")


def rule():
    prettyframe = int(config.cfg.get("OUTPUT", "REPORTWIDTH")) * "-"
    log.info(prettyframe)


def finished_assignments(sequence_list):
    """
    Report that the files have been produced and the night closed.

    Parameters
    ----------
    sequence_list: Iterable

    Returns
    -------
    dictionary: dict
        Dictionary with the sequence names as keys and the corresponding
        output files and sizes as values.
    """
    concept_set = []
    analysis_dir = options.directory
    disk_space_GB = 0
    rawnum = 0
    if options.tel_id == "LST1":
        concept_set = [
            "PEDESTAL",
            "CALIB",
            "TIMECALIB",
            "DL1",
            "DL1AB",
            "MUON",
            "DATACHECK",
            "DL2",
        ]
        rawdir = get_raw_dir()
        if sequence_list is not None:
            for s in sequence_list:
                rawnum += s.subruns
        data_files = glob(
            join(
                rawdir,
                f'*{cfg.get("PATTERN", "R0PREFIX")}*{cfg.get("PATTERN", "R0SUFFIX")}*',
            )
        )
        disk_space = sum(getsize(d) for d in data_files)
        disk_space_GB_f = float(disk_space) / (1000 * 1000 * 1000)
        disk_space_GB = int(round(disk_space_GB_f, 0))

    ana_files = glob(join(analysis_dir, "*" + cfg.get("PATTERN", "R0SUFFIX")))
    file_no = {}
    ana_set = set(ana_files)

    for concept in concept_set:
        pattern = f"{cfg.get('PATTERN', concept + 'PREFIX')}*"
        log.debug(f"Trying with {concept} and searching {pattern}")
        file_no[concept] = 0
        delete_set = set()
        for a in ana_set:
            ana_file = basename(a)
            pattern_found = fnmatchcase(ana_file, pattern)
            if pattern_found:
                log.debug(f"Was pattern {pattern} found in {ana_file}?: {pattern_found}")
                file_no[concept] += 1
                delete_set.add(a)
        ana_set -= delete_set

    now_string = f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"

    dictionary = {
        "NIGHT": options.date,
        "TELESCOPE": options.tel_id,
        "IS_CLOSED": 1,
        "SEQUENCES": len(sequence_list),
        "FILES_RAW": rawnum,
        "RAW_GB": disk_space_GB,
        "END": now_string,
    }

    for concept in concept_set:
        dictionary["FILES_" + concept] = file_no[concept]

    return dictionary


def history(
        run,
        prod_id,
        stage,
        return_code,
        history_file,
        input_file=None,
        config_file=None,
) -> None:
    """
    Appends a history line to the history file. A history line
    reports the outcome of the execution of a lstchain executable.

    Parameters
    ----------
    run : str
        Run/sequence analyzed.
    prod_id : str
        Prod ID of the run analyzed.
    stage : str
        Stage of the analysis pipeline.
    input_file : str
        If needed, input file used for the lstchain executable
    config_file : str
        Input card used for the lstchain executable.
    return_code : int
        Return code of the lstchain executable.
    history_file : pathlib.Path
        The history file that keeps track of the analysis steps.
    """
    date_string = datetime.utcnow().isoformat(sep=" ", timespec="minutes")
    string_to_write = (
        f"{run} {stage} {prod_id} {date_string} "
        f"{input_file} {config_file} {return_code}\n"
    )
    append_to_file(history_file, string_to_write)
