"""Handle the list of closed and vetoed sequences."""

import logging
import os
from pathlib import Path

from osa.configs import options
from osa.utils.logging import myLogger

__all__ = [
    "failed_history",
    "get_veto_list",
    "get_closed_list",
    "set_veto_action",
    "set_closed_action",
    "update_vetoes",
    "set_closed_sequence",
]

log = myLogger(logging.getLogger(__name__))


def get_veto_list(sequence_list):
    """Get a list of vetoed sequences."""
    update_vetoes(sequence_list)
    analysis_dir = Path(options.directory)
    veto_ls = analysis_dir.glob("*.veto")
    veto_list = []
    for file in veto_ls:
        # Extract the job name
        name = file.stem.strip("sequence_")
        veto_list.append(name)
        set_veto_action(name, sequence_list)
    return veto_list


def set_veto_action(name, sequence_list):
    """Set the action for a given sequence to veto."""
    for sequence in sequence_list:
        if sequence.jobname == name:
            sequence.action = "Veto"
            log.debug(f"Attributes of sequence {sequence.seq} updated")


def update_vetoes(sequence_list):
    """Create a .veto file for a given sequence if reached maximum number of trials."""
    for sequence in sequence_list:
        if (
            not os.path.exists(sequence.veto)
            and os.path.exists(sequence.history)
            and failed_history(sequence.history)
        ):
            Path(sequence.veto).touch()
            log.debug(f"Created veto file {sequence.veto}")


def failed_history(history_file: Path) -> bool:
    """
    Check if a processing step has failed twice in a given history file.

    Return True if the last line of the history file contains a non-zero exit
    status and is repeated twice, meaning that a given step has failed twice.
    """
    history_lines = history_file.read_text().splitlines()

    # Check if history file has at least two trials
    if len(history_lines) < 2:
        return False

    # Check if the last line of the history file is repeated twice
    # and the exit status is non-zero
    return history_lines[-1] == history_lines[-2] and history_lines[-1].split()[-1] != "0"


def set_closed_sequence(sequence):
    """Creates a .closed lock file for a given sequence."""
    sequence.closed = Path(options.directory) / f"sequence_{sequence.jobname}.closed"
    sequence.closed.touch()


def get_closed_list(sequence_list) -> list:
    """Get the list of closed sequences."""
    analysis_dir = Path(options.directory)
    closed_ls = analysis_dir.glob("*.closed")
    closed_list = []
    for file in closed_ls:
        # Extract the job name: LST1_XXXXX
        name = file.stem.strip("sequence_")
        closed_list.append(name)
        set_closed_action(name, sequence_list)
    return closed_list


def set_closed_action(name: str, sequence_list):
    """Set the action of a closed sequence object."""
    for sequence in sequence_list:
        if sequence.jobname == name:
            sequence.action = "Closed"
            log.debug(f"Attributes of sequence {sequence.seq} updated")
