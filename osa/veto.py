import logging
import os
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import read_from_file

__all__ = [
    "failed_history",
    "get_veto_list",
    "get_closed_list",
    "set_veto_action",
    "set_closed_action",
    "update_vetoes",
    "set_closed_sequence"
]


log = logging.getLogger(__name__)


def get_veto_list(sequence_list):
    """Get a list of vetoed sequences."""
    update_vetoes(sequence_list)
    analysis_dir = Path(options.directory)
    veto_ls = analysis_dir.glob("*.veto")
    veto_list = []
    for file in veto_ls:
        # we extract the job name
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
    for s in sequence_list:
        if (
            not os.path.exists(s.veto)
            and os.path.exists(s.history)
            and failed_history(s.history, int(cfg.get("LSTOSA", "MAXTRYFAILED")))
        ):
            Path(s.veto).touch()
            log.debug(f"Created veto file {s.veto}")


def failed_history(historyfile: str, max_trials: int) -> bool:
    """Check if maximum amount of failures reached for a given history file."""
    programme = []
    card = []
    goal = []
    exit_status = []
    for line in read_from_file(historyfile).splitlines():
        words = line.split()
        # columns 2, 10, 11 and 12 of history file contains
        # the info (index 1, -3, -2 and -1 respectively)
        programme.append(words[1])
        goal.append(words[-3])
        card.append(words[-2])
        try:
            exit_status.append(int(words[-1]))
        except ValueError as error:
            log.exception(f"Malformed file {historyfile}, {error}")
        log.debug("extracting line: {0}".format(line))
    lsize = len(exit_status)
    if lsize >= max_trials and (goal[-1] != "new_calib"):
        strike = 0
        for i in range(lsize - 1):
            # m  = f"{exit_status[i]}=={exit_status[lsize-1]}, "
            # m += f"{card[i]=={card[lsize-1]}, {programme[i]}=={programme[lsize-1]}"
            # log.debug(f"comparing {m}")
            if (
                (exit_status[i] != 0)
                and (exit_status[i] == exit_status[lsize - 1])
                and (card[i] == card[lsize - 1])
                and (programme[i] == programme[lsize - 1])
            ):
                strike += 1
                if strike == max_trials - 1:
                    log.debug(f"Maximum amount of failures reached for {historyfile}")
                    return True
    return False


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
