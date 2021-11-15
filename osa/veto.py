import glob
import logging
import os

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.iofile import read_from_file

__all__ = [
    "createveto",
    "createclosed",
    "failedhistory",
    "isjobvetoed",
    "getvetolist",
    "getclosedlist",
    "setvetoaction",
    "setclosedaction",
    "updatevetos",
]


log = logging.getLogger(__name__)


def createveto(vetofile):
    """

    Parameters
    ----------
    vetofile
    """
    try:
        open(vetofile, "w").close()
    except IOError as error:
        log.exception(f"Could not touch veto file {vetofile}, {error}")


def isjobvetoed(name, veto_list):
    """

    Parameters
    ----------
    name
    veto_list

    Returns
    -------

    """
    try:
        veto_list.index(name)
    except ValueError:
        return False
    else:
        return True


def getvetolist(sequence_list):
    """

    Parameters
    ----------
    sequence_list

    Returns
    -------

    """
    updatevetos(sequence_list)
    veto_ls = glob.glob(
        os.path.join(options.directory, "*{0}".format(cfg.get("LSTOSA", "VETOSUFFIX")))
    )
    veto_list = []
    for i in veto_ls:
        # we extract the job name
        name = i.rsplit(cfg.get("LSTOSA", "VETOSUFFIX"))[0].split(
            os.path.join(options.directory, "sequence_")
        )[1]
        veto_list.append(name)
        setvetoaction(name, sequence_list)
    return veto_list


def setvetoaction(name, sequence_list):
    """

    Parameters
    ----------
    name
    sequence_list
    """
    for s in sequence_list:
        if s.jobname == name:
            s.action = "Veto"
            log.debug(f"Attributes of sequence {s.seq} updated")


def updatevetos(sequence_list):
    """

    Parameters
    ----------
    sequence_list
    """
    for s in sequence_list:
        if (
            not os.path.exists(s.veto)
            and os.path.exists(s.history)
            and failedhistory(s.history, int(cfg.get("LSTOSA", "MAXTRYFAILED")))
        ):
            createveto(s.veto)
            log.debug(f"Created veto file {s.veto}")


def failedhistory(historyfile, maxnumber):
    """

    Parameters
    ----------
    historyfile
    maxnumber

    Returns
    -------

    """
    programme = []
    card = []
    goal = []
    exit_status = []
    for line in read_from_file(historyfile).splitlines():
        words = line.split()
        # columns 2, 10, 11 and 12 of history file contains the info (index 1, -3, -2 and -1 respectively)
        programme.append(words[1])
        goal.append(words[-3])
        card.append(words[-2])
        try:
            exit_status.append(int(words[-1]))
        except ValueError as error:
            log.exception(f"Malformed file {historyfile}, {error}")
        log.debug("extracting line: {0}".format(line))
    lsize = len(exit_status)
    if (programme[-1] == "merpp") and (exit_status[-1] == 23):
        return False
    if lsize >= maxnumber and (goal[-1] != "new_calib"):
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
                if strike == maxnumber - 1:
                    log.debug(f"Maximum amount of failures reached for {historyfile}")
                    return True
    return False


def createclosed(closedfile):
    """

    Parameters
    ----------
    closedfile
    """
    try:
        open(closedfile, "w").close()
    except IOError as error:
        log.exception(f"Could not touch closed file {closedfile}, {error}")


def getclosedlist(sequence_list):
    """

    Parameters
    ----------
    sequence_list

    Returns
    -------

    """
    closed_ls = glob.glob(
        os.path.join(options.directory, "*{0}".format(cfg.get("LSTOSA", "CLOSEDSUFFIX")))
    )
    closed_list = []
    for i in closed_ls:
        # we extract the job name
        name = i.rsplit(cfg.get("LSTOSA", "CLOSEDSUFFIX"))[0].split(
            os.path.join(options.directory, "sequence_")
        )[1]
        closed_list.append(name)
        setclosedaction(name, sequence_list)
    return closed_list


def setclosedaction(name, sequence_list):
    """

    Parameters
    ----------
    name
    sequence_list
    """
    for s in sequence_list:
        if s.jobname == name:
            s.action = "Closed"
            log.debug(f"Attributes of sequence {s.seq} updated")
