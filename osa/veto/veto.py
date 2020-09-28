from glob import glob
from os.path import exists, join

from osa.configs.config import cfg
from osa.configs import options
from osa.utils.iofile import readfromfile
from osa.utils.standardhandle import error, gettag, verbose


def createveto(vetofile):
    tag = gettag()
    try:
        open(vetofile, "w").close()
    except IOError as e:
        error(tag, f"Could not touch veto file {vetofile}, {e}", 2)


def isjobvetoed(name, veto_list):
    try:
        veto_list.index(name)
    except ValueError as e:
        return False
    else:
        return True


def getvetolist(sequence_list):
    tag = gettag()
    updatevetos(sequence_list)
    veto_ls = glob(join(options.directory, "*{0}".format(cfg.get("LSTOSA", "VETOSUFFIX"))))
    veto_list = []
    for i in veto_ls:
        # we extract the job name
        name = i.rsplit(cfg.get("LSTOSA", "VETOSUFFIX"))[0].split(join(options.directory, "sequence_"))[1]
        veto_list.append(name)
        setvetoaction(name, sequence_list)
    return veto_list


def setvetoaction(name, sequence_list):
    tag = gettag()
    for s in sequence_list:
        if s.jobname == name:
            s.action = "Veto"
            verbose(tag, f"Attributes of sequence {s.seq} updated")


def updatevetos(sequence_list):
    tag = gettag()
    for s in sequence_list:
        if not exists(s.veto) and exists(s.history):
            if failedhistory(s.history, int(cfg.get("LSTOSA", "MAXTRYFAILED"))):
                createveto(s.veto)
                verbose(tag, f"Created veto file {s.veto}")


def failedhistory(historyfile, maxnumber):
    tag = gettag()
    programme = []
    card = []
    goal = []
    exit_status = []
    for line in readfromfile(historyfile).splitlines():
        words = line.split()
        # columns 2, 10, 11 and 12 of history file contains the info (index 1, -3, -2 and -1 respectively)
        programme.append(words[1])
        goal.append(words[-3])
        card.append(words[-2])
        try:
            exit_status.append(int(words[-1]))
        except ValueError as e:
            error(tag, f"Malformed file {historyfile}, {e}", 4)
        verbose(tag, "extracting line: {0}".format(line))
    lsize = len(exit_status)
    strike = 0
    if (programme[-1] == "merpp") and (exit_status[-1] == 23):
        return False
    if lsize >= maxnumber and (goal[-1] != "new_calib"):
        for i in range(lsize - 1):
            # m  = f"{exit_status[i]}=={exit_status[lsize-1]}, "
            # m += f"{card[i]=={card[lsize-1]}, {programme[i]}=={programme[lsize-1]}"
            # verbose(tag, f"comparing {m}")
            if (
                (exit_status[i] != 0)
                and (exit_status[i] == exit_status[lsize - 1])
                and (card[i] == card[lsize - 1])
                and (programme[i] == programme[lsize - 1])
            ):
                strike += 1
                if strike == maxnumber - 1:
                    verbose(tag, f"Maximum amount of failures reached for {historyfile}")
                    return True
    return False


def createclosed(closedfile):
    tag = gettag()
    try:
        open(closedfile, "w").close()
    except IOError as e:
        error(tag, f"Could not touch closed file {closedfile}, {e}", 2)


def getclosedlist(sequence_list):
    closed_ls = glob(join(options.directory, "*{0}".format(cfg.get("LSTOSA", "CLOSEDSUFFIX"))))
    closed_list = []
    for i in closed_ls:
        # we extract the job name
        name = i.rsplit(cfg.get("LSTOSA", "CLOSEDSUFFIX"))[0].split(join(options.directory, "sequence_"))[1]
        closed_list.append(name)
        setclosedaction(name, sequence_list)
    return closed_list


def setclosedaction(name, sequence_list):
    tag = gettag()
    for s in sequence_list:
        if s.jobname == name:
            s.action = "Closed"
            verbose(tag, f"Attributes of sequence {s.seq} updated")
