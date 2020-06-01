from osa.utils.standardhandle import verbose, warning, error, gettag
from osa.utils import options


def createveto(vetofile):
    tag = gettag()
    try:
        open(vetofile, 'w').close()
    except IOError as e:
        error(tag, "Could not touch veto file {0}, {1}".format(vetofile, e), 2)


def isjobvetoed(name, veto_list):
    tag = gettag()
    try:
        veto_list.index(name)
    except ValueError as e:
        return False
    else:
        return True


def getvetolist(sequence_list):
    tag = gettag()
    from glob import glob
    from os.path import join
    from osa.configs.config import cfg
    updatevetos(sequence_list)
    veto_ls = glob(join(options.directory, "*{0}".format(cfg.get('LSTOSA', 'VETOSUFFIX'))))
    veto_list = []
    for i in veto_ls:
        name = i.rsplit(cfg.get('LSTOSA', 'VETOSUFFIX'))[0].split(join(options.directory, 'sequence_'))[1] # We extract the job name
        veto_list.append(name)
        setvetoaction(name, sequence_list)
    return veto_list


def setvetoaction(name, sequence_list):
    tag = gettag()
    for s in sequence_list:
        if s.jobname == name:
            s.action = 'Veto'
            verbose(tag, "Attributes of sequence {0} updated".format(s.seq))


def updatevetos(sequence_list):
    tag = gettag()
    from os.path import exists
    from osa.configs.config import cfg
    for s in sequence_list:
        if (not exists(s.veto) and exists(s.history)):
            if failedhistory(s.history, int(cfg.get('LSTOSA', 'MAXTRYFAILED'))):
                createveto(s.veto)
                verbose(tag, "Created veto file {0}".format(s.veto))


def failedhistory(historyfile, maxnumber):
    tag = gettag()
    from iofile import readfromfile
    programme = []
    card = []
    goal = []
    exit_status = []
    for line in readfromfile(historyfile).splitlines():
        words = line.split()
        # Columns 2, 10 and 11 of history file contains the info (index 1, 9 and 10 respectively)
        programme.append(words[1])
        goal.append(words[8])
        card.append(words[9])
        try:
            exit_status.append(int(words[10]))
        except ValueError as e:
            error(tag, "Malformed file {0}, {1}".format(historyfile, e), 4)
        verbose(tag, "extracting line: {0}".format(line))
    lsize = len(exit_status)
    strike = 0
    if (programme[-1] == "merpp") and (exit_status[-1] == 23):
        return False
    if lsize >= maxnumber and (goal[-1] != "new_calib"):
        for i in range(lsize-1):
            # verbose(tag, "comparing {0}=={1}, {2}=={3}, {4}=={5}".format(exit_status[i], exit_status[lsize-1], card[i], card[lsize-1], programme[i], programme[lsize-1]))
            if ((exit_status[i] != 0) and (exit_status[i] == exit_status[lsize-1]) and (card[i] == card[lsize-1]) and (programme[i] == programme[lsize-1])):
                strike += 1
                if strike == maxnumber - 1:
                    verbose(tag, "Maximum amount of failures reached for {0}".format(historyfile))
                    return True
    return False


def createclosed(closedfile):
    tag = gettag()
    try:
        open(closedfile, 'w').close()
    except IOError as e:
        error(tag, "Could not touch closed file {0}, {1}".format(closedfile, e), 2)


def getclosedlist(sequence_list):
    tag = gettag()
    from glob import glob
    from os.path import join
    from osa.configs.config import cfg
    closed_ls = glob(join(options.directory, '*{0}'.format(cfg.get('LSTOSA', 'CLOSEDSUFFIX'))))
    closed_list = []
    for i in closed_ls:
        # We extract the job name
        name = i.rsplit(cfg.get('LSTOSA', 'CLOSEDSUFFIX'))[0].split(join(options.directory, 'sequence_'))[1]
        closed_list.append(name)
        setclosedaction(name, sequence_list)
    return closed_list


def setclosedaction(name, sequence_list):
    tag = gettag()
    for s in sequence_list:
        if s.jobname == name:
            s.action = 'Closed'
            verbose(tag, "Attributes of sequence {0} updated".format(s.seq))
