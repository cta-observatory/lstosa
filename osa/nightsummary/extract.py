from datetime import datetime

from datamodel import RunObj, SequenceCalibration, SequenceData, SequenceStereo, SubrunObj
from osa.jobs import job
from osa.jobs.job import setsequencefilenames
from osa.utils import options
from osa.utils.standardhandle import error, gettag, verbose, warning
from osa.utils.utils import lstdate_to_iso

__all__ = [
    "extractsubruns",
    "extractruns",
    "extractsequences",
    "extractsequencesstereo",
    "generateworkflow",
    "dependsonpreviousseq",
    "hastemperaturechanged",
]


def extractsubruns(nightsummary):
    tag = gettag()
    subrun_list = []
    run_to_obj = {}
    if nightsummary:
        for i in nightsummary.splitlines():
            word = i.split()
            current_run_str = word[0]
            current_run = int(current_run_str.lstrip("0"))
            sr = SubrunObj()
            sr.subrun_str = word[1]
            sr.subrun = int(sr.subrun_str.lstrip("0"))
            # sr.kind = str(word[12])
            sr.date = str(word[3])
            sr.time = str(word[4])
            sr.timestamp = datetime.strptime(str(word[3] + " " + word[4]), "%Y-%m-%d %H:%M:%S")
            sr.ucts_t0_dragon = str(word[6])
            sr.dragon_counter0 = str(word[7])
            sr.ucts_t0_tib = str(word[9])
            sr.tib_counter0 = str(word[10])
            try:
                sr.runobj = run_to_obj[current_run]
            except:
                # check if this is the first subrun. If not, print a warning
                if sr.subrun != 1:
                    warning(tag, f"Missing {current_run}.001 subrun")

                # build run object
                sr.runobj = RunObj()
                sr.runobj.run_str = current_run_str
                sr.runobj.run = current_run
                sr.runobj.type = str(word[2])
                # sr.runobj.sourcewobble = str(word[5])
                # sr.runobj.source, sep, sr.runobj.wobble = sr.runobj.sourcewobble.partition('-W')
                if sr.runobj.wobble == "":
                    sr.runobj.wobble = None
                sr.runobj.telescope = options.tel_id
                sr.runobj.night = lstdate_to_iso(options.date)
                run_to_obj[sr.runobj.run] = sr.runobj

            sr.runobj.subrun_list.append(sr)
            sr.runobj.subruns = len(sr.runobj.subrun_list)
            subrun_list.append(sr)
        verbose(tag, "Subrun list extracted")
    return subrun_list


def extractruns(subrun_list):
    tag = gettag()

    # after having extracted properly the subruns, the runobj is there
    run_list = []
    for s in subrun_list:
        if s.runobj not in run_list:
            s.runobj.subruns = s.subrun
            run_list.append(s.runobj)

    verbose(tag, "Run list extracted")
    return run_list


def extractsequences(run_list):
    """This function depends on the selected mode (P, S, T)
    It searches for sequences composed out of
    a) Pedestal->Calibration->Data turns into independent runs
    b) Data[->Pedestal]->Data turns into dependent runs
    c) Otherwise orphan runs which are dismissed
    Parameters
    ----------
    run_list

    Returns
    -------
    sequence_list
    """

    tag = gettag()
    # sequence_list = []  # this is the list of sequence objects to return
    head = []  # this is a set with maximum 3 tuples consisting of [run, type, require]
    store = []  # this is a set with runs which constitute every valid data sequence
    require = dict()  # this is a dictionary with runs as keys and required runs as values

    # create a list of sources. For each, we should have at least a PED, CAL and some DATA
    # if not, then we use the previous PED and CAL. Try to sort this list so that the PED
    # and CAL are in the beginning
    sources = []
    run_list_sorted = []
    pending = []

    for r in run_list:
        # extract the basic info
        currentsrc = r.source
        currentrun = r.run
        currenttype = r.type

        # skip runs not belonging to this telescope ID
        # if (r.telescope!=options.tel_id): continue

        if currentsrc not in sources:
            # verbose(tag, f"New source {currentsrc} detected, waiting for PED and CAL")
            hasped = False
            hascal = False
            sources.append(currentsrc)

        if currenttype == "DRS4":
            verbose(tag, f"Detected a new PED run {currentrun} for {currentsrc}")
            hasped = True
            run_list_sorted.append(r)
        elif currenttype == "CALI":
            verbose(tag, f"Detected a new CAL run {currentrun} for {currentsrc}")
            hascal = True
            run_list_sorted.append(r)

        if hasped is False or hascal is False:
            if currenttype == "DATA":
                verbose(tag, f"Detected a new DATA run {currentrun} for {currentsrc}, but still no PED/CAL")
                pending.append(r)
        else:
            if currenttype == "DATA":
                # normal case, we have the PED, the SUB, then append the DATA
                verbose(tag, f"Detected a new DATA run {currentrun} for {currentsrc}")
                run_list_sorted.append(r)
            elif currenttype == "CALI" and pending != []:
                # we just took the CAL, and we had the PED, so we can add the pending runs
                verbose(tag, "PED/CAL are now available, adding the runs in the pending queue")
                for pr in pending:
                    run_list_sorted.append(pr)
                pending = []

    if pending:
        # we reached the end, we can add the pending runs
        verbose(tag, "Adding the pending runs")
        for pr in pending:
            run_list_sorted.append(pr)

    for i in run_list_sorted:
        currentrun = i.run
        currenttype = i.type

        if len(head) == 0:
            if currenttype == "DRS4":
                # normal case
                verbose(tag, f"appending [{currentrun}, {currenttype}, {None}]")
                head.append([currentrun, currenttype, None])
        elif len(head) == 1:
            previousrun = head[0][0]
            previoustype = head[0][1]
            previousreq = head[0][2]
            whichreq = None
            if currentrun == previousrun:
                # it shouldn't happen, same run number, just skip to next run
                continue
            if currenttype == "DRS4":
                if previoustype == "DATA":
                    # replace the first head element, keeping its previous run or requirement run, depending on mode
                    if dependsonpreviousseq(previousrun, currentrun):
                        whichreq = previousrun
                    else:
                        whichreq = previousreq
                elif previoustype == "DRS4":
                    # one pedestal after another, keep replacing
                    whichreq = None
                verbose(tag, f"replacing [{currentrun}, {currenttype}, {whichreq}]")
                head[0] = [currentrun, currenttype, whichreq]
            elif currenttype == "CALI" and previoustype == "DRS4":
                # add it too
                verbose(tag, f"appending [{currentrun}, {currenttype}, {None}]")
                head.append([currentrun, currenttype, None])
                require[currentrun] = previousrun
            elif currenttype == "DATA":
                if previoustype == "DRS4":
                    # it is the pedestal->data mistake from shifters;
                    # replace and store if they are not the first of observations
                    # required run requirement inherited from pedestal run
                    if previousreq is not None:
                        verbose(tag, f"P->C, replacing [{currentrun}, {currenttype}, {previousreq}]")
                        head[0] = [currentrun, currenttype, previousreq]
                        store.append(currentrun)
                        require[currentrun] = previousreq
                elif previoustype == "DATA":
                    # it is the data->data case, replace and store
                    # the whole policy has to be applied here:
                    # if P=parallel, the dependence is previousreq
                    # if S=sequential, the dependence is previousrun
                    # if T=temperature-aware, the dependence has to be evaluated by a function
                    if dependsonpreviousseq(previousrun, currentrun):
                        whichreq = previousrun
                    else:
                        whichreq = previousreq
                    verbose(tag, f"D->D, replacing [{currentrun}, {currenttype}, {whichreq}]")
                    head[0] = [currentrun, currenttype, whichreq]
                    store.append(currentrun)
                    require[currentrun] = whichreq
        elif len(head) == 2:
            previoustype = head[1][1]
            if currenttype == "DATA" and previoustype == "CALI":
                # it is the pedestal->calibration->data case, append, store, resize and replace
                previousrun = head[1][0]
                head.pop()
                verbose(tag, f"P->C->D, appending [{currentrun}, {currenttype}, {previousrun}]")
                head[0] = [currentrun, currenttype, previousrun]
                store.append(currentrun)
                # this is different from currentrun since it marks parent sequence run
                require[currentrun] = previousrun
            elif currenttype == "DRS4" and previoustype == "CALI":
                # there was a problem with the previous calibration and shifters decide to give another try
                head.pop()
                verbose(tag, f"P->C->P, deleting and replacing [{currentrun}, {currenttype}, {None}]")
                head[0] = [currentrun, currenttype, None]

    sequence_list = generateworkflow(run_list_sorted, store, require)
    # ready to return the list of sequences
    verbose(tag, "Sequence list extracted")

    return sequence_list


def extractsequencesstereo(s1_list, s2_list):
    tag = gettag()
    ss_list = []
    for s1 in s1_list:
        ss = None
        if s1.type == "DATA":
            for s2 in s2_list:
                if s2.type == "DATA" and s2.run == s1.run:
                    ss = SequenceStereo(s1, s2)
                    ss.seq = len(ss_list)
                    ss.jobname = f"{ss.telescope}_{str(ss.run).zfill(5)}"
                    setsequencefilenames(ss)
                    ss_list.append(ss)
                    break
    verbose(tag, f"Appended {len(ss_list)} stereo sequences")
    return ss_list


def generateworkflow(run_list, store, require):
    tag = gettag()
    # we have a store set with correct data sequences to give seq numbers and parent dependencies
    sequence_list = []
    verbose(tag, f"The storage contains {len(store)} data sequences")
    parent = None
    for r in run_list:
        # the next seq value to assign (if this happens)
        seq = len(sequence_list)
        # verbose(tag, f"trying to assing run {r.run}, type {r.type} to sequence {seq}")
        if r.type == "DATA":
            try:
                store.index(r.run)
            except ValueError:
                # there is nothing really wrong with that, just a DATA run without sequence
                warning(tag, f"There is no sequence for data run {r.run}")
            else:
                previousrun = require[r.run]
                for s in sequence_list:
                    if s.run == previousrun:
                        parent = s.seq
                        break
                verbose(tag, f"Sequence {seq} assigned to run {r.run} whose parent is {parent} with run {previousrun}")
                s = SequenceData(r)
                s.seq = seq
                s.parent = parent
                for p in sequence_list:
                    if p.seq == parent:
                        s.parent_list.append(p)
                        break

                s.previousrun = previousrun
                s.jobname = f"{r.telescope}_{str(r.run).zfill(5)}"
                job.setsequencefilenames(s)
                if s not in sequence_list:
                    sequence_list.append(s)
        elif r.type == "CALI":
            # calibration sequence are appended to the sequence list if they are parent from data sequences
            for k in iter(require):
                if r.run == require[k]:
                    previousrun = require[r.run]

                    # we found that this calibration is required
                    s = SequenceCalibration(r)
                    s.seq = seq
                    s.parent = None
                    s.previousrun = previousrun
                    s.jobname = f"{r.telescope}_{str(r.run).zfill(5)}"
                    job.setsequencefilenames(s)
                    verbose(tag, f"Sequence {s.seq} assigned to run {r.run} whose parent is" f" {s.parent} with run {s.previousrun}")
                    if s not in sequence_list:
                        sequence_list.append(s)
                    break

    # insert the calibration file names
    job.setsequencecalibfilenames(sequence_list)
    verbose(tag, "Workflow completed")
    return sequence_list


def dependsonpreviousseq(previous, current):
    tag = gettag()
    if options.mode == "P":
        return False
    elif options.mode == "S":
        return True
    elif options.mode == "T":
        if hastemperaturechanged(previous, current):
            return True
        else:
            return False
    elif options.mode is None:
        # not needed, let us assume easy parallel mode
        return False
    else:
        error(tag, f"mode {options.mode} not recognized", 2)


def hastemperaturechanged(previous, current):
    tag = gettag()
    return False
