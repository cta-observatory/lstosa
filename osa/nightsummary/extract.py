"""
Extract subrun, run, sequence list and build corresponding objects.
"""
import logging

from astropy import units as u
from astropy.time import Time

from osa.configs import options
from osa.configs.datamodel import (
    RunObj,
    SequenceCalibration,
    SequenceData,
    SequenceStereo,
    SubrunObj,
)
from osa.job import setsequencecalibfilenames, setsequencefilenames
from osa.utils.utils import lstdate_to_iso

log = logging.getLogger(__name__)

__all__ = [
    "extractsubruns",
    "extractruns",
    "extractsequences",
    "extractsequencesstereo",
    "generateworkflow",
    "dependsonpreviousseq",
]


def extractsubruns(summary_table):
    """
    Extract sub-wun wise information from RunSummary files

    Parameters
    ----------
    summary_table: astropy.Table
    Table containing run-wise information indicated in
    nightsummary.run_summary.

    See Also: nightsummary.run_summary

    Returns
    -------
    subrun_list

    """
    subrun_list = []
    run_to_obj = {}

    # FIXME: Directly build run object instead.

    # Get information run-wise going through each row
    for run_id in summary_table["run_id"]:
        sr = SubrunObj()
        run_info = summary_table.loc[run_id]
        sr.subrun = run_info["n_subruns"]
        sr.timestamp = Time(run_info["run_start"] * u.ns, format="unix").isot
        sr.ucts_timestamp = run_info["ucts_timestamp"]
        sr.dragon_reference_time = run_info["dragon_reference_time"]
        sr.dragon_reference_module_id = run_info["dragon_reference_module_id"]
        sr.dragon_reference_module_index = run_info["dragon_reference_module_index"]
        sr.dragon_reference_counter = run_info["dragon_reference_counter"]
        sr.dragon_reference_source = run_info["dragon_reference_source"]

        try:
            # Build run object
            sr.runobj = RunObj()
            sr.runobj.run_str = f"{run_info['run_id']:05d}"
            # FIXME: Leave only .run attribute
            sr.runobj.run = run_info["run_id"]
            sr.runobj.type = run_info["run_type"]
            sr.runobj.telescope = options.tel_id
            sr.runobj.night = lstdate_to_iso(options.date)
            run_to_obj[sr.runobj.run] = sr.runobj
        except KeyError as err:
            log.warning(f"Key error, {err}")
        except IndexError as err:
            log.warning(f"Index error, {err}")
        else:
            sr.runobj.subrun_list.append(sr)
            sr.runobj.subruns = len(sr.runobj.subrun_list)
            subrun_list.append(sr)

    log.debug("Subrun list extracted")

    if not subrun_list:
        log.warning("No runs found. Nothing to do.")
        exit(1)

    return subrun_list


def extractruns(subrun_list):
    """

    Parameters
    ----------
    subrun_list
        List of subruns

    Returns
    -------
    run_list

    """
    run_list = []
    for s in subrun_list:
        if s.runobj not in run_list:
            s.runobj.subruns = s.subrun
            run_list.append(s.runobj)

    log.debug("Run list extracted")
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
            # log.debug(f"New source {currentsrc} detected, waiting for PED and CAL")
            hasped = False
            hascal = False
            sources.append(currentsrc)

        if currenttype == "DRS4":
            log.debug(f"Detected a new DRS4 run {currentrun} for {currentsrc}")
            hasped = True
            run_list_sorted.append(r)
        elif currenttype == "PEDCALIB":
            log.debug(f"Detected a new PEDCALIB run {currentrun} for {currentsrc}")
            hascal = True
            run_list_sorted.append(r)

        if hasped is False or hascal is False:
            if currenttype == "DATA":
                log.debug(
                    f"Detected a new DATA run {currentrun} for {currentsrc}, but still no PED/CAL"
                )
                pending.append(r)
        else:
            if currenttype == "DATA":
                # normal case, we have the PED, the SUB, then append the DATA
                log.debug(f"Detected a new DATA run {currentrun} for {currentsrc}")
                run_list_sorted.append(r)
            elif currenttype == "PEDCALIB" and pending != []:
                # we just took the CAL, and we had the PED, so we can add the pending runs
                log.debug("PED/CAL are now available, adding the runs in the pending queue")
                for pr in pending:
                    run_list_sorted.append(pr)
                pending = []

    if pending:
        # we reached the end, we can add the pending runs
        log.debug("Adding the pending runs")
        for pr in pending:
            run_list_sorted.append(pr)

    for i in run_list_sorted:
        currentrun = i.run
        currenttype = i.type

        if len(head) == 0:
            if currenttype == "DRS4":
                # normal case
                log.debug(f"appending [{currentrun}, {currenttype}, {None}]")
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
                log.debug(f"replacing [{currentrun}, {currenttype}, {whichreq}]")
                head[0] = [currentrun, currenttype, whichreq]
            elif currenttype == "PEDCALIB" and previoustype == "DRS4":
                # add it too
                log.debug(f"appending [{currentrun}, {currenttype}, {None}]")
                head.append([currentrun, currenttype, None])
                require[currentrun] = previousrun
            elif currenttype == "DATA":
                if previoustype == "DRS4":
                    # it is the pedestal->data mistake from shifters;
                    # replace and store if they are not the first of observations
                    # required run requirement inherited from pedestal run
                    if previousreq is not None:
                        log.debug(f"P->C, replacing [{currentrun}, {currenttype}, {previousreq}]")
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
                    log.debug(f"D->D, replacing [{currentrun}, {currenttype}, {whichreq}]")
                    head[0] = [currentrun, currenttype, whichreq]
                    store.append(currentrun)
                    require[currentrun] = whichreq
        elif len(head) == 2:
            previoustype = head[1][1]
            if currenttype == "DATA" and previoustype == "PEDCALIB":
                # it is the pedestal->calibration->data case, append, store, resize and replace
                previousrun = head[1][0]
                head.pop()
                log.debug(f"P->C->D, appending [{currentrun}, {currenttype}, {previousrun}]")
                head[0] = [currentrun, currenttype, previousrun]
                store.append(currentrun)
                # this is different from currentrun since it marks parent sequence run
                require[currentrun] = previousrun
            elif currenttype == "DRS4" and previoustype == "PEDCALIB":
                # there was a problem with the previous calibration and shifters decide to give another try
                head.pop()
                log.debug(f"P->C->P, deleting and replacing [{currentrun}, {currenttype}, {None}]")
                head[0] = [currentrun, currenttype, None]

    sequence_list = generateworkflow(run_list_sorted, store, require)
    # ready to return the list of sequences
    log.debug("Sequence list extracted")

    if not store:
        log.warning("No data sequences found. Nothing to do")

    return sequence_list


def extractsequencesstereo(s1_list, s2_list):
    """

    Parameters
    ----------
    s1_list
    s2_list

    Returns
    -------
    ss_list
        Stereo sequence

    """
    ss_list = []
    for s1 in s1_list:
        ss = None
        if s1.type == "DATA":
            for s2 in s2_list:
                if s2.type == "DATA" and s2.run == s1.run:
                    ss = SequenceStereo(s1, s2)
                    ss.seq = len(ss_list)
                    ss.jobname = f"{ss.telescope}_{ss.run:05d}"
                    setsequencefilenames(ss)
                    ss_list.append(ss)
                    break
    log.debug(f"Appended {len(ss_list)} stereo sequences")
    return ss_list


def generateworkflow(run_list, store, require):
    """
    Store correct data sequences to give sequence numbers and parent dependencies

    Parameters
    ----------
    run_list
    store
    require

    Returns
    -------
    sequence_list

    """
    sequence_list = []

    log.debug(f"The storage contains {len(store)} data sequences")

    parent = None
    for r in run_list:
        # the next seq value to assign (if this happens)
        seq = len(sequence_list)
        log.debug(f"trying to assign run {r.run}, type {r.type} to sequence {seq}")
        if r.type == "DATA":
            try:
                store.index(r.run)
            except ValueError:
                # there is nothing really wrong with that, just a DATA run without sequence
                log.warning(f"There is no sequence for data run {r.run}")
            else:
                previousrun = require[r.run]
                for s in sequence_list:
                    if s.run == previousrun:
                        parent = s.seq
                        break
                log.debug(
                    f"Sequence {seq} assigned to run {r.run} whose parent is {parent} with run {previousrun}"
                )
                s = SequenceData(r)
                s.seq = seq
                s.parent = parent
                for p in sequence_list:
                    if p.seq == parent:
                        s.parent_list.append(p)
                        break

                s.previousrun = previousrun
                s.jobname = f"{r.telescope}_{r.run:05d}"
                setsequencefilenames(s)
                if s not in sequence_list:
                    sequence_list.append(s)
        elif r.type == "PEDCALIB":
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
                    setsequencefilenames(s)
                    log.debug(
                        f"Sequence {s.seq} assigned to run {r.run} whose parent is"
                        f" {s.parent} with run {s.previousrun}"
                    )
                    if s not in sequence_list:
                        sequence_list.append(s)
                    break

    # insert the calibration file names
    setsequencecalibfilenames(sequence_list)
    log.debug("Workflow completed")
    return sequence_list


def dependsonpreviousseq(previous, current):
    """

    Parameters
    ----------
    previous
    current

    Returns
    -------

    """
    if options.mode == "P":
        return False
    elif options.mode == "S":
        return True
    elif options.mode is None:
        # not needed, let us assume easy parallel mode
        return False
    else:
        log.error(f"mode {options.mode} not recognized")
