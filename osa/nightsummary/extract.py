"""Extract subrun, run, sequence list and build corresponding objects."""

import logging
import sys

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
from osa.job import sequence_calibration_filenames, sequence_filenames
from osa.nightsummary.database import query
from osa.utils.utils import lstdate_to_iso
from pymongo.errors import ServerSelectionTimeoutError

log = logging.getLogger(__name__)

__all__ = [
    "extractsubruns",
    "extractruns",
    "extractsequences",
    "extractsequencesstereo",
    "generate_workflow",
]


def extractsubruns(summary_table):
    """
    Extract sub-wun wise information from RunSummary files.

    Parameters
    ----------
    summary_table: astropy.Table
    Table containing run-wise information indicated in `nightsummary.run_summary`.

    See Also: `nightsummary.run_summary`

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
            sr.runobj.run = run_info["run_id"]
            sr.runobj.type = run_info["run_type"]
            sr.runobj.telescope = options.tel_id
            sr.runobj.night = lstdate_to_iso(options.date)
            if not options.test:
                sr.runobj.source = query(
                    obs_id=sr.runobj.run,
                    property_name="DriveControl_SourceName"
                )
                sr.runobj.source_ra = query(
                    obs_id=sr.runobj.run,
                    property_name="DriveControl_RA_Target"
                )
                sr.runobj.source_dec = query(
                    obs_id=sr.runobj.run,
                    property_name="DriveControl_Dec_Target"
                )
            run_to_obj[sr.runobj.run] = sr.runobj
        except KeyError as err:
            log.warning(f"Key error, {err}")
        except IndexError as err:
            log.warning(f"Index error, {err}")
        except ServerSelectionTimeoutError:
            log.warning("MongoDB server not available.")
        else:
            sr.runobj.subrun_list.append(sr)
            sr.runobj.subruns = len(sr.runobj.subrun_list)
            subrun_list.append(sr)

    log.debug("Subrun list extracted")

    if not subrun_list:
        log.warning("No runs found for this date. Nothing to do. Exiting.")
        sys.exit(0)

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
    for subrun in subrun_list:
        if subrun.runobj not in run_list:
            subrun.runobj.subruns = subrun.subrun
            run_list.append(subrun.runobj)

    log.debug("Run list extracted")
    return run_list


def extractsequences(run_list):
    """
    Search for sequences composed out of
    a) Pedestal->Calibration->Data turns into independent runs
    b) Data[->Pedestal]->Data turns into dependent runs
    c) Otherwise orphan runs which are dismissed

    Parameters
    ----------
    run_list

    Returns
    -------
    sequence_list: Iterable
    """

    # sequence_list = []  # this is the list of sequence objects to return
    head = []  # this is a set with maximum 3 tuples consisting of [run, type, require]
    store = []  # this is a set with runs which constitute every valid data sequence
    require = {}

    # create a list of sources. For each, we should have
    # at least a PED, CAL and some DATA. If not, then we use
    # the previous PED and CAL. Try to sort this list so that
    # the PED and CAL are in the beginning
    sources = []
    run_list_sorted = []
    pending = []

    for run in run_list:
        # extract the basic info
        currentsrc = run.source
        currentrun = run.run
        currenttype = run.type

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
            run_list_sorted.append(run)
        elif currenttype == "PEDCALIB":
            log.debug(f"Detected a new PEDCALIB run {currentrun} for {currentsrc}")
            hascal = True
            run_list_sorted.append(run)

        if currenttype == "DATA":
            if hasped is False or hascal is False:
                log.debug(
                    f"Detected a new DATA run {currentrun} for "
                    f"{currentsrc}, but still no PED/CAL"
                )
                pending.append(run)
            else:
                # normal case, we have the PED, the SUB, then append the DATA
                log.debug(f"Detected a new DATA run {currentrun} for {currentsrc}")
                run_list_sorted.append(run)
    if pending:
        # we reached the end, we can add the pending runs
        log.debug("Adding the pending runs")
        for pr in pending:
            run_list_sorted.append(pr)

    for i in run_list_sorted:
        currentrun = i.run
        currenttype = i.type

        if not head:
            if currenttype == "DRS4":
                # normal case
                log.debug(f"appending [{currentrun}, {currenttype}, None]")
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
                    # replace the first head element, keeping its previous run
                    # or requirement run, depending on mode
                    whichreq = previousreq
                elif previoustype == "DRS4":
                    # one pedestal after another, keep replacing
                    whichreq = None
                log.debug(f"replacing [{currentrun}, {currenttype}, {whichreq}]")
                head[0] = [currentrun, currenttype, whichreq]
            elif currenttype == "PEDCALIB" and previoustype == "DRS4":
                # add it too
                log.debug(f"appending [{currentrun}, {currenttype}, None]")
                head.append([currentrun, currenttype, None])
                require[currentrun] = previousrun
            elif currenttype == "DATA":
                if previoustype == "DRS4":
                    # it is the pedestal->data mistake from shifters;
                    # replace and store if they are not the first of observations
                    # required run requirement inherited from pedestal run
                    if previousreq is not None:
                        log.debug(
                            f"P->C, replacing "
                            f"[{currentrun}, {currenttype}, {previousreq}]"
                        )
                        head[0] = [currentrun, currenttype, previousreq]
                        store.append(currentrun)
                        require[currentrun] = previousreq
                elif previoustype == "DATA":
                    whichreq = previousreq

                    log.debug(
                        f"D->D, " f"replacing [{currentrun}, {currenttype}, {whichreq}]"
                    )
                    head[0] = [currentrun, currenttype, whichreq]
                    store.append(currentrun)
                    require[currentrun] = whichreq
        elif len(head) == 2:
            previoustype = head[1][1]
            if currenttype == "DATA" and previoustype == "PEDCALIB":
                # it is the pedestal->calibration->data case,
                # append, store, resize and replace
                previousrun = head[1][0]
                head.pop()
                log.debug(
                    f"P->C->D,"
                    f"appending "
                    f"[{currentrun}, {currenttype}, {previousrun}]"
                )
                head[0] = [currentrun, currenttype, previousrun]
                store.append(currentrun)
                # this is different from currentrun since it marks parent sequence run
                require[currentrun] = previousrun
            elif currenttype == "DRS4" and previoustype == "PEDCALIB":
                # there was a problem with the previous calibration
                # and shifters decide to give another try
                head.pop()
                log.debug(
                    "P->C->P, "
                    f"deleting and replacing [{currentrun}, {currenttype}, None]"
                )

                head[0] = [currentrun, currenttype, None]

    sequence_list = generate_workflow(run_list_sorted, store, require)
    # ready to return the list of sequences
    log.debug("Sequence list extracted")

    if not store:
        log.warning("No data sequences found for this date. Nothing to do. Exiting.")
        sys.exit(0)

    return sequence_list


def extractsequencesstereo(seq1_list, seq2_list):
    """
    Build stereo sequences from two lists of single-telescope sequences.

    Parameters
    ----------
    seq1_list
    seq2_list

    Returns
    -------
    stereo_seq_list: list
        Stereo sequences list
    """
    stereo_seq_list = []
    for seq1 in seq1_list:
        if seq1.type == "DATA":
            for seq2 in seq2_list:
                if seq2.type == "DATA" and seq2.run == seq1.run:
                    stereo_seq = SequenceStereo(seq1, seq2)
                    stereo_seq.seq = len(stereo_seq_list)
                    stereo_seq.jobname = f"{stereo_seq.telescope}_{stereo_seq.run:05d}"
                    sequence_filenames(stereo_seq)
                    stereo_seq_list.append(stereo_seq)
                    break
    log.debug(f"Appended {len(stereo_seq_list)} stereo sequences")
    return stereo_seq_list


def generate_workflow(run_list, store, require):
    """
    Store correct data sequences to give sequence
    numbers and parent dependencies

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
    for run in run_list:
        # the next seq value to assign (if this happens)
        n_seq = len(sequence_list)
        log.debug(f"Trying to assign run {run.run}, type {run.type} to sequence {n_seq}")
        if run.type == "DATA":
            try:
                store.index(run.run)
            except ValueError:
                # there is nothing really wrong with that,
                # just a DATA run without sequence
                log.warning(f"There is no sequence for data run {run.run}")
            else:
                previousrun = require[run.run]
                for sequence in sequence_list:
                    if sequence.run == previousrun:
                        parent = sequence.seq
                        break
                log.debug(
                    f"Sequence {n_seq} assigned to run {run.run} whose "
                    f"parent is {parent} with run {previousrun}"
                )
                sequence = SequenceData(run)
                sequence.seq = n_seq
                sequence.parent = parent
                for parent_sequence in sequence_list:
                    if parent_sequence.seq == parent:
                        sequence.parent_list.append(parent_sequence)
                        break

                sequence.previousrun = previousrun
                sequence.jobname = f"{run.telescope}_{run.run:05d}"
                sequence_filenames(sequence)
                if sequence not in sequence_list:
                    sequence_list.append(sequence)
        elif run.type == "PEDCALIB":
            # calibration sequence are appended to the sequence
            # list if they are parent from data sequences
            for k in iter(require):
                if run.run == require[k]:
                    previousrun = require[run.run]

                    # we found that this calibration is required
                    sequence = SequenceCalibration(run)
                    sequence.seq = n_seq
                    sequence.parent = None
                    sequence.previousrun = previousrun
                    sequence.jobname = f"{run.telescope}_{str(run.run).zfill(5)}"
                    sequence_filenames(sequence)
                    log.debug(
                        f"Sequence {sequence.seq} assigned to run {run.run} whose "
                        f"parent is {sequence.parent} with run {sequence.previousrun}"
                    )
                    if sequence not in sequence_list:
                        sequence_list.append(sequence)
                    break

    # insert the calibration file names
    sequence_calibration_filenames(sequence_list)
    log.debug("Workflow completed")
    return sequence_list
