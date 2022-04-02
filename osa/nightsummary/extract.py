"""Extract subrun, run, sequence list and build corresponding objects."""

import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from astropy import units as u
from astropy.table import Table
from astropy.time import Time

from osa.configs import options
from osa.configs.config import cfg
from osa.configs.datamodel import (
    RunObj,
    SequenceCalibration,
    SequenceData,
    SubrunObj,
)
from osa.job import sequence_filenames
from osa.nightsummary import database
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import sequence_calibration_files
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_iso, date_to_dir

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "extractsubruns",
    "extractruns",
    "extractsequences",
    "generate_workflow",
    "sort_run_list",
    "build_sequences",
    "get_source_list"
]


def extractsubruns(summary_table):
    """
    Extract sub-wun wise information from RunSummary files.

    Parameters
    ----------
    summary_table: astropy.Table
        Table containing run-wise information indicated in `nightsummary.run_summary`

    See Also: `nightsummary.run_summary`

    Returns
    -------
    subrun_list
    """
    subrun_list = []

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
            sr.runobj.run = int(run_info["run_id"])
            sr.runobj.type = run_info["run_type"]
            sr.runobj.telescope = options.tel_id
            sr.runobj.night = date_to_iso(options.date)
        except KeyError as err:
            log.warning(f"Key error, {err}")
        except IndexError as err:
            log.warning(f"Index error, {err}")
        else:
            sr.runobj.subrun_list.append(sr)
            sr.runobj.subruns = len(sr.runobj.subrun_list)
            subrun_list.append(sr)

    # Before trying to access the information in the database, check if it is available
    # in the RunCatalog file (previously generated with the information in the database).
    nightdir = date_to_dir(options.date)
    source_catalog_dir = Path(cfg.get("LST1", "RUN_CATALOG"))
    source_catalog_dir.mkdir(parents=True, exist_ok=True)
    source_catalog_file = source_catalog_dir / f"RunCatalog_{nightdir}.ecsv"

    if source_catalog_file.exists():
        log.debug(f"RunCatalog file found: {source_catalog_file}")
        source_catalog = Table.read(source_catalog_file)
        # Add index to be able to browse the table
        source_catalog.add_index("run_id")

        # Get information run-wise going through each row of the RunCatalog file
        # and assign it to the corresponding run object.
        for run_id in source_catalog["run_id"]:
            for sr in subrun_list:
                if sr.runobj.run == run_id:
                    sr.runobj.source_name = source_catalog.loc[run_id]["source_name"]
                    sr.runobj.source_ra = source_catalog.loc[run_id]["source_ra"]
                    sr.runobj.source_dec = source_catalog.loc[run_id]["source_dec"]

    # Add metadata from TCU database if available
    # and store it in a ECSV file to be re-used
    elif database.db_available():
        run_table = Table(
            names=["run_id", "source_name", "source_ra", "source_dec"],
            dtype=["int32", str, "float64", "float64"],
        )
        for sr in subrun_list:
            sr.runobj.source_name = database.query(
                obs_id=sr.runobj.run,
                property_name="DriveControl_SourceName"
            )
            sr.runobj.source_ra = database.query(
                obs_id=sr.runobj.run,
                property_name="DriveControl_RA_Target"
            )
            sr.runobj.source_dec = database.query(
                obs_id=sr.runobj.run,
                property_name="DriveControl_Dec_Target"
            )
            # Store this source information (run_id, source_name, source_ra, source_dec)
            # into an astropy Table and save to disk. In this way, the information can be
            # dumped anytime later more easily than accessing the TCU database itself.
            if sr.runobj.source_name is not None:
                line = [
                    sr.runobj.run,
                    sr.runobj.source_name,
                    sr.runobj.source_ra,
                    sr.runobj.source_dec
                ]
                log.debug(f"Adding line with source info to RunCatalog: {line}")
                run_table.add_row(line)

        # Save table to disk
        run_table.write(
            source_catalog_file,
            overwrite=True,
            delimiter=","
        )

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


def sort_run_list(run_list):
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
    run_list: Iterable
    """

    # Create a list of sources. For each, we should have at least a DRS4, PEDCALIB and
    # some DATA. If not, then we use the previous DRS4 and PEDCALIB. Try to sort this
    # list so that the PED and CAL are in the beginning
    sources = []
    run_list_sorted = []
    pending = []
    hasped = False
    hascal = False

    for run in run_list:
        currentsrc = run.source_name
        currentrun = run.run
        currenttype = run.type

        if currentsrc not in sources:
            log.debug(f"New source {currentsrc} found")
            sources.append(currentsrc)

        if currenttype == "DRS4":
            log.debug(f"Detected a new DRS4 run {currentrun}")
            hasped = True
            run_list_sorted.append(run)
        elif currenttype == "PEDCALIB":
            log.debug(f"Detected a new PEDCALIB run {currentrun}")
            hascal = True
            run_list_sorted.append(run)

        if currenttype == "DATA":
            if not hasped or not hascal:
                log.debug(
                    f"Detected a new DATA run {currentrun} for "
                    f"{currentsrc}, but no DRS4/PEDCAL runs yet"
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

    return run_list_sorted


def extractsequences(run_list_sorted):
    """
    Create the sequence list from the sorted run list

    Parameters
    ----------
    run_list_sorted

    Returns
    -------
    sequence_list: Iterable
    """

    head = []  # this is a set with maximum 3 tuples consisting of [run, type, require]
    sequences_to_analyze = []  # set with runs which constitute every valid data sequence
    require = {}

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
                        sequences_to_analyze.append(currentrun)
                        require[currentrun] = previousreq
                elif previoustype == "DATA":
                    whichreq = previousreq
                    log.debug(
                        f"D->D, " f"replacing [{currentrun}, {currenttype}, {whichreq}]"
                    )
                    head[0] = [currentrun, currenttype, whichreq]
                    sequences_to_analyze.append(currentrun)
                    require[currentrun] = whichreq

        elif len(head) == 2:
            previoustype = head[1][1]
            if currenttype == "DATA" and previoustype == "PEDCALIB":
                # it is the pedestal->calibration->data case,
                # append, store, resize and replace
                previousrun = head[1][0]
                head.pop()
                log.debug(
                    f"P->C->D, appending [{currentrun}, {currenttype}, {previousrun}]"
                )
                head[0] = [currentrun, currenttype, previousrun]
                sequences_to_analyze.append(currentrun)
                # this is different from currentrun since it marks parent sequence run
                require[currentrun] = previousrun
            elif currenttype == "DRS4" and previoustype == "PEDCALIB":
                # there was a problem with the previous calibration
                # and shifters decide to give another try
                head.pop()
                log.debug(
                    "P->C->P, deleting and replacing [{currentrun}, {currenttype}, None]"
                )
                head[0] = [currentrun, currenttype, None]

    if not sequences_to_analyze:
        log.warning("No data sequences found for this date. Nothing to do. Exiting.")
        sys.exit(0)

    sequence_list = generate_workflow(run_list_sorted, sequences_to_analyze, require)

    log.debug("Sequence list extracted")

    return sequence_list


def generate_workflow(run_list, sequences_to_analyze, require):
    """
    Store correct data sequences to give sequence
    numbers and parent dependencies

    Parameters
    ----------
    run_list
    sequences_to_analyze
    require

    Returns
    -------
    sequence_list
    """
    sequence_list = []

    log.debug(f"There are {len(sequences_to_analyze)} data sequences")

    parent = None
    for run in run_list:
        # the next seq value to assign (if this happens)
        n_seq = len(sequence_list)
        log.debug(f"Trying to assign run {run.run}, type {run.type} to sequence {n_seq}")
        if run.type == "DATA":
            try:
                sequences_to_analyze.index(run.run)
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
    sequence_calibration_files(sequence_list)
    log.debug("Workflow completed")
    return sequence_list


def build_sequences(date: datetime):
    """Build the list of sequences to process from a given date."""
    summary_table = run_summary_table(date)
    subrun_list = extractsubruns(summary_table)
    run_list = extractruns(subrun_list)
    # modifies run_list by adding the seq and parent info into runs
    sorted_run_list = sort_run_list(run_list)
    return extractsequences(sorted_run_list)


def get_source_list(date: datetime) -> dict:
    """
    Get the list of sources from the sequences' information and corresponding runs.

    Parameters
    ----------
    date : datetime

    Returns
    -------
    sources : Dict[str, list]
    """

    # Build the sequences
    sequence_list = build_sequences(date)

    # Create a dictionary of sources and their corresponding sequences
    source_dict = {
        sequence.run: sequence.source_name for sequence in sequence_list
        if sequence.source_name is not None
    }

    source_dict_grouped = defaultdict(list)
    for key, val in sorted(source_dict.items()):
        source_dict_grouped[val].append(key)

    if list(source_dict_grouped) is None:
        sys.exit("No sources found. Check the access to database. Exiting.")

    return dict(source_dict_grouped)
