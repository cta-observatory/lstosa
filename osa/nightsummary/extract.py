"""Extract subrun, run, sequence list and build corresponding objects."""

import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List

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
from osa.configs.datamodel import Sequence
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
    "extract_sequences",
    "build_sequences",
    "get_source_list",
]


def get_data_runs(date: datetime):
    """Return the list of DATA runs to analyze based on the run summary table."""
    summary = run_summary_table(date)
    return summary[summary["run_type"] == "DATA"]["run_id"].tolist()


def get_last_drs4(date: datetime) -> int:
    """Return run_id of the last DRS4 run for the given date to be used for data processing."""
    summary = run_summary_table(date)
    return summary[summary["run_type"] == "DRS4"]["run_id"].max()


def get_last_pedcalib(date) -> int:
    """Return run_id of the last PEDCALIB run for the given date to be used for data processing."""
    summary = run_summary_table(date)
    return summary[summary["run_type"] == "PEDCALIB"]["run_id"].max()


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
                obs_id=sr.runobj.run, property_name="DriveControl_SourceName"
            )
            sr.runobj.source_ra = database.query(
                obs_id=sr.runobj.run, property_name="DriveControl_RA_Target"
            )
            sr.runobj.source_dec = database.query(
                obs_id=sr.runobj.run, property_name="DriveControl_Dec_Target"
            )
            # Store this source information (run_id, source_name, source_ra, source_dec)
            # into an astropy Table and save to disk. In this way, the information can be
            # dumped anytime later more easily than accessing the TCU database itself.
            if sr.runobj.source_name is not None:
                line = [
                    sr.runobj.run,
                    sr.runobj.source_name,
                    sr.runobj.source_ra,
                    sr.runobj.source_dec,
                ]
                log.debug(f"Adding line with source info to RunCatalog: {line}")
                run_table.add_row(line)

        # Save table to disk
        run_table.write(source_catalog_file, overwrite=True, delimiter=",")

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


def extract_sequences(date: datetime, run_obj_list: List[RunObj]) -> List[Sequence]:
    """
    Create calibration and data sequences from run objects.

    Parameters
    ----------
    date : datetime
        Date of the runs to analyze
    run_obj_list : List[RunObj]
        List of run objects

    Returns
    -------
    sequence_list : List[Sequence]
    """

    # Last DRS4 and PEDCALIB runs required to process the sky-data runs
    required_drs4_run = get_last_drs4(date)
    required_pedcal_run = get_last_pedcalib(date)

    # Get DATA runs to be processed
    data_runs_to_process = get_data_runs(date)
    if not data_runs_to_process:
        log.warning("No data sequences found for this date. Nothing to do. Exiting.")
        sys.exit(0)

    log.debug(f"There are {len(data_runs_to_process)} data sequences: {data_runs_to_process}")

    # Loop over the list of run objects and create the corresponding sequences
    # First, the calibration sequence based on the last DRS4 and PEDCALIB runs.
    # Then, the data sequences which require the calibration files produced
    # by the calibration sequence.
    sequence_list = []

    for run in run_obj_list:
        if run.run == required_pedcal_run:
            sequence = SequenceCalibration(run)
            sequence.jobname = f"{run.telescope}_{run.run:05d}"
            sequence_list.insert(0, sequence)
            sequence.drs4_run = required_drs4_run
            sequence.pedcal_run = run.run
            sequence_filenames(sequence)
            log.debug(
                f"Calibration sequence {sequence.seq} composed of "
                f"DRS4 run {required_drs4_run} and Ped-Cal run {required_pedcal_run}"
            )

        elif run.run in data_runs_to_process:
            sequence = SequenceData(run)
            # data sequences counted after the calibration sequence
            sequence.seq = data_runs_to_process.index(run.run) + 2
            sequence.jobname = f"{run.telescope}_{run.run:05d}"
            sequence.drs4_run = required_drs4_run
            sequence.pedcal_run = required_pedcal_run
            sequence_filenames(sequence)
            log.debug(
                f"Data sequence {sequence.seq} from run {run.run} whose parent is "
                f"{sequence.parent} (DRS4 {required_drs4_run} & Ped-Cal {required_pedcal_run})"
            )
            sequence_list.append(sequence)

    # Add the calibration file names
    sequence_calibration_files(sequence_list)
    log.debug("Workflow completed")

    log.debug("Sequence list extracted")

    return sequence_list


def build_sequences(date: datetime) -> List:
    """Build the list of sequences to process from a given date."""
    summary_table = run_summary_table(date)
    subrun_list = extractsubruns(summary_table)
    run_list = extractruns(subrun_list)
    # modifies run_list by adding the seq and parent info into runs
    return extract_sequences(date, run_list)


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
        sequence.run: sequence.source_name
        for sequence in sequence_list
        if sequence.source_name is not None
    }

    source_dict_grouped = defaultdict(list)
    for key, val in sorted(source_dict.items()):
        source_dict_grouped[val].append(key)

    if list(source_dict_grouped) is None:
        sys.exit("No sources found. Check the access to database. Exiting.")

    return dict(source_dict_grouped)
