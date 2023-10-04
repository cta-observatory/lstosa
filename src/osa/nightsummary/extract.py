"""Extract subrun, run, sequence list and build corresponding objects."""


import itertools
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
from astropy.table import Table

from osa.configs import options
from osa.configs.config import cfg
from osa.configs.datamodel import (
    RunObj,
    SequenceCalibration,
    SequenceData,
)
from osa.configs.datamodel import Sequence
from osa.job import sequence_filenames
from osa.nightsummary import database
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import sequence_calibration_files, get_run_date
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_iso, date_to_dir

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "extract_runs",
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
    n_max = 4
    n = 1
    while (np.array(summary["run_type"] == "DRS4")).any() == False & n <= n_max:
        date = date - timedelta(days=1)
        summary = run_summary_table(date)
        n += 1

    try:
        return summary[summary["run_type"] == "DRS4"]["run_id"].max()

    except ValueError:
        log.warning("No DRS4 run found. Nothing to do. Exiting.")
        sys.exit(0)


def get_last_pedcalib(date) -> int:
    """Return run_id of the last PEDCALIB run for the given date to be used for data processing."""
    summary = run_summary_table(date)
    n_max = 4
    n = 1
    while (np.array(summary["run_type"] == "PEDCALIB")).any() == False & n <= n_max:
        date = date - timedelta(days=1)
        summary = run_summary_table(date)
        n += 1

    try:
        return summary[summary["run_type"] == "PEDCALIB"]["run_id"].max()

    except ValueError:
        log.warning("No PEDCALIB run found. Nothing to do. Exiting.")
        sys.exit(0)


def extract_runs(summary_table):
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

    if len(summary_table) == 0:
        log.warning("No runs found for this date. Nothing to do. Exiting.")
        sys.exit(0)

    run_list = []

    required_drs4_run = get_last_drs4(options.date)
    required_pedcal_run = get_last_pedcalib(options.date)

    if required_drs4_run not in summary_table["run_id"]:
        drs4_date = get_run_date(required_drs4_run)
        drs4_date_summary = run_summary_table(drs4_date)
        run_info = drs4_date_summary.loc[required_drs4_run]
        run = RunObj(
            run=required_drs4_run,
            run_str=f"{required_drs4_run:05d}",
            type=run_info["run_type"],
            night=date_to_iso(drs4_date),
            subruns=run_info["n_subruns"],
        )
        run_list.append(run)

    if required_pedcal_run not in summary_table["run_id"]:
        pedcal_date = get_run_date(required_pedcal_run)
        pedcal_date_summary = run_summary_table(pedcal_date)
        run_info = pedcal_date_summary.loc[required_pedcal_run]
        run = RunObj(
            run=required_pedcal_run,
            run_str=f"{required_pedcal_run:05d}",
            type=run_info["run_type"],
            night=date_to_iso(pedcal_date),
            subruns=run_info["n_subruns"],
        )
        run_list.append(run)

    # Get information run-wise going through each row
    for run_id in summary_table["run_id"]:
        run_info = summary_table.loc[run_id]
        # Build run object
        run = RunObj(
            run=run_id,
            run_str=f"{run_id:05d}",
            type=run_info["run_type"],
            night=date_to_iso(options.date),
            subruns=run_info["n_subruns"],
        )
        run_list.append(run)

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
        for run_id, run in itertools.product(source_catalog["run_id"], run_list):
            if run.run == run_id:
                run.source_name = source_catalog.loc[run_id]["source_name"]
                run.source_ra = source_catalog.loc[run_id]["source_ra"]
                run.source_dec = source_catalog.loc[run_id]["source_dec"]

    elif database.db_available():
        run_table = Table(
            names=["run_id", "source_name", "source_ra", "source_dec"],
            dtype=["int32", str, "float64", "float64"],
        )
        for run in run_list:
            # Make sure we are looking at actual data runs. Avoid test runs.
            if run.run > 0 and run.type == "DATA":
                log.debug(f"Looking info in TCU DB for run {run.run}")
                run.source_name = database.query(
                    obs_id=run.run, property_name="DriveControl_SourceName"
                )
                run.source_ra = database.query(
                    obs_id=run.run, property_name="DriveControl_RA_Target"
                    )
                run.source_dec = database.query(
                    obs_id=run.run, property_name="DriveControl_Dec_Target"
                    )
                # Store this source information (run_id, source_name, source_ra, source_dec)
                # into an astropy Table and save to disk in RunCatalog files. In this way, the
                # information can be dumped anytime later more easily than accessing the
                # TCU database.
                if run.source_name is not None:
                    line = [
                        run.run,
                        run.source_name,
                        run.source_ra,
                        run.source_dec,
                    ]
                    log.debug(f"Adding line with source info to RunCatalog: {line}")
                    run_table.add_row(line)

        # Save table to disk
        run_table.write(source_catalog_file, overwrite=True, delimiter=",")

    log.debug("Subrun list extracted")

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
    run_list = extract_runs(summary_table)
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
