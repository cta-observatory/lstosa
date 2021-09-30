"""
Create a run summary for a given date containing the number of subruns,
the start time of the run, type pf the run: DATA, DRS4, CALI, and
the reference timestamp and counter of the run.
"""

import argparse
import logging
import os
from collections import Counter
from glob import glob
from pathlib import Path

import numpy as np
from astropy import units as u
from astropy.table import Table
from astropy.time import Time
from ctapipe.containers import EventType
from ctapipe_io_lst import (
    CDTS_AFTER_37201_DTYPE,
    CDTS_BEFORE_37201_DTYPE,
    DRAGON_COUNTERS_DTYPE,
    LSTEventSource,
    MultiFiles,
)
from ctapipe_io_lst.event_time import calc_dragon_time, combine_counters
from lstchain.paths import parse_r0_filename
from traitlets.config import Config

log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Create run summary file")

parser.add_argument(
    "-d",
    "--date",
    help="Date for the creation of the run summary in format YYYYMMDD",
    required=True,
)

parser.add_argument(
    "--r0-path",
    type=Path,
    help="Path to the R0 files. Default is /fefs/aswg/data/real/R0",
    default=Path("/fefs/aswg/data/real/R0"),
)

dtypes = {
    "time_start": str,
    "time_end": str,
    "elapsed": u.quantity.Quantity,
}


def get_list_of_files(r0_path):
    """
    Get the list of R0 files from a given date.

    Parameters
    ----------
    r0_path : pathlib.Path
        Path to the R0 files

    Returns
    -------
    list_of_files: pathlib.Path.glob
        List of files
    """
    return r0_path.glob("LST*.fits.fz")


def get_list_of_runs(list_of_files):
    """
    Get the sorted list of run objects from R0 filenames.

    Parameters
    ----------
    list_of_files : pathlib.Path.glob
        List of files
    Returns
    -------
    list_of_run_objects
    """
    return sorted(parse_r0_filename(file) for file in list_of_files)


def get_runs_and_subruns(list_of_run_objects, stream=1):
    """
    Get the list of run numbers and the number of sequential files (subruns)
    of each run.

    Parameters
    ----------
    list_of_run_objects
    stream: int, optional
        Number of the stream to obtain the number of sequential files (default is 1).

    Returns
    -------
    (run, number_of_files) : tuple
        Run numbers and corresponding subrun of each run.
    """
    list_filtered_stream = filter(lambda x: x.stream == stream, list_of_run_objects)

    run, number_of_files = np.unique(
        list(map(lambda x: x.run, list_filtered_stream)), return_counts=True
    )

    return run, number_of_files


def type_of_run(date_path, run_number, counters, n_events=200):
    """
    Guessing empirically the type of run based on the percentage of
    pedestals/mono trigger types from the first n_events:
    DRS4 pedestal run (DRS4): 100% mono events (trigger_type == 1)
    cosmic data run (DATA): <10% pedestal events (trigger_type == 32)
    pedestal-calibration run (PEDCALIB): ~50% mono, ~50% pedestal events
    Otherwise (ERROR) the run is not expected to be processed.
    This method may not give always the correct type.
    At some point this should be taken directly from TCU.

    Parameters
    ----------
    date_path : pathlib.Path
        Path to the R0 files
    run_number : int
        Run id
    counters : dict
        Dict containing the reference counters and timestamps
    n_events : int
        Number of events used to infer the type of the run

    Returns
    -------
    run_type: str
        Type of run (DRS4, PEDCALIB, DATA, ERROR)
    """

    pattern = f"LST-1.1.Run{run_number:05d}.0000*.fits.fz"
    list_of_files = sorted(date_path.glob(pattern))
    if len(list_of_files) == 0:
        log.error(f"First subrun not found for {pattern}")
        return "ERROR"

    filename = list_of_files[0]

    config = Config()
    config.EventTimeCalculator.dragon_reference_time = int(counters["dragon_reference_time"])
    config.EventTimeCalculator.dragon_reference_counter = int(counters["dragon_reference_counter"])
    config.EventTimeCalculator.dragon_module_id = int(counters["dragon_reference_module_id"])

    try:
        with LSTEventSource(filename, config=config, max_events=n_events) as source:
            source.log.setLevel(logging.ERROR)

            event_type_counts = Counter(event.trigger.event_type for event in source)
            n_pedestals = event_type_counts[EventType.SKY_PEDESTAL]
            n_subarray = event_type_counts[EventType.SUBARRAY]

        if n_subarray / n_events > 0.999:
            run_type = "DRS4"
        elif n_pedestals / n_events > 0.1:
            run_type = "PEDCALIB"
        elif n_pedestals / n_events < 0.1:
            run_type = "DATA"
        else:
            run_type = "ERROR"

    except (AttributeError, ValueError, IOError, IndexError) as err:
        log.debug(f"File {filename} has error: {err!r}")

        run_type = "ERROR"

    return run_type


def read_counters(date_path, run_number):
    """
    Get initial valid timestamps from the first subrun.
    Write down the reference Dragon module used, reference event_id.

    Parameters
    ----------
    date_path: pathlib.Path
        Directory that contains the R0 files
    run_number: int
        Number of the run

    Returns
    -------
    dict: reference counters and timestamps
    """
    pattern = date_path / f"LST-1.*.Run{run_number:05d}.0000*.fits.fz"
    try:
        f = MultiFiles(glob(str(pattern)))
        first_event = next(f)

        if first_event.event_id != 1:
            raise ValueError("Must be used on first file streams (subrun)")

        module_index = np.where(first_event.lstcam.module_status)[0][0]
        module_id = np.where(f.camera_config.lstcam.expected_modules_id == module_index)[0][0]
        dragon_counters = first_event.lstcam.counters.view(DRAGON_COUNTERS_DTYPE)
        dragon_reference_counter = combine_counters(
            dragon_counters["pps_counter"][module_index],
            dragon_counters["tenMHz_counter"][module_index],
        )

        ucts_available = bool(first_event.lstcam.extdevices_presence & 2)
        run_start = int(round(Time(f.camera_config.date, format="unix").unix_tai)) * int(1e9)

        if ucts_available:
            if int(f.camera_config.lstcam.idaq_version) > 37201:
                cdts = first_event.lstcam.cdts_data.view(CDTS_AFTER_37201_DTYPE)
            else:
                cdts = first_event.lstcam.cdts_data.view(CDTS_BEFORE_37201_DTYPE)

            ucts_timestamp = np.int64(cdts["timestamp"][0])
            dragon_reference_time = ucts_timestamp
            dragon_reference_source = "ucts"
        else:
            ucts_timestamp = np.int64(-1)
            dragon_reference_time = run_start
            dragon_reference_source = "run_start"

        return dict(
            ucts_timestamp=ucts_timestamp,
            run_start=run_start,
            dragon_reference_time=dragon_reference_time,
            dragon_reference_module_id=module_id,
            dragon_reference_module_index=module_index,
            dragon_reference_counter=dragon_reference_counter,
            dragon_reference_source=dragon_reference_source,
        )

    except Exception as err:
        log.debug(f"Files {pattern} have error: {err}")

        return dict(
            ucts_timestamp=-1,
            run_start=-1,
            dragon_reference_time=-1,
            dragon_reference_module_id=-1,
            dragon_reference_module_index=-1,
            dragon_reference_counter=-1,
            dragon_reference_source=None,
        )


def get_time_start_end_of_run(date_path, run_number, num_files):
    """
    Get final timestamps from the last subrun.
    Write down the reference Dragon module used, reference event_id.

    Parameters
    ----------
    date_path: pathlib.Path
        Directory that contains the R0 files
    run_number: int
        Number of the run
    num_files: int
        Number of the sequential files (subruns) of a given run

    Returns
    -------
    end_timestamp
    """

    try:
        # First subrun
        pattern_first_subrun = date_path / f"LST-1.*.Run{run_number:05d}.0000*.fits.fz"

        first_file = MultiFiles(glob(str(pattern_first_subrun)))
        first_event_first_file = next(first_file)
        ucts_available = bool(first_event_first_file.lstcam.extdevices_presence & 2)

        module_index = np.where(first_event_first_file.lstcam.module_status)[0][0]
        module_id = np.where(first_file.camera_config.lstcam.expected_modules_id == module_index)[
            0
        ][0]
        dragon_counters = first_event_first_file.lstcam.counters.view(DRAGON_COUNTERS_DTYPE)
        dragon_reference_counter = combine_counters(
            dragon_counters["pps_counter"][module_index],
            dragon_counters["tenMHz_counter"][module_index],
        )

        if ucts_available:
            if int(first_file.camera_config.lstcam.idaq_version) > 37201:
                cdts = first_event_first_file.lstcam.cdts_data.view(CDTS_AFTER_37201_DTYPE)
            else:
                cdts = first_event_first_file.lstcam.cdts_data.view(CDTS_BEFORE_37201_DTYPE)

            ucts_timestamp = np.int64(cdts["timestamp"][0])
            reference_time = ucts_timestamp
            run_start_first_file = Time(ucts_timestamp * u.ns, format="unix")
        else:
            run_start_first_file = Time(first_file.camera_config.date, format="unix")
            reference_time = int(
                round(Time(first_file.camera_config.date, format="unix").unix_tai)
            ) * int(1e9)

        # Last subrun
        last_subrun = num_files - 1  # first subrun is 0
        pattern_last_subrun = date_path / f"LST-1.*.Run{run_number:05d}.{last_subrun:04d}*.fits.fz"

        pattern = f"LST-1.1.Run{run_number:05d}.{last_subrun:04d}*.fits.fz"
        list_of_files = sorted(date_path.glob(pattern))

        if len(list_of_files) == 0:
            log.error(f"Subrun not found for {pattern}")
            return "ERROR"

        filename = list_of_files[0]

        last_file = MultiFiles(glob(str(pattern_last_subrun)))
        first_event_last_file = next(last_file)
        ucts_available = bool(first_event_last_file.lstcam.extdevices_presence & 2)

        if ucts_available:
            if int(last_file.camera_config.lstcam.idaq_version) > 37201:
                cdts = first_event_last_file.lstcam.cdts_data.view(CDTS_AFTER_37201_DTYPE)
            else:
                cdts = first_event_last_file.lstcam.cdts_data.view(CDTS_BEFORE_37201_DTYPE)

            ucts_timestamp = np.int64(cdts["timestamp"][0])
            run_start_last_file = Time(ucts_timestamp * u.ns, format="unix")
        else:
            timestamp_last_file = end_time(
                filename, reference_time, dragon_reference_counter, module_id
            )
            run_start_last_file = Time(timestamp_last_file * u.ns, format="unix")

        elapsed_time = run_start_last_file - run_start_first_file

        return dict(
            time_start=run_start_first_file.iso,
            time_end=run_start_last_file.iso,
            elapsed=np.round(elapsed_time.to("min"), decimals=1),
        )

    except Exception as err:
        log.debug(f"Files {pattern_first_subrun} or {pattern_last_subrun} have error: {err}")

        return dict(
            time_start=None,
            time_end=None,
            elapsed=0.0,
        )


def end_time(filename, ref_time, ref_counter, ref_module_id):

    config = Config()
    config.EventTimeCalculator.dragon_reference_time = int(ref_time)
    config.EventTimeCalculator.dragon_reference_counter = int(ref_counter)
    config.EventTimeCalculator.dragon_module_id = int(ref_module_id)

    try:
        with LSTEventSource(filename, config=config, max_events=1) as source:
            source.log.setLevel(logging.ERROR)

            for event in source:
                lst_event_container = event.lst.tel[1]

            return calc_dragon_time(lst_event_container, ref_module_id, ref_time, ref_counter)

    except Exception as err:
        log.debug(f"Files {filename} have error: {err}")

        return -1


def start_end_of_run_files_stat(date_path, run_number, num_files):
    """
    Get first timestamps from the last subrun.
    Write down the reference Dragon module used, reference event_id.

    Parameters
    ----------
    date_path: pathlib.Path
        Directory that contains the R0 files
    run_number: int
        Number of the run
    num_files: int
        Number of the sequential files (subruns) of a given run

    Returns
    -------
    end_timestamp
    """

    last_subrun = num_files - 1  # first subrun is 0
    pattern_first_subrun = date_path / f"LST-1.1.Run{run_number:05d}.0000.fits.fz"
    pattern_last_subrun = date_path / f"LST-1.1.Run{run_number:05d}.{last_subrun:04d}.fits.fz"
    try:
        run_start_first_file = Time(os.path.getctime(pattern_first_subrun), format="unix")
        run_end_last_file = Time(os.path.getmtime(pattern_last_subrun), format="unix")
        elapsed_time = run_end_last_file - run_start_first_file

        return dict(
            time_start=run_start_first_file.iso,
            time_end=run_end_last_file.iso,
            elapsed=np.round(elapsed_time.to_value("min"), decimals=1),
        )

    except Exception as err:
        log.error(f"Files {pattern_first_subrun} and/or {pattern_last_subrun} have error: {err}")

        return dict(
            time_start=None,
            time_end=None,
            elapsed=0.0,
        )


def main():
    """
    Build an astropy Table with run summary information and write it
    as ECSV file with the following information (one row per run):
     - run_id
     - number of subruns
     - type of run (DRS4, CALI, DATA, CONF)
     - start of the run
     - dragon reference UCTS timestamp if available (-1 otherwise)
     - dragon reference time source ("ucts" or "run_date")
     - dragon_reference_module_id
     - dragon_reference_module_index
     - dragon_reference_counter
    """

    args = parser.parse_args()

    date_path = args.r0_path / args.date

    file_list = get_list_of_files(date_path)
    runs = get_list_of_runs(file_list)
    run_numbers, n_subruns = get_runs_and_subruns(runs)

    reference_counters = [read_counters(date_path, run) for run in run_numbers]

    run_types = [
        type_of_run(date_path, run, counters)
        for run, counters in zip(run_numbers, reference_counters)
    ]

    start_end_timestamps = [
        start_end_of_run_files_stat(date_path, run, n_files)
        for run, n_files in zip(run_numbers, n_subruns)
    ]

    run_summary = Table(
        {
            col: np.array([d[col] for d in start_end_timestamps], dtype=dtype)
            for col, dtype in dtypes.items()
        }
    )

    run_summary.add_column(run_numbers, name="run_id", index=0)
    run_summary.add_column(n_subruns, name="n_subruns", index=1)
    run_summary.add_column(run_types, name="run_type", index=2)

    run_summary["elapsed"].unit = u.min

    c = " Run summary "
    print(f"{c.center(50, '*')}")
    run_summary.pprint_all()
    print("\n")

    # Sum elapsed times:
    obs_by_type = run_summary.group_by("run_type")
    obs_by_type["number_of_runs"] = 1
    total_obs_time = obs_by_type[
        "run_type", "number_of_runs", "n_subruns", "elapsed"
    ].groups.aggregate(np.sum)
    total_obs_time["elapsed"].format = "7.1f"

    c = " Observation time per run type "
    print(f"{c.center(50, '*')}")
    total_obs_time.pprint_all()
    print("\n")

    run_summary["number_of_runs"] = 1
    total_obs = run_summary["number_of_runs", "n_subruns", "elapsed"].groups.aggregate(np.sum)
    total_obs["elapsed"].format = "7.1f"
    c = " Total observation time "
    print(f"{c.center(50, '*')}")
    total_obs.pprint_all()


if __name__ == "__main__":
    main()
