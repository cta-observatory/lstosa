"""
End-of-night script and functions. Check that everything has been processed,
collect results and merge them if needed.
"""

import logging
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Iterable, List

from osa import osadb
from osa.configs import options
from osa.configs.config import cfg
from osa.job import (
    are_all_jobs_correctly_finished, 
    save_job_information, 
    run_sacct, 
    get_closer_sacct_output
)
from osa.nightsummary.extract import extract_runs, extract_sequences
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import (
    destination_dir,
    create_longterm_symlink,
    dl1_datacheck_longterm_file_exits
)
from osa.raw import is_raw_data_available
from osa.report import start
from osa.utils.cliopts import closercliparsing
from osa.utils.logging import myLogger
from osa.utils.register import register_found_pattern
from osa.utils.mail import send_warning_mail
from osa.utils.utils import (
    night_finished_flag,
    is_day_closed,
    stringify,
    date_to_dir,
    create_lock,
    gettag,
    date_to_iso,
)

__all__ = [
    "is_sequencer_successful",
    "ask_for_closing",
    "post_process",
    "post_process_files",
    "is_finished_check",
    "extract_provenance",
    "merge_dl1_datacheck",
    "set_closed_with_file",
    "merge_files",
    "daily_datacheck",
    "daily_longterm_cmd",
    "observation_finished",
]

log = myLogger(logging.getLogger())


def main():
    """Main function in charge of closing the sequences."""
    closercliparsing()

    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    if options.simulate:
        log.info("Running in simulation mode.")

    # initiating report
    tag = gettag()
    start(tag)

    # starting the algorithm
    if not observation_finished():
        log.warning("Observations not over, it is earlier than 08:00 UTC.")
        sys.exit(0)

    elif is_day_closed():
        log.info(f"Date {date_to_iso(options.date)} already closed for {options.tel_id}")
        sys.exit(0)
    else:
        if options.seqtoclose is not None:
            log.info(f"Closing sequence {options.seqtoclose}")

        sequencer_tuple = [False, []]

        if is_raw_data_available(options.date):
            # proceed normally
            log.debug(f"Checking sequencer_tuple {sequencer_tuple}")
            night_summary_table = run_summary_table(options.date)
            sequencer_tuple = is_finished_check(night_summary_table)

            if not is_sequencer_successful(sequencer_tuple):
                log.info("Sequencer did not complete or finish unsuccessfully")
                ask_for_closing()
        else:
            log.error("Never thought about this possibility, please check the code")
            sys.exit(-1)

        save_job_information()
        post_process(sequencer_tuple)


def is_sequencer_successful(seq_tuple: Tuple[bool, Iterable]):
    """Return a bool assessing whether the sequencer has successfully finished or not."""
    return seq_tuple[0]


def ask_for_closing():
    """
    Ask the user whether sequences should be closed or not.
    A True (Y/y) closes, while False(N/n) answer stops the program.

    Returns
    -------
    object
    """

    if options.noninteractive:
        return

    answer_check = False

    while not answer_check:
        try:
            answer_user = "n"
        except KeyboardInterrupt:
            log.warning("Program exited by user.")
            sys.exit(1)
        except EOFError as error:
            log.exception(f"End of file not expected, {error}")
            sys.exit(2)
        else:
            answer_check = True
            if answer_user in {"n", "N"}:
                # the user does not want to close
                log.info(
                    f"Day {options.date} for {options.tel_id} will "
                    f"remain open unless closing is forced"
                )
                sys.exit(0)
            elif answer_user in {"y", "Y"}:
                continue
            else:
                log.warning("Answer not understood, please type y or n")
                answer_check = False


def post_process(seq_tuple):
    """Set of last instructions."""
    seq_list = seq_tuple[1]
    
    if dl1_datacheck_longterm_file_exits() and not options.test:
        create_longterm_symlink()

    else:
        # Close the sequences
        post_process_files(seq_list)

        # Extract the provenance info
        extract_provenance(seq_list)

        # Merge DL1b files run-wise
        merge_files(seq_list, data_level="DL1AB")

        merge_muon_files(seq_list)

        # Merge DL2 files run-wise
        if not options.no_dl2:
            merge_files(seq_list, data_level="DL2")

        # Merge DL1 datacheck files and produce PDFs. It also produces
        # the daily datacheck report using the longterm script, and updates
        # the longterm DL1 datacheck file with the cherenkov_transparency script.
        if cfg.getboolean("lstchain", "merge_dl1_datacheck"):
            list_job_id = merge_dl1_datacheck(seq_list)
            longterm_job_id = daily_datacheck(daily_longterm_cmd(list_job_id))
            cherenkov_job_id = cherenkov_transparency(cherenkov_transparency_cmd(longterm_job_id))
            create_longterm_symlink(cherenkov_job_id)

        time.sleep(600)

    # Check if all jobs launched by autocloser finished correctly 
    # before creating the NightFinished.txt file
    n_max = 6
    n = 0
    while not all_closer_jobs_finished_correctly() and n <= n_max:
        log.info(
            "All jobs launched by autocloser did not finished correctly yet. "
            "Checking again in 10 minutes..."
        )
        time.sleep(600)
        n += 1

    if n > n_max:
        send_warning_mail(date=date_to_iso(options.date))
        return False

    if options.seqtoclose is None:
        database = cfg.get("database", "path")
        if database:
            osadb.end_processing(date_to_iso(options.date))
        # Creating closing flag files will be deprecated in future versions
        return set_closed_with_file()

    return False


def post_process_files(seq_list: list):
    """
    Identify the different types of files, try to close the sequences
    and copy output files to corresponding data directories.

    Parameters
    ----------
    seq_list: list
        list of sequences
    """

    output_files_set = set(Path(options.directory).rglob("*Run*"))

    DL1AB_RE = re.compile(rf"{options.dl1_prod_id}/dl1.*.(?:h5|hdf5|hdf)")
    MUONS_RE = re.compile(r"muons.*.fits")
    DATACHECK_RE = re.compile(r"datacheck_dl1.*.(?:h5|hdf5|hdf)")
    INTERLEAVED_RE = re.compile(r"interleaved.*.(?:h5|hdf5|hdf)")

    pattern_files = dict(
        [
            ("DL1AB", DL1AB_RE),
            ("MUON", MUONS_RE),
            ("DATACHECK", DATACHECK_RE),
            ("INTERLEAVED", INTERLEAVED_RE),
        ]
    )

    if not options.no_dl2:
        DL2_RE = re.compile(f"{options.dl2_prod_id}/dl2.*.(?:h5|hdf5|hdf)")
        pattern_files["DL2"] = DL2_RE

    for concept, pattern_re in pattern_files.items():
        log.info(f"Post processing {concept} files, {len(output_files_set)} files left")

        dst_path = destination_dir(concept, create_dir=True)

        log.debug(f"Checking if {concept} files need to be moved to {dst_path}")

        for file_path in output_files_set.copy():

            file = str(file_path)
            # If seqtoclose is set, we only want to close that sequence
            if options.seqtoclose is not None and options.seqtoclose not in file:
                continue

            if pattern_found := pattern_re.search(file):
                log.debug(f"Pattern {concept} found, {pattern_found} in {file}")
                registered_file = register_found_pattern(file_path, seq_list, concept, dst_path)
                output_files_set.remove(registered_file)


def set_closed_with_file():
    """Write the analysis report to the closer file."""
    night_finished_file = night_finished_flag()
    is_closed = False
    if not options.simulate:
        # Generate NightFinished lock file
        is_closed = create_lock(night_finished_file)
    else:
        log.debug(f"Simulate the creation of lock file {night_finished_file}")

    return is_closed


def observation_finished(date=datetime.utcnow()) -> bool:
    """
    We consider the observation as finished if it is later
    than 08:00 UTC of the next day set by `options.date`
    """
    next_morning_limit = options.date + timedelta(days=1, hours=8)
    return date > next_morning_limit


def is_finished_check(run_summary):
    """
    Check that all sequences are finished.

    Parameters
    ----------
    run_summary: astropy.Table
        Table containing the run information from a given date.

    Returns
    -------
    seq_finished: bool
        True if all sequences are finished, False otherwise.
    seq_list: list
    """

    sequence_success = False
    if run_summary is not None:
        # building the sequences (the same way as the sequencer)
        run_list = extract_runs(run_summary)
        sequence_list = extract_sequences(options.date, run_list)

        if are_all_jobs_correctly_finished(sequence_list):
            sequence_success = True
        else:
            log.info("Jobs did not correctly/yet finish")

    else:
        # empty file (no sensible data)
        sequence_success = True
        sequence_list = []

    return [sequence_success, sequence_list]


def merge_dl1_datacheck(seq_list) -> List[str]:
    """
    Merge every DL1 datacheck h5 files run-wise and generate the PDF files

    Parameters
    ----------
    seq_list: list of sequence objects
        List of Sequence Objects
    """
    log.debug("Merging dl1 datacheck files and producing PDFs")

    muons_dir = destination_dir("MUON", create_dir=False)
    datacheck_dir = destination_dir("DATACHECK", create_dir=False)

    list_job_id = []

    for sequence in seq_list:
        if sequence.type == "DATA":
            cmd = [
                "sbatch",
                "--parsable",
                "-D",
                options.directory,
                "-o",
                f"log/merge_dl1_datacheck_{sequence.run:05d}_%j.out",
                "-e",
                f"log/merge_dl1_datacheck_{sequence.run:05d}_%j.err",
                "lstchain_check_dl1",
                "--input-file",
                f"{datacheck_dir}/datacheck_dl1_LST-1.Run{sequence.run:05d}.*.h5",
                f"--output-dir={datacheck_dir}",
                f"--muons-dir={muons_dir}",
            ]
            if not options.simulate and not options.test:
                job = subprocess.run(
                    cmd,
                    encoding="utf-8",
                    capture_output=True,
                    text=True,
                    check=True,
                )
                list_job_id.append(job.stdout.strip())
            else:
                log.debug("Simulate launching scripts")

            log.debug(f"Executing {stringify(cmd)}")

    return list_job_id


def extract_provenance(seq_list):
    """
    Extract provenance run wise from the prov.log file
    where it was stored sub-run wise

    Parameters
    ----------
    seq_list: list of sequence objects
        List of Sequence Objects
    """
    log.info("Extract provenance run wise")

    nightdir = date_to_dir(options.date)

    for sequence in seq_list:
        if sequence.type == "DATA":
            drs4_pedestal_run_id = str(sequence.drs4_run)
            pedcal_run_id = str(sequence.pedcal_run)
            cmd = [
                "sbatch",
                "-D",
                options.directory,
                "-o",
                f"log/provenance_{sequence.run:05d}_%j.log",
                "provprocess",
                "-c",
                options.configfile,
                drs4_pedestal_run_id,
                pedcal_run_id,
                f"{sequence.run:05d}",
                nightdir,
                options.prod_id,
            ]
            if options.no_dl2:
                cmd.append("--no-dl2")
                
            if not options.simulate and not options.test and shutil.which("sbatch") is not None:
                subprocess.run(cmd, check=True)
            else:
                log.debug("Simulate launching scripts")


def get_pattern(data_level) -> Tuple[str, str]:
    """Return the subrun wise file pattern for the data level."""
    if data_level == "DL1AB":
        return "dl1_LST-1.Run?????.????.h5", "dl1"
    if data_level == "MUON":
        return "muons_LST-1.Run?????.????.fits", "muon"
    if data_level == "DL2":
        return "dl2_LST-1.Run?????.????.h5", "dl2"

    raise ValueError(f"Unknown data level {data_level}")


def merge_files(sequence_list, data_level="DL2"):
    """Merge DL1b or DL2 h5 files run-wise."""
    log.info(f"Looping over the sequences and merging the {data_level} files")

    data_dir = destination_dir(data_level, create_dir=False)
    pattern, prefix = get_pattern(data_level)

    for sequence in sequence_list:
        if sequence.type == "DATA":
            merged_file = Path(data_dir) / f"{prefix}_LST-1.Run{sequence.run:05d}.h5"

            cmd = [
                "sbatch",
                "-D",
                options.directory,
                "-o",
                f"log/merge_{prefix}_{sequence.run:05d}_%j.log",
                "lstchain_merge_hdf5_files",
                f"--input-dir={data_dir}",
                f"--output-file={merged_file}",
                "--no-image",
                "--no-progress",
                f"--run-number={sequence.run}",
                f"--pattern={pattern}",
            ]

            log.debug(f"Executing {stringify(cmd)}")

            if not options.simulate and not options.test and shutil.which("sbatch") is not None:
                subprocess.run(cmd, check=True)
            else:
                log.debug("Simulate launching scripts")


def merge_muon_files(sequence_list):
    """Merge muon files run-wise."""
    log.info("Looping over the sequences and merging the MUON files")

    data_dir = destination_dir("MUON", create_dir=False)
    pattern, prefix = get_pattern("MUON")

    for sequence in sequence_list:
        merged_file = Path(data_dir) / f"muons_LST-1.Run{sequence.run:05d}.fits"

        cmd = [
            "sbatch",
            "-D",
            options.directory,
            "-o",
            f"log/merge_{prefix}_{sequence.run:05d}_%j.log",
            "lstchain_merge_muon_files",
            f"--input-dir={data_dir}",
            f"--output-file={merged_file}",
            f"--run-number={sequence.run}",
            f"--pattern={pattern}",
        ]

        log.debug(f"Executing {stringify(cmd)}")

        if not options.simulate and not options.test and shutil.which("sbatch") is not None:
            subprocess.run(cmd, check=True)
        else:
            log.debug("Simulate launching scripts")


def daily_longterm_cmd(parent_job_ids: List[str]) -> List[str]:
    """Build the daily longterm command."""
    nightdir = date_to_dir(options.date)
    datacheck_dir = destination_dir("DATACHECK", create_dir=False)
    muons_dir = destination_dir("MUON", create_dir=False)
    longterm_dir = Path(cfg.get("LST1", "LONGTERM_DIR")) / options.prod_id / nightdir
    longterm_output_file = longterm_dir / f"DL1_datacheck_{nightdir}.h5"

    return [
        "sbatch",
        "--parsable",
        "-D",
        options.directory,
        "-o",
        "log/longterm_daily_%j.log",
        f"--dependency=afterok:{','.join(parent_job_ids)}",
        "lstchain_longterm_dl1_check",
        f"--input-dir={datacheck_dir}",
        f"--output-file={longterm_output_file}",
        f"--muons-dir={muons_dir}",
        "--batch",
    ]


def daily_datacheck(cmd: List[str]):
    """Run daily dl1 checks using longterm script."""
    log.info("Daily dl1 checks using longterm script.")
    log.debug(f"Executing {stringify(cmd)}")

    if not options.simulate and not options.test and shutil.which("sbatch") is not None:
        job = subprocess.run(
            cmd,
            encoding="utf-8",
            capture_output=True,
            text=True,
            check=True,
        )
        job_id = job.stdout.strip()
        return job_id
    else:
        log.debug("Simulate launching scripts")


def cherenkov_transparency_cmd(longterm_job_id: str) -> List[str]:
    """Build the cherenkov transparency command."""
    nightdir = date_to_dir(options.date)
    datacheck_dir = destination_dir("DATACHECK", create_dir=False)
    longterm_dir = Path(cfg.get("LST1", "LONGTERM_DIR")) / options.prod_id / nightdir
    longterm_datacheck_file = longterm_dir / f"DL1_datacheck_{nightdir}.h5"

    return [
        "sbatch",
        "--parsable",
        "-D",
        options.directory,
        "-o",
        "log/cherenkov_transparency_%j.log",
        f"--dependency=afterok:{longterm_job_id}",
        "lstchain_cherenkov_transparency",
        f"--update-datacheck-file={longterm_datacheck_file}",
        f"--input-dir={datacheck_dir}",
    ]


def cherenkov_transparency(cmd: List[str]):
    """Update longterm dl1 check file with cherenkov transparency information."""
    log.info("Update longterm dl1 check file with cherenkov_transparency script.")
    log.debug(f"Executing {stringify(cmd)}")

    if not options.simulate and not options.test and shutil.which("sbatch") is not None:
        job = subprocess.run(
            cmd,
            encoding="utf-8",
            capture_output=True,
            text=True,
            check=True,
        )
        job_id = job.stdout.strip()
        return job_id

    else:
        log.debug("Simulate launching scripts")


def all_closer_jobs_finished_correctly():
    """Check if all the jobs launched by autocloser finished correctly."""
    sacct_output = run_sacct()
    jobs_closer = get_closer_sacct_output(sacct_output)
    if len(jobs_closer[jobs_closer["State"]!="COMPLETED"])==0:
        return True
    else:
        return False


if __name__ == "__main__":
    main()
