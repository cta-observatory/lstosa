"""
End-of-Night script and functions. Check that everything has been processed,
collect results and merge them if needed.
"""
import logging
import os.path
import re
import subprocess
import sys
from filecmp import cmp
from glob import glob
from os.path import basename, exists, isdir, islink, join

from osa.configs import options
from osa.configs.config import cfg
from osa.jobs.job import are_all_jobs_correctly_finished
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import get_nightsummary_file, read_nightsummary
from osa.rawcopy.raw import are_rawfiles_transferred, get_check_rawdir
from osa.reports.report import finished_assignments, finished_text, start
from osa.utils.cliopts import closercliparsing
from osa.utils.logging import MyFormatter
from osa.utils.register import register_run_concept_files
from osa.utils.standardhandle import gettag
from osa.utils.utils import (
    getlockfile,
    is_day_closed,
    is_defined,
    lstdate_to_dir,
    make_directory,
)

__all__ = [
    "use_night_summary",
    "is_raw_data_available",
    "is_sequencer_successful",
    "notify_sequencer_errors",
    "notify_neither_data_nor_reason_given",
    "ask_for_closing",
    "post_process",
    "post_process_files",
    "setclosedfilename",
    "is_finished_check",
    "extract_provenance",
    "merge_dl1datacheck",
    "set_closed_with_file"
]

log = logging.getLogger(__name__)

# Logging
fmt = MyFormatter()
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logging.root.addHandler(handler)


def main():
    """Main function in charge of closing the sequences"""

    # set the options through cli parsing
    closercliparsing()

    if options.verbose:
        logging.root.setLevel(logging.DEBUG)
    else:
        logging.root.setLevel(logging.INFO)

    # initiating report
    tag = gettag()
    start(tag)

    # starting the algorithm
    if is_day_closed():
        log.error(f"Night {options.date} already closed for {options.tel_id}")
    else:
        # proceed
        if options.seqtoclose is not None:
            log.info(f"Closing sequence {options.seqtoclose}")
        sequencer_tuple = []
        if options.reason is not None:
            log.warning("No data found")
            sequencer_tuple = [False, []]
            if is_defined(options.reason):
                # good user, proceed automatically
                pass
            else:
                # notify and ask for closing and a reason
                notify_neither_data_nor_reason_given()
                ask_for_closing()
                # FIXME: ask_for_reason is not defined anywhere
                # ask_for_reason()
            # proceed with a reason
        elif is_raw_data_available() or use_night_summary():
            # proceed normally
            log.debug(f"Checking sequencer_tuple {sequencer_tuple}")
            night_summary_output = read_nightsummary()
            sequencer_tuple = is_finished_check(night_summary_output)

            if is_sequencer_successful(sequencer_tuple):
                # close automatically
                pass
            else:
                # notify and ask for closing
                notify_sequencer_errors()
                ask_for_closing()
        else:
            log.error("Never thought about this possibility, please check the code")

        post_process(sequencer_tuple)

        # TODO: Copy datacheck to www automation (currently done by another script)
        # It is not so straightforward the usage of ssh/scp with SLURM.
        # Once cp's are open to datacheck server, this could be done.


def use_night_summary():
    """Check for the usage of night summary option and file existence."""

    answer = False
    if options.nightsummary:
        night_file = get_nightsummary_file()
        if exists(night_file):
            answer = True
        else:
            log.info("Night Summary expected but it does not exists.")
            log.info("Please check it or use the -r option to give a reason.")
            log.error("Night Summary missing and no reason option")
    return answer


def is_raw_data_available():
    """Get the rawdir and check existence.
    This means the raw directory could be empty !"""

    answer = False
    if options.tel_id != "ST":
        # FIXME: adapt this function
        raw_dir = get_check_rawdir()
        if isdir(raw_dir):
            answer = True
    else:
        answer = True
    return answer


def is_sequencer_successful(seq_tuple):
    """

    Parameters
    ----------
    seq_tuple

    Returns
    -------

    """
    # TODO: implement a more reliable non - intrusive than is_finished_check.
    answer = seq_tuple[0]
    return answer


def notify_sequencer_errors():
    """A good notification helps the user on deciding to close it or not."""

    log.info("Sequencer did not complete or finish unsuccessfully")


def ask_for_closing():
    """Ask to the user whether sequences should be closed or not.
    A True (Y/y) closes, while False(N/n) answer stops the program.

    Returns
    -------
    object
    """

    if options.noninteractive:
        # in not-interactive mode, we assume the answer is yes
        pass
    else:
        answer_check = False
        while not answer_check:
            try:
                question = "Close that day? (y/n): "
                if options.simulate:
                    question += "[SIMULATE ongoing] "
                # FIXME: figure out where raw_input comes from. I set it to answer no
                # answer_user = input(question)
                answer_user = "n"
            except KeyboardInterrupt:
                log.warning("Program quitted by user. No answer")
                sys.exit(1)
            except EOFError as ErrorValue:
                log.exception("End of file not expected", ErrorValue)
            else:
                answer_check = True
                if answer_user == "n" or answer_user == "N":
                    # the user does not want to close
                    log.info(f"Day {options.date} for {options.tel_id} will remain open")
                    sys.exit(0)
                elif answer_user == "y" or answer_user == "Y":
                    continue
                else:
                    log.warning("Answer not understood, please type y or n")
                    answer_check = False


def notify_neither_data_nor_reason_given():
    """Message informing the user of the situation."""

    log.info("There is no data and you did not enter any reason for that")


def post_process(seq_tuple):
    """Set of last instructions."""

    seq_list = seq_tuple[1]
    analysis_dict = finished_assignments(seq_list)
    analysis_text = finished_text(analysis_dict)

    # Close the sequences
    post_process_files(seq_list)

    # First merge DL1 datacheck files and produce PDFs
    merge_dl1datacheck(seq_list)

    # Extract the provenance info
    extract_provenance(seq_list)

    if options.seqtoclose is None:
        return set_closed_with_file(analysis_text)
    return False


def post_process_files(seq_list):
    """
    Identify the different types of files, try to close the sequences
    and copy output files to corresponding data directories.

    Parameters
    ----------
    seq_list: list of sequences
    """

    concept_set = []
    if options.tel_id == "LST1":
        concept_set = [
            "DL1",
            "DL2",
            "MUON",
            "DATACHECK",
            "PEDESTAL",
            "CALIB",
            "TIMECALIB",
        ]
    elif options.tel_id == "ST":
        concept_set = []

    nightdir = lstdate_to_dir(options.date)
    output_files = glob(join(options.directory, "*Run*"))
    output_files_set = set(output_files)
    for concept in concept_set:
        log.info(f"Processing {concept} files, {len(output_files_set)} files left")
        if cfg.get("LSTOSA", concept + "PREFIX"):
            pattern = cfg.get("LSTOSA", concept + "PREFIX")
        else:
            pattern = cfg.get("LSTOSA", concept + "PATTERN")

        # Create final destination directory for each data level
        if concept in ["DL1", "MUON", "DATACHECK"]:
            dir = join(cfg.get(options.tel_id, concept + "DIR"), nightdir, options.dl1_prod_id)
        elif concept == "DL2":
            dir = join(cfg.get(options.tel_id, concept + "DIR"), nightdir, options.dl2_prod_id)
        elif concept in ["PEDESTAL", "CALIB", "TIMECALIB"]:
            dir = join(cfg.get(options.tel_id, concept + "DIR"), nightdir, options.calib_prod_id)

        delete_set = set()
        log.debug(f"Checking if {concept} files need to be moved to {dir}")
        for file in output_files_set:
            file_basename = basename(file)
            pattern_found = re.search(f"^{pattern}", file_basename)
            if options.seqtoclose is not None:
                seqtoclose_found = re.search(options.seqtoclose, file_basename)
                if seqtoclose_found is None:
                    pattern_found = None
            if pattern_found is not None:
                new_dst = join(dir, file_basename)
                if not options.simulate:
                    make_directory(dir)
                    if exists(new_dst):
                        if islink(file):
                            # delete because the link has been correctly copied
                            log.debug(f"Original file {file} is just a link")
                            if options.seqtoclose is None:
                                log.debug(f"Deleting {file}")
                                # unlink(file)
                        elif exists(file) and cmp(file, new_dst):
                            # delete
                            log.debug(f"Destination file exists and it is equal to {file}")
                            if options.seqtoclose is None:
                                log.debug(f"Deleting {file}")
                                # unlink(file)
                        else:
                            log.warning(
                                f"Original file {file} is not a link or is different than destination {new_dst}"
                            )
                    else:
                        log.debug(f"Destination file {new_dst} does not exists")
                        for s in seq_list:
                            log.debug(f"Looking for {s}")

                            if s.type == "DATA":
                                run_str_found = re.search(s.run_str, file_basename)

                                if run_str_found is not None:
                                    # register and delete
                                    log.debug(f"Registering file {run_str_found}")
                                    register_run_concept_files(s.run_str, concept)
                                    if options.seqtoclose is None and not exists(file):
                                        log.debug("File does not exists")

                            elif s.type in ["CALI", "DRS4"]:
                                calib_run_str_found = re.search(str(s.run), file_basename)
                                drs4_run_str_found = re.search(str(s.previousrun), file_basename)

                                if calib_run_str_found is not None:
                                    # register and delete
                                    log.debug(f"Registering file {calib_run_str_found}")
                                    register_run_concept_files(str(s.run), concept)
                                    if options.seqtoclose is None and not exists(file):
                                        log.debug("File does not exists")

                                if drs4_run_str_found is not None:
                                    # register and delete
                                    log.debug(f"Registering file {drs4_run_str_found}")
                                    register_run_concept_files(str(s.previousrun), concept)
                                    if options.seqtoclose is None and not exists(file):
                                        log.debug("File does not exists")

                # FIXME: for the moment we do not want to close
                # setclosedfilename(s)
                # createclosed(s.closed)
                delete_set.add(file)
        output_files_set -= delete_set


def set_closed_with_file(ana_text):
    """Write the analysis report to the closer file."""

    closer_file = getlockfile()
    is_closed = False
    if not options.simulate:
        pass
        # For the moment we do not generate NightFinished lock file
        # is_closed = createlock(closer_file, ana_text)
    else:
        log.info(f"SIMULATE Creation of lock file {closer_file}")

    # the close file will be send to a remote monitor server
    if is_closed:
        # synchronize_remote(closer_file)
        # FIXME: do we have to sync?
        pass

    return is_closed


def is_finished_check(nightsum):
    """

    Parameters
    ----------
    nightsum

    Returns
    -------

    """
    # we ought to implement a method of successful or unsuccessful finishing
    # and it is done looking at the files
    sequence_success = False
    if nightsum == "":
        # empty file (no sensible data)
        sequence_success = True
        sequence_list = []
    else:
        # building the sequences (the same way than the sequencer)
        subrun_list = extractsubruns(nightsum)
        run_list = extractruns(subrun_list)
        sequence_list = extractsequences(run_list)
        # adds the scripts to sequences
        # job.preparejobs(sequence_list, run_list, subrun_list)
        # FIXME: How can we check that all files are there?
        if are_rawfiles_transferred():
            log.debug(f"Are files transferred? {sequence_list}")
            if are_all_jobs_correctly_finished(sequence_list):
                sequence_success = True
            else:
                log.info(
                    "All raw files are transferred but the jobs did not correctly/yet finish",
                )
        else:
            log.info("More raw files are expected to appear")
    return [sequence_success, sequence_list]


# def synchronize_remote(lockfile):
#     from os.path import join
#     import subprocess
#     from osa.configs.config import cfg
#     user = cfg.get('REMOTE', 'STATISTICSUSER')
#     host = cfg.get('REMOTE', 'STATISTICSHOST')
#     remotedirectory = join(cfg.get('REMOTE', 'STATISTICSDIR'), options.tel_id)
#     remotebasename = options.date + cfg.get('REMOTE', 'STATISTICSSUFFIX')
#     remotepath = join(remotedirectory, remotebasename)
#     log.info(
#         f"Synchronizing {options.tel_id} {options.date} by copying lock file to {user}@{host}:{remotepath}"
#     )
#     commandargs = [
#         'scp', '-P', cfg.get('REMOTE', 'STATISTICSSSHPORT'),
#         lockfile, user + '@' + host + ':' + join(remotedirectory, remotebasename)
#     ]
#     try:
#         subprocess.call(commandargs)
#     # except OSError as (ValueError, NameError):
#     except OSError:
#         log.warning(
#             f"Could not copy securely with command: {stringify(commandargs)}, {OSError}"
#         )


def setclosedfilename(seq):
    """
    Close sequence and creates a .closed file

    Parameters
    ----------
    seq: Sequence Object
    """
    closed_suffix = cfg.get("LSTOSA", "CLOSEDSUFFIX")
    basename = f"sequence_{seq.jobname}"
    seq.closed = os.path.join(options.directory, basename + closed_suffix)


def merge_dl1datacheck(seq_list):
    """
    Merge every DL1 datacheck h5 files run-wise and generate the PDF files

    Parameters
    ----------
    seq_list: list of sequence objects
        List of Sequence Objects
    """

    log.debug("Merging dl1 datacheck files and producing PDFs")
    nightdir = lstdate_to_dir(options.date)
    dl1_directory = join(cfg.get("LST1", "DL1DIR"), nightdir, options.prod_id)

    for sequence in seq_list:
        if sequence.type == "DATA":
            cmd = [
                "sbatch",
                "--parsable",
                "-D",
                options.directory,
                "-o",
                f"log/slurm_mergedl1datacheck_{str(sequence.run).zfill(5)}_%j.out",
                "-e",
                f"log/slurm_mergedl1datacheck_{str(sequence.run).zfill(5)}_%j.err",
                "lstchain_check_dl1",
                f"--input-file={dl1_directory}/datacheck_dl1_LST-1.Run0{sequence.run}.*.h5",
                f"--output-dir={dl1_directory}",
            ]
            if not options.simulate:
                try:
                    subprocess.run(cmd, stdout=subprocess.PIPE, universal_newlines=True)
                except subprocess.CalledProcessError as err:
                    log.exception(f"Not able to merge DL1 datacheck: {err}")
                # TODO implement an automatic scp to www datacheck,
                # right after the production of the PDF files
            else:
                log.debug("Simulate launching scripts")
            log.debug(cmd)


def extract_provenance(seq_list):
    """
    Extract provenance run-wise from the prov.log file

    Parameters
    ----------
    seq_list: list of sequence objects
        List of Sequence Objects
    """
    log.debug("Extract provenance run-wise")

    nightdir = lstdate_to_dir(options.date)

    for sequence in seq_list:
        if sequence.type == "DATA":
            cmd = [
                "sbatch",
                "-D",
                options.directory,
                "-o",
                f"log/slurm_provenance_{str(sequence.run).zfill(5)}_%j.log",
                f"{cfg.get('LSTOSA', 'SCRIPTSDIR')}/provprocess.py",
                "-c",
                options.configfile,
                f"{str(sequence.run).zfill(5)}",
                nightdir,
                options.prod_id,
            ]
            if not options.simulate:
                subprocess.run(cmd, stdout=subprocess.PIPE, universal_newlines=True)
            else:
                log.debug("Simulate launching scripts")
            log.debug(cmd)


if __name__ == "__main__":
    main()
