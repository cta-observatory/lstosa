"""
End-of-Night script and functions. Check that everthing has been processed
Collect results, merge them if needed
"""
import os.path
import re
import sys
from filecmp import cmp
from glob import glob
from os import unlink
from os.path import basename, exists, isdir, islink, join
from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import closercliparsing
from osa.utils.standardhandle import error, gettag, output, verbose, warning
from osa.jobs.job import arealljobscorrectlyfinished
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import getnightsummaryfile, readnightsummary
from osa.rawcopy.raw import arerawfilestransferred, get_check_rawdir
from osa.reports.report import finished_assignments, finished_text, start
from osa.utils.utils import createlock, getlockfile, is_defined, lstdate_to_dir, make_directory, is_day_closed
from osa.veto.veto import createclosed
from osa.utils.register import register_run_concept_files


def closer():
    """Main function in charge of closing the sequences"""

    # initiating report
    start(tag)

    # starting the algorithm
    if is_day_closed():
        # exit
        error(tag, f"Night {options.date} already closed for {options.tel_id}", 1)
    else:
        # proceed
        if options.seqtoclose is not None:
            output(tag, f"Closing sequence {options.seqtoclose}")
        sequencer_tuple = []
        if options.reason is not None:
            # no data
            warning(tag, "No data found")
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
            verbose(tag, f"Checking sequencer_tuple {sequencer_tuple}")
            night_summary_output = readnightsummary()
            sequencer_tuple = is_finished_check(night_summary_output)

            if is_sequencer_successful(sequencer_tuple):
                # close automatically
                pass
            else:
                # notify and ask for closing
                notify_sequencer_errors()
                ask_for_closing()
        else:
            error(tag, "Never thought about this possibility, please check the code", 9)

        post_process(sequencer_tuple)


def use_night_summary():
    """Check for the usage of night summary option and file existence."""

    answer = False
    if options.nightsum:
        night_file = getnightsummaryfile()
        if exists(night_file):
            answer = True
        else:
            output(tag, "Night Summary expected but it does not exists.")
            output(tag, "Please check it or use the -r option to give a reason.")
            error(tag, "Night Summary missing and no reason option", 2)
    return answer


def is_raw_data_available():
    """Get the rawdir and check existence.
    This means the raw directory could be empty !"""

    answer = False
    if options.tel_id != "ST":
        raw_dir = get_check_rawdir()
        if isdir(raw_dir):
            answer = True
    else:
        answer = True
    return answer


def is_sequencer_successful(seq_tuple):

    # TODO: implement a more reliable non - intrusive than is_finished_check.
    answer = seq_tuple[0]
    return answer


def notify_sequencer_errors():
    """A good notification helps the user on deciding to close it or not."""

    output(tag, "Sequencer did not complete or finish unsuccessfully")
    pass


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
                warning(tag, "Program quitted by user. No answer")
                sys.exit(1)
            except EOFError as ErrorValue:
                error(tag, "End of file not expected", ErrorValue)
            else:
                answer_check = True
                if answer_user == "n" or answer_user == "N":
                    # the user does not want to close
                    output(tag, f"Day {options.date} for {options.tel_id} will remain open")
                    sys.exit(0)
                elif answer_user == "y" or answer_user == "Y":
                    continue
                else:
                    warning(tag, "Answer not understood, please type y or n")
                    answer_check = False


def notify_neither_data_nor_reason_given():
    """Message informing the user of the situation."""

    output(tag, "There is no data and you did not enter any reason for that")


def post_process(seq_tuple):
    """Set of last instructions."""

    seq_list = seq_tuple[1]
    analysis_dict = finished_assignments(seq_list)
    analysis_text = finished_text(analysis_dict)
    post_process_files(seq_list)
    if options.seqtoclose is None:
        is_closed = set_closed_with_file(analysis_text)
        return is_closed
    return False


def post_process_files(seq_list):
    """Identify the different types of files, try to close the sequences
    and copy output files to corresponding data directories.

    Parameters
    ----------
    seq_list: list of sequences
    """

    concept_set = []
    if options.tel_id == "LST1" or options.tel_id == "LST2":
        concept_set = ["DL1", "DL2", "MUON", "DATACHECK"]
    elif options.tel_id == "ST":
        concept_set = []

    nightdir = lstdate_to_dir(options.date)
    output_files = glob(join(options.directory, "*LST-1.*"))
    output_files_set = set(output_files)
    for concept in concept_set:
        output(tag, f"Processing {concept} files, {len(output_files_set)} files left")
        if cfg.get("LSTOSA", concept + "PREFIX"):
            pattern = cfg.get("LSTOSA", concept + "PREFIX")
        else:
            pattern = cfg.get("LSTOSA", concept + "PATTERN")

        dir = join(cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id)
        delete_set = set()
        verbose(tag, f"Checking if {concept} files need to be moved to {dir}")
        for r in output_files_set:
            r_basename = basename(r)
            pattern_found = re.search(f"^{pattern}", r_basename)
            verbose(tag, f"Was pattern {pattern} found in {r_basename} ?: {pattern_found}")
            if options.seqtoclose is not None:
                seqtoclose_found = re.search(options.seqtoclose, r_basename)
                verbose(tag, f"Was pattern {options.seqtoclose} found in {r_basename} ?: {seqtoclose_found}")
                if seqtoclose_found is None:
                    pattern_found = None
            if pattern_found is not None:
                new_dst = join(dir, r_basename)
                if not options.simulate:
                    make_directory(dir)
                    if exists(new_dst):
                        if islink(r):
                            # delete because the link has been correctly copied
                            verbose(tag, f"Original file {r} is just a link")
                            if options.seqtoclose is None:
                                verbose(tag, f"Deleting {r}")
                                unlink(r)
                        elif exists(r) and cmp(r, new_dst):
                            # delete
                            verbose(tag, f"Destination file exists and it is equal to {r}")
                            if options.seqtoclose is None:
                                verbose(tag, f"Deleting {r}")
                                unlink(r)
                        else:
                            warning(tag, f"Original file {r} is not a link or is different than destination {new_dst}")
                    else:
                        verbose(tag, f"Destination file {new_dst} does not exists")
                        for s in seq_list:
                            verbose(tag, "Looking for {s}")
                            run_str_found = re.search(s.run_str, r_basename)
                            if run_str_found is not None:
                                # register and delete
                                verbose(tag, f"Registering file {run_str_found}")
                                register_run_concept_files(s.run_str, concept)
                                if options.seqtoclose is None:
                                    if exists(r):
                                        unlink(r)
                                    else:
                                        verbose(tag, "File does not exists")

                                setclosedfilename(s)
                                createclosed(s.closed)
                                break
                delete_set.add(r)
        output_files_set -= delete_set


def set_closed_with_file(ana_text):
    """Write the analysis report to the closer file."""

    closer_file = getlockfile()
    is_closed = False
    if not options.simulate:
        is_closed = createlock(closer_file, ana_text)
    else:
        output(tag, f"SIMULATE Creation of lock file {closer_file}")

    # the close file will be send to a remote monitor server
    if is_closed:
        # synchronize_remote(closer_file)
        # FIXME: do we have to sync?
        pass

    return is_closed


def is_finished_check(nightsum):

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
        if arerawfilestransferred():
            verbose(tag, f"Are files transferred? {sequence_list}")
            if arealljobscorrectlyfinished(sequence_list):
                sequence_success = True
            else:
                output(tag, "All raw files are transferred but the jobs did not correctly/yet finish")
        else:
            output(tag, "More raw files are expected to appear")
    return [sequence_success, sequence_list]


# def synchronize_remote(lockfile):
#     tag = gettag()
#     from os.path import join
#     import subprocess
#     from osa.configs.config import cfg
#     user = cfg.get('REMOTE', 'STATISTICSUSER')
#     host = cfg.get('REMOTE', 'STATISTICSHOST')
#     remotedirectory = join(cfg.get('REMOTE', 'STATISTICSDIR'), options.tel_id)
#     remotebasename = options.date + cfg.get('REMOTE', 'STATISTICSSUFFIX')
#     remotepath = join(remotedirectory, remotebasename)
#     output(
#         tag,
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
#         warning(
#             tag,
#             f"Could not copy securely with command: {stringify(commandargs)}, {OSError}"
#         )


def setclosedfilename(s):

    closed_suffix = cfg.get("LSTOSA", "CLOSEDSUFFIX")
    basename = f"sequence_{s.jobname}"
    s.closed = os.path.join(options.directory, basename + closed_suffix)


if __name__ == "__main__":

    tag = gettag()
    # set the options through cli parsing
    closercliparsing()
    # run the routine
    closer()
