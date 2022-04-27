"""
Script to handle the automatic closing of the OSA
checking that all jobs are correctly finished.
"""

import argparse
import datetime
import logging
import os
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.cliopts import get_prod_id
from osa.utils.cliopts import set_default_directory_if_needed, valid_date
from osa.utils.logging import myLogger

__all__ = ["Telescope", "Sequence"]

log = myLogger(logging.getLogger())


# settings / global variables
def argument_parser():
    parser = argparse.ArgumentParser(
        description="This script is an automatic error handler and closer for lstosa."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Turn on verbose mode"
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test mode with example sequences, only works locally",
    )
    parser.add_argument(
        "-s",
        "--simulate",
        action="store_true",
        help="Create nothing, only simulate closer (safe mode)",
    )
    parser.add_argument(
        "--ignorecronlock", action="store_true", help='Ignore "cron.lock"'
    )
    parser.add_argument(
        "-i",
        "--onlyIncidences",
        action="store_true",
        help="Writing down only incidences, not closing",
    )
    parser.add_argument("-d", "--date", help="Date - format YYYY_MM_DD", type=valid_date)
    parser.add_argument(
        "-f", "--force", action="store_true", help="Force the autocloser to close the day"
    )
    parser.add_argument(
        "-e",
        "--equal",
        action="store_true",
        help="Skip check for equal amount of sequences",
    )
    parser.add_argument(
        "-w",
        "--woIncidences",
        action="store_true",
        help="Close without writing down incidences.",
    )
    parser.add_argument(
        "-r", "--runwise", action="store_true", help="Close the day run-wise."
    )
    parser.add_argument(
        "-c",
        "--config-file",
        dest="osa_config_file",
        default="cfg/sequencer.cfg",
        help="OSA config file.",
    )
    parser.add_argument("-l", "--log", default="", help="Write log to a file.")
    parser.add_argument("tel", nargs="*", choices=["LST1"])
    return parser


def analysis_path(tel):
    options.tel_id = tel
    options.date = f"{year:04}_{month:02}_{day:02}"
    return set_default_directory_if_needed()


def closedFlag(tel):
    if args.test:
        return f"./{tel}/NightFinished.txt"
    basename = cfg.get("LSTOSA", "end_of_activity")
    return Path(cfg.get(tel, "CLOSER_DIR")) / nightdir / prod_id / basename


def exampleSeq():
    return "./extra/example_sequencer.txt"


def cronLock(tel):
    return f"{analysis_path(tel)}/cron.lock"


def incidencesFile(tel):
    return f"{analysis_path(tel)}/Incidences.txt"


def incidencesFileTmp(tel):
    return f"{analysis_path(tel)}/AutoCloser_Incidences_tmp.txt"


class Telescope(object):
    """

    Parameters
    ----------
    telescope : str
        Options: LST1, LST2 or ST

    Attributes
    ----------
    sequences: list of autocloser.Sequence
        Holds a Sequence object for each Sequence/Run belonging to the
        telescope.
    """

    def __init__(self, telescope):
        self.telescope = telescope
        # necessary to make sure that cron.lock gets deleted in the end
        self.cl = cronLock(self.telescope)
        self.locked = False
        self.closed = False
        self.header_lines = []
        self.data_lines = []
        self.sequences = []

        if self.is_closed():
            log.info(f"{self.telescope} is already closed! Ignoring {self.telescope}")
            return
        if not os.path.exists(analysis_path(self.telescope)):
            log.warning(
                f"'Analysis' folder does not exist for {self.telescope}! "
                f"Ignoring {self.telescope}"
            )
            return
        if not self.lockAutomaticSequencer() and not args.ignorecronlock:
            log.warning(f"{self.telescope} already locked! Ignoring {self.telescope}")
            return
        if not self.simulate_sequencer():
            log.warning(
                f"Simulation of the sequencer failed "
                f"for {self.telescope}! Ignoring {self.telescope}"
            )
            return

        self.parse_sequencer()

        if not self.build_Sequences():
            log.warning(
                f"Sequencer for {self.telescope} is empty! Ignoring {self.telescope}"
            )
            return
        self.incidence = Incidence(self.telescope)

    def __iter__(self):
        return iter(self.sequences)

    def __del__(self, log=log, os=os):
        if self.locked:
            log.debug(f"Deleting {self.cl}")
            os.remove(self.cl)

    def is_closed(self):
        log.debug(f"Checking if {self.telescope} is closed")
        if os.path.isfile(closedFlag(self.telescope)):
            self.closed = True
            return True
        return False

    def lockAutomaticSequencer(self):
        if os.path.isfile(cronLock(self.telescope)):
            return False
        log.debug(f"Creating {cronLock(self.telescope)}")
        open(cronLock(self.telescope), "a").close()
        self.locked = True
        return True

    def is_transferred(self):
        log.debug(f"Checking if raw data is completely transferred for {self.telescope}")
        return all("Expecting more raw data" not in line for line in self.header_lines)

    def simulate_sequencer(self):
        if args.test:
            self.read_file()
        else:
            seqArgs = [
                "sequencer",
                "-s",
                "-c",
                args.osa_config_file,
                "-d",
                f"{year:04}_{month:02}_{day:02}",
                self.telescope,
            ]
            log.debug(f"Executing {' '.join(seqArgs)}")
            seqr = subprocess.Popen(
                seqArgs,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            self.stdout, self.stderr = seqr.communicate()
            log.info(self.stdout)
            if seqr.returncode != 0:
                log.warning(f"Sequencer returns error code {seqr.returncode}")
                return False
            self.seqLines = self.stdout.split("\n")
        return True

    def read_file(self):
        log.debug(f"Reading example of a sequencer output {exampleSeq()}")
        with open(exampleSeq(), "r") as self.stdout:
            stdout_tmp = self.stdout.read()
            log.info(stdout_tmp)
            self.seqLines = stdout_tmp.split("\n")
        return

    def parse_sequencer(self):
        log.debug(f"Parsing sequencer table of {self.telescope}")
        header = True
        data = False
        for line in self.seqLines:
            if data and line:
                self.data_lines.append(line)
            elif "Tel   Seq" in line:
                data = True
                header = False
                self.keyLine = line
            elif header:
                self.header_lines.append(line)
        return

    def build_Sequences(self):
        log.debug(f"Creating Sequence objects for {self.telescope}")
        self.sequences = [Sequence(self.keyLine, line) for line in self.data_lines]
        return bool(self.sequences)

    def close(self):
        log.info("Closing...")
        if args.simulate:
            closerArgs = [
                "closer",
                "-s",
                "-c",
                args.osa_config_file,
                "-v",
                "-y",
                "-d",
                f"{year:04}_{month:02}_{day:02}",
                self.telescope,
            ]
        else:
            closerArgs = [
                "closer",
                "-c",
                args.osa_config_file,
                "-v",
                "-y",
                "-d",
                f"{year:04}_{month:02}_{day:02}",
                self.telescope,
            ]

        if args.test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closerArgs)}")
        closer = subprocess.Popen(
            closerArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False
        )
        stdout, _ = closer.communicate()
        if closer.returncode != 0:
            log.warning(
                f"closer returned error code {closer.returncode}! See output: {stdout}"
            )
            return False
        self.closed = True
        return True


class Sequence(object):
    """
    As for now the keys for the 'dictSequence' are:
    (LST1) Tel Seq Parent Type Run Subruns Source Wobble Action Tries JobID
    State Host CPU_time Walltime Exit DL1% MUONS% DATACHECK% DL2%

    All the values in the 'dictSequence' are strings
    """

    def __init__(self, keyLine, sequence):
        self.keyLine = keyLine
        self.sequence = sequence
        self.understood = False
        self.readyToClose = False
        self.discarded = False
        self.closed = False

        self.parse_sequence()

    def parse_sequence(self):
        log.debug("Parsing sequence")
        self.dictSequence = dict(zip(self.keyLine.split(), self.sequence.split()))
        log.debug(self.dictSequence)
        return

    def is_closed(self):
        return self.dictSequence["Action"] == "Closed"

    def is_running(self):
        return self.dictSequence["State"] == "RUNNING"

    def is_complete(self):
        return self.dictSequence["State"] == "COMPLETED"

    def is_on_hold(self):
        return self.dictSequence["State"] == "PENDING"

    def is_100(self):
        """Check that all analysis products are 100% complete."""
        if (
            self.dictSequence["Tel"] != "ST"
                and self.dictSequence["DL1%"] == "100"
                and self.dictSequence["DL1AB%"] == "100"
                and self.dictSequence["MUONS%"] == "100"
                and self.dictSequence["DL2%"] == "100"
        ):
            return True

    def is_flawless(self):
        log.debug("Check if flawless")
        if (
            self.dictSequence["Type"] == "DATA"
                and self.dictSequence["Exit"] == "0:0"
                and self.is_100()
                and self.dictSequence["State"] == "COMPLETED"
                and int(self.dictSequence["Subruns"]) > 0
        ):
            return True
        if (
            self.dictSequence["Type"] == "PEDCALIB"
                and self.dictSequence["Exit"] == "0:0"
                and self.dictSequence["DL1%"] == "None"
                and self.dictSequence["DATACHECK%"] == "None"
                and self.dictSequence["MUONS%"] == "None"
                and self.dictSequence["DL2%"] == "None"
                and self.dictSequence["State"] == "COMPLETED"
        ):
            return True

        return False

    def has_all_subruns(self):
        import glob

        log.debug("Check for missing subruns in the middle")
        if self.dictSequence["Type"] == "PEDCALIB":
            log.debug("Cannot check for missing subruns in the middle for CALIBRATION")
            return True
        search_str = f"{analysis_path(self.dictSequence['Tel'])}/" \
                     f"dl1*{int(self.dictSequence['Run']):05d}*.h5"
        subrun_nrs = sorted(
            [int(os.path.basename(f).split(".")[2]) for f in glob.glob(search_str)]
        )
        return bool(subrun_nrs and len(subrun_nrs) == int(self.dictSequence["Subruns"]))

    def close(self):
        log.info("Closing sequence...")
        if args.simulate:
            closerArgs = [
                "closer",
                "-s",
                "-v",
                "-y",
                "-d",
                f"{year:04}_{month:02}_{day:02}",
                f"--seq={self.dictSequence['Run']}",
                self.dictSequence["Tel"],
            ]
        else:
            closerArgs = [
                "closer",
                "-v",
                "-y",
                "-d",
                f"{year:04}_{month:02}_{day:02}",
                f"--seq={self.dictSequence['Run']}",
                self.dictSequence["Tel"],
            ]

        if args.test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closerArgs)}")
        closer = subprocess.Popen(
            closerArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        stdout, stderr = closer.communicate()
        if closer.returncode != 0:
            log.warning(
                f"'closer' returned error code {closer.returncode}! See output: {stdout}"
            )
            return False
        self.closed = True
        return True


# add the sequences to the incidencesDict
class Incidence(object):
    def __init__(self, telescope):
        self.header = None
        self.incidencesStereo = None
        self.telescope = telescope
        # known incidences (keys in both dicts have to be unique!):
        self.incidencesMono = {
            "short_run": "Short runs (few number of subruns)",
            "default_time_calib": "Used a default time calibration file",
        }
        self.incidencesDict = {}
        for k in self.incidencesMono:
            self.incidencesDict[k] = []
        # runs with these errors get discarded
        self.errors_to_discard = []
        self.write_header()

    def write_header(self):
        self.header = (
            f"NIGHT={year:04}-{month:02}-{day:02}\nTELESCOPE={self.telescope}\nCOMMENTS="
        )
        return

    def write_error(self, text, runs):
        return f"{text}: {', '.join(run for run in runs)}."

    def has_incidences(self):
        return any(self.incidencesDict[i] for i in self.incidencesDict)

    def check_previous_incidences(self, tel):
        input_file = incidencesFileTmp(tel)
        log.debug(f"Checking for previous incidences in {input_file}")
        found_incidences = False
        if self.read_previous_incidences(input_file):
            found_incidences = True
        if tel == "ST":
            for t in ("LST1", "LST2"):
                if self.check_previous_incidences(t):
                    found_incidences = True
        return found_incidences

    def read_previous_incidences(self, input_file):
        log.debug(f"Trying to read {input_file}")
        try:
            with open(input_file, "r") as f:
                for line in [line for line in f.read().split("\n") if line]:
                    self.add_incidence(line.split(":")[0], line.split(":")[1])
            return True
        except IOError:
            log.warning(f"Could not open {input_file}")
            return False

    def add_incidence(self, error, runstr):
        log.debug(f"Adding {runstr} to incidence {error}")
        if error not in self.incidencesDict:
            log.warning(f"Unknown incidence {error} !")
            return False
        if runstr in self.incidencesDict[error]:
            log.debug(f"Incidence {error} already contains run {runstr}")
            return False
        self.incidencesDict[error].append(runstr)
        return True

    def save_incidences(self):
        log.debug(f"Saving incidences to {incidencesFileTmp(self.telescope)}")
        if args.simulate:
            log.debug(self.incidencesDict)
            return True
        with open(incidencesFileTmp(self.telescope), "w") as f:
            for error in self.incidencesDict:
                for run in self.incidencesDict[error]:
                    log.debug(f"{error}:{run}")
                    f.write(f"{error}:{run}\n")
        return True

    def is_run_discarded(self, run):
        for error in self.errors_to_discard:
            for r in self.incidencesDict[error]:
                if run in r:
                    return True
        return False

    def write_incidences(self):
        log.info(f"Writing down incidences for {self.telescope}:")
        incidences = "".join(self.write_error(
                    self.incidencesMono[k], self.incidencesDict[k]
                ) for k in self.incidencesMono if self.incidencesDict[k])
        log.info(self.header + incidences)
        self.create_incidenceFile(self.header + incidences)
        return

    def create_incidenceFile(self, text):
        log.debug(f"Creating {incidencesFile(self.telescope)} file")
        if not args.simulate:
            with open(incidencesFile(self.telescope), "w+") as f:
                f.write(text)
        return


def is_night_time():
    if 8 <= hour <= 18:
        return False
    log.error("It is dark outside...")
    return True


def understand_sequence(tel, seq):
    if seq.is_closed():
        seq.understood = True
        log.info("Is closed")
        seq.closed = True
        seq.readyToClose = True
        return True

    if tel.incidence.is_run_discarded(seq.dictSequence["Run"]):
        seq.understood = True
        log.info("Was discarded")
        seq.discarded = True
        seq.readyToClose = True
        return True

    if not seq.is_complete():
        log.info("Is not completed yet")
        return True

    #  returns True if check is not possible, e.i. for ST and CALIBRATION
    if not seq.has_all_subruns():
        log.warning("At least one subrun is missing!")
        return False

    if seq.is_flawless():
        seq.understood = True
        log.info("Is flawless")
        seq.readyToClose = True
        return True
    return False


if __name__ == "__main__":

    args = argument_parser().parse_args()

    # for the console output
    log.setLevel(logging.INFO)

    if args.log:
        fh = logging.FileHandler(args.log)
        fh.setLevel(logging.DEBUG)
        log.addHandler(fh)
        log.info(f"Logging verbose output to {args.log}")

    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.debug("Verbose output.")

    if args.simulate:
        log.debug("Simulation mode")

    if args.test:
        log.debug("Test mode.")

    if args.ignorecronlock:
        log.debug("Ignoring cron.lock")

    if args.onlyIncidences:
        log.debug("Writing only incidences")

    if args.force:
        log.debug("Force the closing")

    if args.runwise:
        log.debug("Closing run-wise")

    if args.onlyIncidences and args.force:
        log.error(
            "The command line arguments 'onlyIncidences' and 'force' are incompatible"
        )
        sys.exit(1)

    if args.date:
        year = args.date.year
        month = args.date.month
        day = args.date.day
        hour = 12
    else:
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        day = datetime.datetime.now().day
        hour = datetime.datetime.now().hour

    nightdir = f"{year:04d}{month:02d}{day:02d}"

    message = (
        f"\n========== Starting {os.path.basename(__file__)}"
        f" at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f" for night {year:04}_{month:02}_{day:02} for {' '.join(args.tel)} ===========\n"
    )
    log.info(message)

    if is_night_time():
        sys.exit(1)

    prod_id = get_prod_id()

    # create telescope, sequence, problem and incidence objects
    log.info("Simulating sequencer...")

    telescopes = dict((tel, Telescope(tel)) for tel in args.tel)

    # loop over telescopes and trying to interpret sequencer
    # a goodTel is a telescope that has sequences :)
    for tel in (goodTel for goodTel in args.tel if telescopes[goodTel].sequences):
        log.info(f"Processing {tel}...")

        if not telescopes[tel].incidence.check_previous_incidences(tel):
            log.debug("No previous incidences found")

        # loop over sequences
        for seq in telescopes[tel]:
            log.info(f"Processing sequence {seq.dictSequence['Run']}...")

            if not understand_sequence(telescopes[tel], seq):
                log.warning(f"Could not interpret sequence {seq.dictSequence['Run']}")
                continue

            if args.runwise and seq.readyToClose and not seq.closed:
                seq.close()

        if not telescopes[tel].incidence.save_incidences():
            log.warning("Could not save incidences to tmp file")

        if args.onlyIncidences:
            telescopes[tel].incidence.write_incidences()
            continue

        # skip these checks if closing is forced
        if not args.force:

            if not telescopes[tel].is_transferred():
                log.warning(f"More raw data expected for {tel}!")
                continue
            if not all([seq.readyToClose for seq in telescopes[tel]]):
                log.warning(f"{tel} is NOT ready to close!")
                continue

        log.info(f"Closing {tel}...")
        if not args.woIncidences:
            telescopes[tel].incidence.write_incidences()
        if not telescopes[tel].close():
            log.warning(f"Could not close the day for {tel}!")
            # TODO send email, executing the closer failed!

    log.info("Exit")
