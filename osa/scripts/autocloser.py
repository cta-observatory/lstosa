"""
Script to handle the automatic closing of the OSA
checking that all jobs are correctly finished.
"""

import argparse
import datetime
import glob
import logging
import os
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import analysis_path, DEFAULT_CFG
from osa.utils.cliopts import valid_date
from osa.utils.logging import myLogger
from osa.utils.utils import night_finished_flag, lstdate_to_dir, is_day_closed, get_prod_id
from osa.webserver.utils import set_no_observations_flag

__all__ = ["Telescope", "Sequence"]

log = myLogger(logging.getLogger())


parser = argparse.ArgumentParser(
    description="Automatic completion and sequence closer."
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
    "--ignore-cronlock", action="store_true", help='Ignore "cron.lock"'
)
parser.add_argument(
    "-d",
    "--date",
    help="Date - format YYYY_MM_DD",
    type=valid_date
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="Force the autocloser to close the day"
)
parser.add_argument(
    "--no-dl2",
    action="store_true",
    default=False,
    help="Disregard the production of DL2 files",
)
parser.add_argument(
    "-r", "--runwise", action="store_true", help="Close the day run-wise."
)
parser.add_argument(
    "-c", "--config-file", type=Path, default=DEFAULT_CFG, help="OSA config file."
)
parser.add_argument("-l", "--log", type=Path, default=None, help="Write log to a file.")
parser.add_argument("tel_id", type=str, choices=["LST1"])


def example_seq():
    return "./extra/example_sequencer.txt"


def cron_lock(tel) -> Path:
    """Create a lock file for the cron jobs."""
    return analysis_path(tel) / "cron.lock"


class Telescope(object):
    """

    Parameters
    ----------
    telescope : str
        Options: LST1, LST2 or ST
    date : str
        Date in format YYYY_MM_DD

    Attributes
    ----------
    sequences: list of autocloser.Sequence
        Holds a Sequence object for each Sequence/Run belonging to the telescope.
    """

    def __init__(
            self,
            telescope,
            date,
            ignore_cronlock: bool = False,
            test: bool = False,
            simulate: bool = False
    ):

        self.telescope = telescope
        # necessary to make sure that cron.lock gets deleted in the end
        self.cron_lock = cron_lock(self.telescope)
        self.keyLine = None
        self.locked = False
        self.closed = False
        self.header_lines = []
        self.data_lines = []
        self.sequences = []
        self.seq_lines = None
        self.stdout = None
        self.stderr = None

        if self.is_closed():
            log.info(f"{self.telescope} is already closed! Ignoring {self.telescope}")
            return
        if not analysis_path(self.telescope).exists():
            log.warning(
                f"Analysis directory does not exist for {self.telescope}! "
                f"Ignoring {self.telescope}"
            )
            return
        if not self.lock_automatic_sequencer() and not ignore_cronlock:
            log.warning(f"{self.telescope} already locked! Ignoring {self.telescope}")
            return
        if not self.simulate_sequencer(date, test, simulate):
            log.warning(
                f"Simulation of the sequencer failed "
                f"for {self.telescope}! Ignoring {self.telescope}"
            )
            return

        self.parse_sequencer()

        if not self.build_sequences():
            log.warning(
                f"Sequencer for {self.telescope} is empty! Ignoring {self.telescope}"
            )

            if not simulate and not test:
                host = cfg.get("WEBSERVER", "HOST")
                flat_date = lstdate_to_dir(date)
                set_no_observations_flag(host, flat_date, options.prod_id)
            return

    def __iter__(self):
        return iter(self.sequences)

    def __del__(self):
        if self.locked:
            log.debug(f"Deleting {self.cron_lock}")
            os.remove(self.cron_lock)

    def is_closed(self):
        log.debug(f"Checking if {self.telescope} is closed")
        if night_finished_flag():
            self.closed = True
            return True
        return False

    def lock_automatic_sequencer(self):
        """Check for cron lock file or create it if it does not exist."""
        if cron_lock(self.telescope).exists():
            return False
        log.debug(f"Creating {cron_lock(self.telescope)}")
        cron_lock(self.telescope).touch()
        self.locked = True
        return True

    def is_transferred(self):
        log.debug(f"Checking if raw data is completely transferred for {self.telescope}")
        return all("Expecting more raw data" not in line for line in self.header_lines)

    def simulate_sequencer(self, date, config_file, test):
        if test:
            self.read_file()
        else:
            sequencer_cmd = [
                "sequencer",
                "-s",
                "-c",
                config_file,
                "-d",
                date,
                self.telescope,
            ]
            log.debug(f"Executing {' '.join(sequencer_cmd)}")
            sequencer = subprocess.Popen(
                sequencer_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            self.stdout, self.stderr = sequencer.communicate()
            log.info(self.stdout)
            if sequencer.returncode != 0:
                log.warning(f"Sequencer returns error code {sequencer.returncode}")
                return False
            self.seq_lines = self.stdout.split("\n")
        return True

    def read_file(self):
        log.debug(f"Reading example of a sequencer output {example_seq()}")
        with open(example_seq(), "r") as self.stdout:
            stdout_tmp = self.stdout.read()
            log.info(stdout_tmp)
            self.seq_lines = stdout_tmp.split("\n")
        return

    def parse_sequencer(self):
        log.debug(f"Parsing sequencer table of {self.telescope}")
        header = True
        data = False
        for line in self.seq_lines:
            if data and line:
                self.data_lines.append(line)
            elif "Tel   Seq" in line:
                data = True
                header = False
                self.keyLine = line
            elif header:
                self.header_lines.append(line)
        return

    def build_sequences(self):
        log.debug(f"Creating Sequence objects for {self.telescope}")
        self.sequences = [Sequence(self.keyLine, line) for line in self.data_lines]
        return bool(self.sequences)

    def close(
            self,
            date: str,
            config_file,
            simulate: bool = False,
            test: bool = False
    ):
        log.info("Closing...")
        if simulate:
            closer_cmd = [
                "closer",
                "-s",
                "-c",
                str(config_file),
                "-v",
                "-y",
                "-d",
                date,
                self.telescope,
            ]
        else:
            closer_cmd = [
                "closer",
                "-c",
                config_file,
                "-v",
                "-y",
                "-d",
                date,
                self.telescope,
            ]

        if test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closer_cmd)}")
        closer = subprocess.Popen(
            closer_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False
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
    As for now the keys for the 'dict_sequence' are:
    (LST1) Tel Seq Parent Type Run Subruns Source Wobble Action Tries JobID
    State CPU_time Exit DL1% MUONS% DL1AB% DATACHECK% DL2%

    All the values in the 'dict_sequence' are strings
    """

    def __init__(self, keyLine, sequence):
        self.dict_sequence = {}
        self.keyLine = keyLine
        self.sequence = sequence
        self.understood = False
        self.readyToClose = False
        self.discarded = False
        self.closed = False

        self.parse_sequence()

    def parse_sequence(self):
        log.debug("Parsing sequence")
        self.dict_sequence = dict(zip(self.keyLine.split(), self.sequence.split()))
        log.debug(self.dict_sequence)
        return

    def is_closed(self):
        return self.dict_sequence["Action"] == "Closed"

    def is_running(self):
        return self.dict_sequence["State"] == "RUNNING"

    def is_complete(self):
        return self.dict_sequence["State"] == "COMPLETED"

    def is_on_hold(self):
        return self.dict_sequence["State"] == "PENDING"

    def is_100(self, no_dl2: bool):
        """Check that all analysis products are 100% complete."""
        if no_dl2:
            if (
                self.dict_sequence["Tel"] != "ST"
                    and self.dict_sequence["DL1%"] == "100"
                    and self.dict_sequence["DL1AB%"] == "100"
                    and self.dict_sequence["MUONS%"] == "100"
            ):
                return True
        elif (
            self.dict_sequence["Tel"] != "ST"
                and self.dict_sequence["DL1%"] == "100"
                and self.dict_sequence["DL1AB%"] == "100"
                and self.dict_sequence["MUONS%"] == "100"
                and self.dict_sequence["DL2%"] == "100"
        ):
            return True

    def is_flawless(self, no_dl2: bool):
        log.debug("Check if flawless")
        if (
            self.dict_sequence["Type"] == "DATA"
                and self.dict_sequence["Exit"] == "0:0"
                and self.is_100(no_dl2=no_dl2)
                and self.dict_sequence["State"] == "COMPLETED"
        ):
            return True
        if (
            self.dict_sequence["Type"] == "PEDCALIB"
                and self.dict_sequence["Exit"] == "0:0"
                and self.dict_sequence["State"] == "COMPLETED"
        ):
            return True

        return False

    def has_all_subruns(self):
        """Check that all subruns are complete."""
        if self.dict_sequence["Type"] == "PEDCALIB":
            log.debug("Cannot check for missing subruns for CALIBRATION sequence")
            return True
        search_str = (
            f"{analysis_path(self.dict_sequence['Tel'])}/"
            f"dl1*{self.dict_sequence['Run']}*.h5"
        )
        subrun_nrs = sorted(
            [int(os.path.basename(file).split(".")[2]) for file in glob.glob(search_str)]
        )
        return bool(subrun_nrs and len(subrun_nrs) == int(self.dict_sequence["Subruns"]))

    def close(self, date: str, simulate: bool = False, test: bool = False):
        log.info("Closing sequence...")
        if simulate:
            closerArgs = [
                "closer",
                "-s",
                "-v",
                "-y",
                "-d",
                date,
                f"--seq={self.dict_sequence['Run']}",
                self.dict_sequence["Tel"],
            ]
        else:
            closerArgs = [
                "closer",
                "-v",
                "-y",
                "-d",
                date,
                f"--seq={self.dict_sequence['Run']}",
                self.dict_sequence["Tel"],
            ]

        if test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closerArgs)}")
        closer = subprocess.Popen(
            closerArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        stdout, stderr = closer.communicate()
        if closer.returncode != 0:
            log.warning(
                f"closer returned error code {closer.returncode}! See output: {stdout}"
            )
            return False
        self.closed = True
        return True


def is_night_time(hour):
    if 8 <= hour <= 18:
        return False
    log.error("It is dark outside...")
    return True


def understand_sequence(seq, no_dl2: bool):

    if no_dl2:
        log.info("Assumed no DL2 production")

    if seq.is_closed():
        seq.understood = True
        log.info("Is closed")
        seq.closed = True
        seq.readyToClose = True
        return True

    if not seq.is_complete():
        log.info("Is not completed yet")
        return True

    #  returns True if check is not possible, e.i. for ST and CALIBRATION
    if not seq.has_all_subruns():
        log.warning("At least one subrun is missing!")
        return False

    if seq.is_flawless(no_dl2=no_dl2):
        seq.understood = True
        log.info("Is flawless")
        seq.readyToClose = True
        return True
    return False


def main():
    args = parser.parse_args()

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

    if args.ignore_cronlock:
        log.debug("Ignoring cron.lock")

    if args.force:
        log.debug("Force the closing")

    if args.runwise:
        log.debug("Closing run-wise")

    if args.date:
        date = args.date.strftime("%Y_%m_%d")
        hour = 12
    else:
        date = datetime.datetime.now().strftime("%Y_%m_%d")
        hour = datetime.datetime.now().hour

    options.date = date
    options.tel_id = args.tel_id
    options.prod_id = get_prod_id()

    message = (
        f"========== Starting {Path(__file__).stem}"
        f" at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f" for night {date} ==========="
    )
    log.info(message)

    if is_night_time(hour):
        sys.exit(1)

    elif is_day_closed():
        log.info(f"Night {options.date} already closed for {options.tel_id}")
        sys.exit(0)

    # create telescope and sequence objects
    log.info("Simulating sequencer...")

    telescope = Telescope(args.tel_id, date)

    log.info(f"Processing {args.tel_id}...")

    # loop over sequences
    for seq in telescope:
        log.info(f"Processing sequence {seq.dict_sequence['Run']}...")

        if not understand_sequence(seq, no_dl2=args.no_dl2):
            log.warning(f"Could not interpret sequence {seq.dict_sequence['Run']}")
            continue

        if args.runwise and seq.readyToClose and not seq.closed:
            seq.close()

    # skip these checks if closing is forced
    if not args.force:
        if not telescope.is_transferred():
            log.warning(f"More raw data expected for {args.tel}!")
        if not all(seq.readyToClose for seq in telescope):
            log.warning(f"{args.tel} is NOT ready to close!")

    log.info(f"Closing {args.tel_id}...")

    if not telescope.close(
            date=date,
            config_file=args.config_file,
            test=args.test
    ):
        log.warning(f"Could not close the day for {args.tel}!")
        # TODO send email, executing the closer failed!

    log.info("Exit")


if __name__ == "__main__":
    main()
