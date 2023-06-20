"""
Script to handle the automatic closing of the OSA
checking that all jobs are correctly finished.
"""

import datetime
import glob
import logging
import os
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.paths import analysis_path
from osa.utils.cliopts import autocloser_cli_parser
from osa.utils.logging import myLogger
from osa.utils.mail import send_warning_mail
from osa.utils.utils import (
    night_finished_flag,
    is_day_closed,
    get_prod_id,
    is_night_time,
    cron_lock,
    example_seq,
    date_to_iso,
)

__all__ = ["Telescope", "Sequence"]

log = myLogger(logging.getLogger())


class Telescope:
    """Handle the telescope sequences, simulate and check them."""

    def __init__(
        self,
        telescope,
        date,
        config_file: Path,
        ignore_cronlock: bool = False,
        test: bool = False,
    ):
        """
        Parameters
        ----------
        telescope : str
            Options: LST1
        date : str
            Date in format YYYY-MM-DD
        config_file : pathlib.Path
            Path to the configuration file
        ignore_cronlock : bool
            Ignore cron lock file
        test : bool
            Run sequencer in test mode
        """

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
        if not self.simulate_sequencer(date, config_file, test):
            log.warning(
                f"Simulation of the sequencer failed "
                f"for {self.telescope}! Ignoring {self.telescope}"
            )
            return

        self.parse_sequencer()

        if not self.build_sequences():
            log.info(f"Sequencer for {self.telescope} is empty! Exiting.")
            sys.exit()

    def __iter__(self):
        return iter(self.sequences)

    def __del__(self):
        """Delete cron lock file if it exists."""
        if self.locked:
            log.debug(f"Deleting {self.cron_lock}")
            os.remove(self.cron_lock)

    def is_closed(self):
        """Check if night is finished flag exists."""
        log.debug(f"Checking if {self.telescope} is closed")
        if night_finished_flag().exists():
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

    def simulate_sequencer(self, date: str, config_file: Path, test: bool):
        """Launch the sequencer in simulation mode."""
        if test:
            self.read_file()
        else:
            sequencer_cmd = [
                "sequencer",
                "-s",
                "-c",
                str(config_file),
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
        """Read an example sequencer output."""
        log.debug(f"Reading example of a sequencer output {example_seq()}")
        with open(example_seq(), "r") as self.stdout:
            stdout_tmp = self.stdout.read()
            log.info(stdout_tmp)
            self.seq_lines = stdout_tmp.split("\n")

    def parse_sequencer(self):
        """Parse the sequencer output lines."""
        log.debug(f"Parsing sequencer table of {self.telescope}")
        header = True
        data = False
        for line in self.seq_lines:
            if data and line:
                if line.startswith("LST1"):
                    self.data_lines.append(line)
            elif "Tel   Seq" in line:
                data = True
                header = False
                self.keyLine = line
            elif header:
                self.header_lines.append(line)

    def build_sequences(self):
        """Build the sequences and return True if there are any."""
        log.debug(f"Creating Sequence objects for {self.telescope}")
        self.sequences = [Sequence(self.keyLine, line) for line in self.data_lines]
        return bool(self.sequences)

    def close(
        self,
        date: str,
        config: Path,
        no_dl2: bool,
        simulate: bool = False,
        test: bool = False,
    ):
        """Launch the closer command."""
        log.info("Closing...")

        closer_cmd = [
            "closer",
            "-c",
            str(config),
            "-y",
            "-d",
            date,
            self.telescope,
        ]

        if simulate:
            closer_cmd.insert(1, "-s")

        if no_dl2:
            closer_cmd.insert(1, "--no-dl2")

        if test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closer_cmd)}")
        closer = subprocess.Popen(
            closer_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False
        )
        stdout, _ = closer.communicate()
        if closer.returncode != 0:
            log.warning(f"closer returned error code {closer.returncode}! See output: {stdout}")
            return False
        self.closed = True
        return True


class Sequence:
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
        if (
            no_dl2
            and self.dict_sequence["Tel"] != "ST"
            and self.dict_sequence["DL1%"] == "100"
            and self.dict_sequence["DL1AB%"] == "100"
            and self.dict_sequence["MUONS%"] == "100"
        ):
            return True

        if (
            self.dict_sequence["Tel"] != "ST"
            and self.dict_sequence["DL1%"] == "100"
            and self.dict_sequence["DL1AB%"] == "100"
            and self.dict_sequence["MUONS%"] == "100"
            and self.dict_sequence["DL2%"] == "100"
        ):
            return True

        return False

    def is_flawless(self, no_dl2: bool):
        """Check that all jobs statuses are completed."""
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
        """
        Check that all subruns are complete by checking the
        total number of subrun wise DL1 files.
        """
        if self.dict_sequence["Type"] == "PEDCALIB":
            log.debug("Cannot check for missing subruns for CALIBRATION sequence")
            return True
        search_str = (
            f"{analysis_path(self.dict_sequence['Tel'])}/" f"dl1*{self.dict_sequence['Run']}*.h5"
        )
        subrun_nrs = sorted(
            [int(os.path.basename(file).split(".")[2]) for file in glob.glob(search_str)]
        )
        return bool(subrun_nrs and len(subrun_nrs) == int(self.dict_sequence["Subruns"]))

    def close(
        self, date: str, config: Path, no_dl2: bool, simulate: bool = False, test: bool = False
    ):
        """Close the sequence by calling the 'closer' script for a given sequence."""
        log.info("Closing sequence...")

        closer_cmd = [
            "closer",
            "-c",
            str(config),
            "-y",
            "-d",
            date,
            f"--seq={self.dict_sequence['Run']}",
            self.dict_sequence["Tel"],
        ]

        if simulate:
            closer_cmd.insert(1, "-s")

        if no_dl2:
            closer_cmd.insert(1, "--no-dl2")

        if test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closer_cmd)}")
        closer = subprocess.Popen(closer_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, _ = closer.communicate()
        if closer.returncode != 0:
            log.warning(f"closer returned error code {closer.returncode}! See output: {stdout}")
            return False

        self.closed = True

        return True


def understand_sequence(seq, no_dl2: bool):
    """Check if sequence is completed and ready to be closed."""
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
    """Check for job completion and close sequence if necessary."""
    args = autocloser_cli_parser().parse_args()

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

    if args.no_dl2:
        log.debug("Assumed no DL2 production")

    if args.date:
        options.date = args.date
        hour = 12
    else:
        options.date = datetime.datetime.now()
        hour = datetime.datetime.now().hour

    options.tel_id = args.tel_id
    options.prod_id = get_prod_id()
    date = date_to_iso(options.date)

    log.info(
        f"========== Starting {Path(__file__).stem} at "
        f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f" for date {date} ==========="
    )

    if is_night_time(hour):
        sys.exit(1)

    elif is_day_closed():
        log.info(f"Date {date} already closed for {options.tel_id}")
        sys.exit(0)

    # create telescope and sequence objects
    log.info("Simulating sequencer...")

    telescope = Telescope(args.tel_id, date, args.config)

    log.info(f"Processing {args.tel_id}...")

    # Loop over the sequences
    for sequence in telescope:
        log.info(f"Processing sequence {sequence.dict_sequence['Run']}...")

        if not understand_sequence(sequence, no_dl2=args.no_dl2):
            log.warning(f"Could not interpret sequence {sequence.dict_sequence['Run']}")
            continue

        if args.runwise and sequence.readyToClose and not sequence.closed:
            sequence.close(
                date=date,
                config=args.config,
                no_dl2=args.no_dl2,
                simulate=args.simulate,
                test=args.test,
            )

    # skip these checks if closing is forced
    if not args.force and not all(seq.readyToClose for seq in telescope):
        sys.exit(f"{args.tel_id} is NOT ready to close. Exiting.")

    log.info(f"Closing {args.tel_id}...")

    if not telescope.close(
        date=date, config=args.config, no_dl2=args.no_dl2, simulate=args.simulate, test=args.test
    ):
        log.warning(f"Could not close the day for {args.tel_id}!")
        # Send email, if later than 18:00 UTC and telescope is not ready to close
        if hour > 14:
            send_warning_mail(date=date)

    log.info("Exit")


if __name__ == "__main__":
    main()
