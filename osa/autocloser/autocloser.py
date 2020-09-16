import argparse
import datetime
import logging
import os
import re
import subprocess

from osa.utils import options
from osa.utils.utils import lstdate_to_dir
from osa.configs.config import cfg
from osa.utils.cliopts import set_default_directory_if_needed

__all__ = ["Telescope", "Sequence"]

log = logging.getLogger("autocloser_logger")
log.setLevel(logging.DEBUG)


# formatter for the output
class MyFormatter(logging.Formatter):
    FORMATS = {logging.INFO: "%(message)s", "DEFAULT": "%(levelname)s: %(message)s"}

    def format(self, record):
        self._fmt = self.FORMATS.get(record.levelno, self.FORMATS["DEFAULT"])
        return logging.Formatter.format(self, record)


# settings / global variables
def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y_%m_%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'."
        raise argparse.ArgumentTypeError(msg)


def argument_parser():
    parser = argparse.ArgumentParser(description="This script is an automatic error handler and closer for LSTOSA.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose mode")
    parser.add_argument("-t", "--test", action="store_true",
                        help="Test mode with example sequences, only works locally")
    parser.add_argument("-s", "--simulate", action="store_true",
                        help="Create nothing, only simulate closer (safe mode)")
    parser.add_argument("--ignorecronlock", action="store_true", help='Ignore "cron.lock"')
    parser.add_argument("-i", "--onlyIncidences", action="store_true", help="Writing down only incidences, not closing")
    parser.add_argument("-d", "--date", help="Date - format YYYY_MM_DD", type=valid_date)
    parser.add_argument("-f", "--force", action="store_true", help="Force the autocloser to close the day")
    parser.add_argument("-e", "--equal", action="store_true", help="Skip check for equal amount of sequences")
    parser.add_argument("-w", "--woIncidences", action="store_true", help="Close without writing down incidences.")
    parser.add_argument("-r", "--runwise", action="store_true", help="Close the day run-wise.")
    parser.add_argument("-c", "--config-file", dest="osa_config_file", default="cfg/sequencer.cfg",
                        help="OSA config file.")
    parser.add_argument("-l", "--log", default="", help="Write log to a file.")
    parser.add_argument("tel", nargs="*", default="LST1", choices=["LST1", "LST2", "ST"])
    return parser


def analysis_path(tel):
    options.tel_id = tel
    options.date = f"{year:04}_{month:02}_{day:02}"
    running_analysis_dir = set_default_directory_if_needed()
    if args.test:
        return f"./{tel}"
    return running_analysis_dir


def closedFlag(tel):
    if args.test:
        return f"./{tel}/NightFinished.txt"
    return f"/data/{tel}/OSA/Closer{year:04}/{month:02}/{day:02}/NightFinished.txt"


def exampleSeq(tel):
    return f"./ExampleSequences/SeqExpl{tel}_4.txt"


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
        Holds an Sequence object for each Sequence/Run belonging to the
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
            log.warning(f"{self.telescope} is already closed! Ignoring {self.telescope}")
            return
        if not os.path.exists(analysis_path(self.telescope)):
            log.warning(f"'Analysis' folder does not exist for {self.telescope}! Ignoring {self.telescope}")
            return
        if not self.lockAutomaticSequencer() and not args.ignorecronlock:
            log.warning(f"{self.telescope} already locked! Ignoring {self.telescope}")
            return
        if not self.simulate_sequencer():
            log.warning(f"Simulation of the sequencer failed for {self.telescope}! Ignoring {self.telescope}")
            return
        self.parse_sequencer()
        if not self.build_Sequences():
            log.warning(f"Sequencer for {self.telescope} is empty! Ignoring {self.telescope}")
            return
        self.problem = Problem(self.telescope)
        self.incidence = Incidence(self.telescope)

    def __iter__(self):
        return iter(self.sequences)

    def __del__(self, log=log, os=os):
        if self.locked:
            log.debug(f"Deleting {self.cl}")
            try:
                os.remove(self.cl)
            except:
                log.warning(f"Could not delete {self.cl}")
                # send email

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
        for line in self.header_lines:
            if "Expecting more raw data" in line:
                return False
        return True

    def simulate_sequencer(self):
        if args.test:
            self.read_file()
        else:
            seqArgs = [
                "python", "sequencer.py",
                "-s",
                "-c", f"{args.osa_config_file}",
                "-d", f"{year:04}_{month:02}_{day:02}",
                self.telescope
            ]
            log.debug(f"Executing {' '.join(seqArgs)}")
            seqr = subprocess.Popen(seqArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            self.stdout, self.stderr = seqr.communicate()
            log.info(self.stdout)
            if seqr.returncode != 0:
                log.warning(f"Sequencer returns error code {seqr.returncode}")
                return False
            self.seqLines = self.stdout.split("\n")
        return True

    def read_file(self):
        log.debug(f"Reading example of a sequencer output {exampleSeq(self.telescope)}")
        with open(exampleSeq(self.telescope), "r") as self.stdout:
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
        if self.sequences:
            return True
        return False

    def close(self):
        log.info("Closing...")
        if args.simulate:
            closerArgs = ["closer", "-s", "-v", "-y", "-d", f"{year:04}_{month:02}_{day:02}", self.telescope]
        else:
            closerArgs = ["closer", "-v", "-y", "-d", f"{year:04}_{month:02}_{day:02}", self.telescope]

        if args.test:
            self.closed = True
            return True

        log.debug(f"Executing {' '.join(closerArgs)}")
        closer = subprocess.Popen(closerArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = closer.communicate()
        if closer.returncode != 0:
            log.warning(f"'closer' returned error code {closer.returncode}! See output: {stdout}")
            return False
        self.closed = True
        return True


class Sequence(object):
    """As for now the keys for the 'dictSequence' are:
    (LST1) Tel Seq Parent Type Run Subruns Source Wobble Action Tries JobID
    State Host CPU_time Walltime Exit  DL1% MUONS% DATACHECK% DL2%

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
        if self.dictSequence["Action"] == "Closed":
            return True
        return False

    def is_running(self):
        if self.dictSequence["State"] == "R":
            return True
        return False

    def is_complete(self):
        if self.dictSequence["State"] == "C":
            return True
        return False

    def is_onHold(self):
        if self.dictSequence["State"] == "H":
            return True
        return False

    def is_100(self):
        if (
                self.dictSequence["Tel"] != "ST"
                and self.dictSequence["DL1%"] == "100"
                and self.dictSequence["DATACHECK%"] == "100"
                and self.dictSequence["MUONS%"] == "100"
                and self.dictSequence["DL2%"] == "100"
        ):
            return True
        if self.dictSequence["Tel"] == "ST" and self.dictSequence["DL3 %"] == "100":
            return True
        return False

    def is_flawless(self):
        log.debug("Check if flawless")
        if (
                self.dictSequence["Type"] == "DATA"
                and self.dictSequence["Exit"] == "0"
                and self.is_100()
                and self.dictSequence["State"] == "C"
                and int(self.dictSequence["Subruns"]) > 0
        ):
            return True
        if (
                self.dictSequence["Type"] == "CALI"
                and self.dictSequence["Exit"] == "0"
                and self.dictSequence["DL1%"] == "None"
                and self.dictSequence["DATACHECK%"] == "None"
                and self.dictSequence["MUONS%"] == "None"
                and self.dictSequence["DL2%"] == "None"
                and self.dictSequence["State"] == "C"
                #and int(self.dictSequence["Subruns"]) == 1
        ):
            return True
        # if (
        #         self.dictSequence["Type"] == "STEREO"
        #         and self.dictSequence["Exit"] == "0"
        #         and self.is_100()
        #         and self.dictSequence["State"] == "C"
        #         and int(self.dictSequence["Subruns"]) > 0
        # ):
        #    return True
        return False

    def is_error2(self):
        log.debug("Check for 'short runs'")
        if (
                self.dictSequence["Type"] == "DATA"
                and (self.dictSequence["Exit"] == "2" or self.dictSequence["Exit"] == "222")
                and self.dictSequence["_Y_%"] == "100"
                and self.dictSequence["_D_%"] == "0"
                and self.dictSequence["_I_%"] == "0"
                and self.dictSequence["State"] == "C"
                and int(self.dictSequence["Subruns"]) < min_subruns_per_run
        ):
            return True
        return False

    def is_error2noint(self):
        log.debug("Check if runs were calibrated with no or few interleaved ped/cal events")
        historyFile = f"{analysis_path(self.dictSequence['Tel'])}/sequence_{self.dictSequence['Tel']}_0{self.dictSequence['Run']}.history"
        with open(historyFile, "r") as f:
            for line in f:
                try:
                    if line.split()[1] == "sorcerer" and int(line.split()[10]) == 2:
                        return True
                except IndexError:
                    continue
        return False

    def is_error23(self):
        log.debug("Check for error 23 in merpp")
        historyFile = f"{analysis_path(self.dictSequence['Tel'])}/sequence_{self.dictSequence['Tel']}_0{self.dictSequence['Run']}.history"
        subruns = []
        with open(historyFile, "r") as f:
            for line in f:
                try:
                    if line.split()[1] == "merpp" and int(line.split()[10]) == 23:
                        subruns.append(line.split()[8].split(".")[1][0:3])
                except IndexError:
                    continue
        return subruns


    def is_rc1_for_ST(self):
        log.debug("Check for rc 1 in ST")
        if (
                self.dictSequence["Type"] == "STEREO"
                and self.dictSequence["Exit"] == "1"
                and self.dictSequence["_S_%"] == "0"
                and self.dictSequence["_Q_%"] == "0"
                and self.dictSequence["State"] == "C"
                and self.dictSequence["Action"] == "Simulate"
        ):
            return True
        return False

    def is_missingReport(self):
        log.debug("Check for missing reports")
        if self.dictSequence["Tel"] == "ST":
            return False
        if (
                self.dictSequence["Type"] == "DATA"
                and self.dictSequence["_Y_%"] == "100"
                and int(self.dictSequence["_D_%"]) < 100
                and self.dictSequence["_I_%"] == "0"
                and self.dictSequence["State"] == "C"
                and self.dictSequence["Action"] == "Veto"
                and int(self.dictSequence["Subruns"]) > 0
                and (self.dictSequence["Exit"] == "255" or self.dictSequence["Exit"] == "25" or self.dictSequence["Exit"] == "254")
        ):
            return True
        return False

    # this check is obsolete since now we have the has_all_subruns check
    def has_equal_nr_of_subruns(self, teldict):
        log.debug("Check for equal nr of subruns")
        if self.dictSequence["Tel"] == "ST":
            log.debug("Cannot check for equal nr of subruns for ST")
            return True
        if "LST1" not in teldict or "LST2" not in teldict:
            log.debug("Cannot check for equal nr of subruns in mono case")
            return True
        otherTel = "LST1" if self.dictSequence["Tel"] == "LST2" else "LST2"
        if teldict[otherTel].closed:
            log.debug("The other telescope seems to be closed")
            return True
        otherSubruns = 0
        for s in teldict[otherTel]:
            if s.dictSequence["Run"] == self.dictSequence["Run"]:
                otherSubruns = int(s.dictSequence["Subruns"])
        if abs(int(self.dictSequence["Subruns"]) - otherSubruns) > max_diff_subruns:
            return False
        return True

    def has_all_subruns(self):
        import glob

        log.debug("Check for missing subruns in the middle")
        if self.dictSequence["Tel"] == "ST":
            log.debug("Cannot check for missing subruns in the middle for ST")
            return True
        if self.dictSequence["Type"] == "CALIBRATION":
            log.debug("Cannot check for missing subruns in the middle for CALIBRATION")
            return True
        search_str = f"{analysis_path(self.dictSequence['Tel'])}/20*_{int(self.dictSequence['Run']):08d}.*_Y_*.root"
        subrun_nrs = sorted([int(os.path.basename(f).split(".")[1][0:3]) for f in glob.glob(search_str)])
        for i, x in enumerate(subrun_nrs, 1):
            if i != x:
                return False
        if not subrun_nrs or subrun_nrs[-1] != int(self.dictSequence["Subruns"]):
            return False
        return True

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
        closer = subprocess.Popen(closerArgs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = closer.communicate()
        if closer.returncode != 0:
            log.warning(f"'closer' returned error code {closer.returncode}! See output: {stdout}")
            return False
        self.closed = True
        return True


# add the sequences to the incidencesDict
class Incidence(object):
    def __init__(self, telescope):
        self.telescope = telescope
        # known incidences (keys in both dicts have to be unique!):
        self.incidencesMono = {
            "error2": "Short runs(# of subruns) excluded from OSA",
            "error2noint": "Runs(# of subruns) for which sorcerer "
                           "returned error 2 "
                           "(too few interleaved Ped/Cal events)",
            "error7": "Runs for which star returned error 7 (No starguider information available)",
            "error17": "Runs for which star returned error 17 (Too many mirrors in bad state)",
            "error34": "Runs for which star returned error 34 (Not appropriate image cleaning)",
            "recReps": "Recovered report files for subruns",
            "error222": "Subruns that could not be calibrated",
            "error23": "Broken report lines for subruns",
        }
        self.incidencesStereo = {"error3": "Short runs(# of subruns) for which superstar files are empty"}
        self.incidencesDict = {}
        for k in self.incidencesMono:
            self.incidencesDict[k] = []
        for k in self.incidencesStereo:
            self.incidencesDict[k] = []
        # runs with these errors get discarded
        self.errors_to_discard = ["error2"]
        self.write_header()

    def write_header(self):
        self.header = f"NIGHT={year:04}-{month:02}-{day:02}\nTELESCOPE={self.telescope}\nCOMMENTS="
        return

    def write_error(self, text, runs):
        return f"{text}: {', '.join(run for run in runs)}."

    def has_incidences(self):
        if any([self.incidencesDict[i] for i in self.incidencesDict]):
            return True
        return False

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
            f = open(input_file, "r")
            for l in [line for line in f.read().split("\n") if line]:
                self.add_incidence(l.split(":")[0], l.split(":")[1])
            f.close()
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
        incidences = ""
        if self.telescope == "ST":
            for k in self.incidencesStereo:
                if self.incidencesDict[k]:
                    incidences += self.write_error(self.incidencesStereo[k], self.incidencesDict[k])
        else:
            for k in self.incidencesMono:
                if self.incidencesDict[k]:
                    incidences += self.write_error(self.incidencesMono[k], self.incidencesDict[k])

        # suggestion by Lab: always create incidence file, even if empty,
        # incidence table is clearer like this
        # if incidences == "" and not args.onlyIncidences:
        #     log.info('   No incidences found.')
        #     return
        log.info(self.header + incidences)
        self.create_incidenceFile(self.header + incidences)
        return

    def create_incidenceFile(self, text):
        log.debug(f"Creating {incidencesFile(self.telescope)} file")
        if not args.simulate:
            with open(incidencesFile(self.telescope), "w+") as f:
                f.write(text)
        return


class Problem(object):
    def __init__(self, telescope):
        self.telescope = telescope
        # self.recoveredReports = ''

    # def recoverReport(self, run):
    #     self.recoveredReports = ''
    #     log.debug('Recovering report files for %s, sequence %s' %
    #               (self.telescope, run))
    #
    #     fileList = [item for item in
    #                 os.listdir('%s' % analysis_path(self.telescope))
    #                 if all(i in item for i in ['%s' % run, '_Y_', '.root'])]
    #     repsList = [item.replace('.rep', '.root').replace('_D_', '_Y_')
    #                 for item in os.listdir('%s' % reportsPath(self.telescope))
    #                 if (all(i in item for i in ['%s' % run, '_D_', '.rep']) and
    #                     os.stat('%s/%s' % (reportsPath(self.telescope),
    #                                        item)).st_size != 0)]
    #     missingReps = [item for item in fileList if item not in repsList]
    #     if not missingReps:
    #         log.warning('Could not determine which report files are missing! '
    #                     'Most likely the report was already recovered and you '
    #                     'have to wait for the sequencer. If this warning '
    #                     'persists there something seriously wrong!')
    #         return False

    # recoverFailure = False
    # subrunList = []
    # for subrun in missingReps:
    #     if args.simulate:
    #         subrunList.append(subrun[20:24])
    #         continue
    #     createReportArgs = ['timeout', '1200', 'bash',
    #                         '%s' % recRepMacro(),
    #                         '%s/%s' % (analysis_path(self.telescope),
    #                                    subrun)]
    #     log.debug('Executing "%s"' % ' '.join(createReportArgs))
    #     createReport = subprocess.Popen(createReportArgs,
    #                                     stdout=subprocess.PIPE,
    #                                     stderr=subprocess.STDOUT)
    #     stdout, stderr = createReport.communicate()
    #     if createReport.returncode == 124:
    #         log.warning('Time out, CreateReport.sh took longer than 20min')
    #     if createReport.returncode != 0:
    #         log.warning("Could not create missing report file for %s!\n"
    #                     "See below the output of the "
    #                     "CreateReport.sh script:\n---\n%s---" %
    #                     (subrun, stdout))
    #         recoverFailure = True
    #         continue
    #     log.debug(stdout)
    #     subrunList.append(subrun[20:24])

    # if subrunList:
    #     subrunList.sort()
    #     self.recoveredReports = '%s(%s)' % (run, ', '.join(i for i in
    #                                                        subrunList))
    # if recoverFailure:
    #     return False
    # return True

    # def determineErrorWithApe(self, run):
    #     log.debug("Running ape to determine error of run %s" % run)
    #     starSummaryFile = '%s/star*%s*%s.root' % (analysis_path(self.telescope),
    #                                               run, self.telescope)
    #     ape = subprocess.Popen('ape %s' % starSummaryFile, shell=True,
    #                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     stdout, stderr = ape.communicate()
    #     if ape.returncode != 1:
    #         log.warning('Return code from ape is %i, should be 1.\n'
    #                     'See output:\n' % ape.returncode + stdout)
    #         return ''
    #     if any([error for error in stdout.split(' ')
    #             if error not in ['7', '17', '34', '2', '20'] and not
    #         error == '\x1b[0m\n\x1b[0m']):
    #         log.warning('Could not interpret "ape", see output:\n' + stdout)
    #         return ''
    #     log.debug('ape output:\n' + stdout)
    #     return stdout

    # def is_too_few_star_events(self, run):
    #     log.debug("Check if too few events in star files")
    #     import ROOT as root
    #     marssys = os.path.expandvars("$MARSSYS")
    #     # if var does not exists, it will not get expanded ...
    #     if marssys == "$MARSSYS":
    #         log.warning("$MARSSYS variable not set in bash")
    #         return False
    #     if root.gSystem.Load(marssys + "/libmars.so") < 0:
    #         log.warning("Could not load Mars library")
    #         return False
    #     # check if there are more than 100 events in the star files
    #     for t in ['LST1', 'LST2']:
    #         star_dir = analysis_path(t).replace("Analysis", "Star")
    #         ch = root.TChain("Events")
    #         ch.Add("%s/20*%s*_I_*.root" % (star_dir, run))
    #         if ch.GetEntries() > 100:
    #             log.debug("More than 100 events found in %s star files" % t)
    #             return False
    #     return True


def is_night_time():
    if 8 <= hour <= 18:
        return False
    log.error("It is dark outside...")
    return True


def understand_mono_sequence(tel, seq):
    """These are "issues" the sequencer solved by itself and we only want
    to note them down by checking the *.history file!

    Parameters
    ----------
    tel
    seq

    Returns
    -------

    """
    if seq.is_error2noint():
        log.info("Updating incidences: run calibrated w/o interleaved ped/cal")
        tel.incidence.add_incidence("error2noint", f"{seq.dictSequence['Run']}({seq.dictSequence['Subruns']})")

    if seq.is_error23():
        subruns_with_error23 = seq.is_error23()
        if len(subruns_with_error23) > max_broken_report_lines_per_sequence:
            log.warning("Too many broken report lines for this sequence!")
            seq.understood = True
            return True
        for subrun in subruns_with_error23:
            run_with_subrun_str = f"{seq.dictSequence['Run']}.{subrun}"
            if tel.incidence.add_incidence("error23", run_with_subrun_str):
                log.info(f"Updating incidences: merpp error 23 for {run_with_subrun_str}")

    # maybe this check is obsolete since we introduced error2noint
    if seq.is_error2():
        seq.understood = True
        log.info("Updating incidences: short runs")
        tel.incidence.add_incidence("error2", f"{seq.dictSequence['Run']}({seq.dictSequence['Subruns']})")
        seq.discarded = True
        log.info("Discarding this sequence from OSA")
        seq.readyToClose = True
        return True

    if seq.is_error7or17or34():
        seq.understood = True
        apeOutput = tel.problem.determineErrorWithApe(seq.dictSequence["Run"])
        if not apeOutput:
            log.warning("Could not determine error 7/17/34 for star with ape")
            return True
        if "7" in apeOutput:
            log.info("Updating incidences: error 7 for star")
            tel.incidence.add_incidence("error7", seq.dictSequence["Run"])
        if "17" in apeOutput:
            log.info("Updating incidences: error 17 for star")
            tel.incidence.add_incidence("error17", seq.dictSequence["Run"])
        if "34" in apeOutput:
            log.info("Updating incidences: error 34 for star")
            tel.incidence.add_incidence("error34", seq.dictSequence["Run"])
        seq.readyToClose = True
        return True

    if seq.is_missingReport():
        seq.understood = True
        if not tel.problem.recoverReport(seq.dictSequence["Run"]):
            # send email that i could not recover report files!
            log.warning(f"Failed to recover at least one missing report file for sequence {seq.dictSequence['Run']}")
        if tel.problem.recoveredReports:
            log.info(f"Updating incidences: Reports recovered for {tel.problem.recoveredReports}")
            tel.incidence.add_incidence("recReps", tel.problem.recoveredReports)
        return True

    return False


def understand_stereo_sequence(tel, seq):
    if seq.is_rc1_for_ST():
        log.info("Waiting for sequence to be closed in M1 or M2...")
        return True

    if seq.is_error3() and tel.problem.is_too_few_star_events(seq.dictSequence["Run"]):
        seq.understood = True
        log.info("Updating incidences: empty superstar file")
        tel.incidence.add_incidence("error3", f"{seq.dictSequence['Run']}({seq.dictSequence['Subruns']})")
        seq.readyToClose = True
        return True

    return False


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

    if tel.telescope == "LST1" or tel.telescope == "LST2":
        if understand_mono_sequence(tel, seq):
            return True

    if tel.telescope == "ST":
        if understand_stereo_sequence(tel, seq):
            return True

    if seq.is_flawless():
        seq.understood = True
        log.info("Is flawless")
        seq.readyToClose = True
        return True
    return False


def has_equal_nr_of_sequences(teldict):
    log.debug("Check for equal nr of runs")
    if "LST1" not in teldict or "LST2" not in teldict:
        log.debug("Cannot check for equal nr of sequences in mono case")
        return True
    if teldict["LST1"].closed or teldict["LST2"].closed:
        log.debug("The other telescope seems to be closed")
        return True
    if len(teldict["LST1"].sequences) != len(teldict["LST2"].sequences):
        return False
    return True


# def sendEmail(message):
# FIXME: Adap it to LST
#     msg = MIMEText(message)
#     msg['Subject'] = 'AutoCloser'
#     msg['From'] = 'LSTOSA Daemon <analysis@ana7.magic.iac.es>'
#     msg['To'] = '<gae-lst-onsite@gae.ucm.es>'
#     try:
#         s = smtplib.SMTP('localhost')
#         s.sendmail('analysis@ana7.magic.iac.es', ['gae-lst-onsite@gae.ucm.es'],
#                    msg.as_string())
#         return True
#     except smtplib.SMTPException:
#         return False


def check_for_output_files(path):
    found_files = [re.search("LST-1.Run", file) for file in os.listdir(path)]
    if any(found_files):
        file_names = [m.group() for m in found_files if m]
        # file_names_str = ', '.join(file_names)
        # sendEmail('WARNING: Following *LST-1.Run* files found '
        #           'in "%s" after closing: %s' % (path, file_names_str))


# the if is basically necessary when building the docs
if __name__ == "__main__":

    #########################################################
    # this stuff should probably go into a separate cfg file!

    # positions of the values in the CC*.run file
    # at the moment we do not use the CC*.run file in the AutoCloser
    hv_settings_pos = 20
    test_run_pos = 38
    sumt_pos = 53
    Moon_filter_pos = 62

    # difference in subruns for one sequence that LST1 and LST2 are allowed to have
    # this check is obsolete since now we have the has_all_subruns check
    max_diff_subruns = 8  # 5
    # how many broken report lines should be tolerated?
    max_broken_report_lines_per_sequence = 2
    max_broken_report_lines_per_telescope = 8
    # how many subruns for a run is the minimum for the run not to be discarded
    # when problems occur?
    min_subruns_per_run = 5
    #########################################################

    args = argument_parser().parse_args()

    if "ST" in args.tel:
        args.tel = ["LST1", "LST2", "ST"]

    # for the console output
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    ch.setFormatter(MyFormatter())
    log.addHandler(ch)

    if args.log:
        fh = logging.FileHandler(args.log)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(MyFormatter())
        log.addHandler(fh)
        log.info(f"Logging verbose output to {args.log}")

    if args.verbose:
        ch.setLevel(logging.DEBUG)
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
        log.error("The command line arguments 'onlyIncidences' and 'force' are incompatible with each other")
        exit(1)

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

    message = f"\n========== Starting {os.path.basename(__file__)}" \
              f" at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" \
              f" for night {year:04}_{month:02}_{day:02} for {' '.join(args.tel)} ===========\n"
    log.info(message)

    # if is_night_time():
    #     exit(1)

    # create telescope, sequence, problem and incidence objects
    log.info("Simulating sequencer...")
    telescopes = dict((tel, Telescope(tel)) for tel in args.tel)

    # loop over telescopes and trying to interpret sequencer
    # a goodTel is a telescope that has sequences :)
    for tel in (goodTel for goodTel in args.tel if telescopes[goodTel].sequences):
        log.info(f"Processing {tel}...")

        if not telescopes[tel].incidence.check_previous_incidences(tel):
            log.debug("No previous incidences found")

        # if len(telescopes[tel].incidence.incidencesDict["error23"]) > max_broken_report_lines_per_telescope:
        #     log.warning("Too many broken report lines! Something is wrong!")
        #     # interrupt unless closing is forced
        #     if not args.force:
        #         continue

        # loop over sequences
        for seq in telescopes[tel]:
            log.info(f"Processing sequence {seq.dictSequence['Run']}...")

            # check if LST1 and LST2 have the same nr of subruns for this sequence.
            # returns True if check is not possible...
            # became obsolete since the has_all_subruns check!
            # if not seq.has_equal_nr_of_subruns(telescopes):
            #     log.warning("LST1 and LST2 have different nrs of subruns!")
            #     continue
            #

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

            if not has_equal_nr_of_sequences(telescopes):
                log.warning("LST1 and LST2 have different nrs of sequences!")
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
        elif not args.simulate:
            check_for_output_files(analysis_path(tel))

    log.info("Exit")
