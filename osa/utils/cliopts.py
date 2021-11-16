"""Command line argument parser for lstosa scripts."""

import datetime
import logging
import os
from argparse import ArgumentParser, ArgumentTypeError

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import (
    get_calib_prod_id,
    get_dl1_prod_id,
    get_dl2_prod_id,
    get_lstchain_version,
    get_prod_id,
    getcurrentdate,
    night_directory,
    is_defined,
)

__all__ = [
    "calibrationsequencecliparsing",
    "closer_argparser",
    "closercliparsing",
    "copy_datacheck_parsing",
    "datasequencecliparsing",
    "provprocessparsing",
    "rawcopycliparsing",
    "sequencer_argparser",
    "sequencer_cli_parsing",
    "set_default_date_if_needed",
    "set_default_directory_if_needed",
    "simprocparsing",
    "sequencer_webmaker_argparser",
    "valid_date",
]

log = logging.getLogger(__name__)


def closer_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="./cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "-n",
        "--usenightsummary",
        action="store_true",
        dest="nightsum",
        default=False,
        help="rely on existing nightsumary file",
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        action="store",
        type=str,
        dest="directory",
        help="analysis output directory",
    )
    parser.add_argument(
        "-r",
        "--reason",
        action="store",
        type=str,
        dest="reason",
        choices=["moon", "weather", "other"],
        help="reason for closing without data: (moon, weather, other)",
    )
    parser.add_argument(
        "-s",
        "--simulate",
        action="store_true",
        dest="simulate",
        default=False,
        help="do not run, just show what would happen",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        dest="test",
        default=False,
        help="Avoiding interaction with SLURM",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        dest="noninteractive",
        default=False,
        help="assume yes to all questions",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_argument(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_argument(
        "--stderr",
        action="store",
        type=str,
        dest="stderr",
        help="file for standard error",
    )
    parser.add_argument(
        "--stdout",
        action="store",
        type=str,
        dest="stdout",
        help="file for standard output",
    )
    parser.add_argument(
        "--seq",
        action="store",
        type=str,
        dest="seqtoclose",
        help="If you only want to close a certain sequence",
    )
    parser.add_argument(
        "--nodl2",
        action="store_true",
        default=False,
        help="Do not produce DL2 files (default False)",
    )
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])

    return parser


def closercliparsing():
    # parse the command line
    opts = closer_argparser().parse_args()

    # set global variables
    options.configfile = os.path.abspath(opts.configfile)
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.directory = opts.directory
    options.nightsummary = opts.nightsum
    options.noninteractive = opts.noninteractive
    options.simulate = opts.simulate
    options.test = opts.test
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.reason = opts.reason
    options.seqtoclose = opts.seqtoclose
    options.tel_id = opts.tel_id
    options.nodl2 = opts.nodl2

    log.debug(f"the options are {opts}")

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()

    # setting on the usage of night summary
    options.nightsummary = True

    options.prod_id = get_prod_id()

    if cfg.get("LST1", "CALIB-PROD-ID") is not None:
        options.calib_prod_id = get_calib_prod_id()
    else:
        options.calib_prod_id = options.prod_id

    if cfg.get("LST1", "DL1-PROD-ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id

    if cfg.get("LST1", "DL2-PROD-ID") is not None:
        options.dl2_prod_id = get_dl2_prod_id()
    else:
        options.dl2_prod_id = options.prod_id


def calibrationsequence_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        action="store",
        type=str,
        dest="directory",
        help="write files to output directory",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_argument(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_argument(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_argument(
        "--stderr",
        action="store",
        type=str,
        dest="stderr",
        help="file for standard error",
    )
    parser.add_argument(
        "--stdout",
        action="store",
        type=str,
        dest="stdout",
        help="file for standard output",
    )
    parser.add_argument(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )
    parser.add_argument(
        "-s",
        "--simulate",
        action="store_true",
        dest="simulate",
        default=False,
        help="do not submit sequences as jobs",
    )
    parser.add_argument(
        "pedoutfile", help="Full path of the DRS4 pedestal file to be created"
    )
    parser.add_argument(
        "caloutfile", help="Full path of the calibration file to be created"
    )
    parser.add_argument("calib_run_number", help="Calibration run number")
    parser.add_argument("ped_run_number", help="DRS4 pedestal run number")
    parser.add_argument("run_summary_file", help="Run summary file")
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])

    return parser


def calibrationsequencecliparsing():
    opts = calibrationsequence_argparser().parse_args()

    # set global variables
    options.configfile = opts.configfile
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.directory = opts.directory
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed
    options.prod_id = opts.prod_id
    options.tel_id = opts.tel_id
    options.simulate = opts.simulate

    log.debug(f"the options and arguments are {opts}")

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()
    if cfg.get("LST1", "CALIB-PROD-ID") is not None:
        options.calib_prod_id = get_calib_prod_id()
    else:
        options.calib_prod_id = options.prod_id
    return (
        opts.pedoutfile,
        opts.caloutfile,
        opts.calib_run_number,
        opts.ped_run_number,
        opts.run_summary_file,
    )


def datasequence_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        action="store",
        type=str,
        dest="directory",
        help="analysis working directory",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_argument(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_argument(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_argument(
        "--stderr",
        action="store",
        type=str,
        dest="stderr",
        help="file for standard error",
    )
    parser.add_argument(
        "--stdout",
        action="store",
        type=str,
        dest="stdout",
        help="file for standard output",
    )
    parser.add_argument(
        "-s",
        "--simulate",
        action="store_true",
        dest="simulate",
        default=False,
        help="do not submit sequences as jobs",
    )
    parser.add_argument(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )
    parser.add_argument(
        "--nodl2",
        action="store_true",
        default=False,
        help="Do not produce DL2 files (default False)",
    )
    parser.add_argument("calib_file", help="Path of the calibration file")
    parser.add_argument("drs4_ped_file", help="Path of the DRS4 pedestal file")
    parser.add_argument("time_calib_file", help="Path of the time calibration file")
    parser.add_argument(
        "drive_log_file", help="Path of drive log file with pointing information"
    )
    parser.add_argument(
        "run_summary_file",
        help="Path of run summary file with time reference information",
    )
    parser.add_argument("run_number", help="Number of the run to be processed")
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])
    return parser


def datasequencecliparsing():

    # parse the command line
    opts = datasequence_argparser().parse_args()

    # set global variables
    options.configfile = opts.configfile
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.directory = opts.directory
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed
    options.simulate = opts.simulate
    options.prod_id = opts.prod_id
    options.nodl2 = opts.nodl2
    options.tel_id = opts.tel_id

    log.debug(f"The options and arguments are {opts}")

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()

    if cfg.get("LST1", "CALIB-PROD-ID") is not None:
        options.calib_prod_id = get_calib_prod_id()
    else:
        options.calib_prod_id = options.prod_id

    if cfg.get("LST1", "DL1-PROD-ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id

    if cfg.get("LST1", "DL2-PROD-ID") is not None:
        options.dl2_prod_id = get_dl2_prod_id()
    else:
        options.dl2_prod_id = options.prod_id

    options.lstchain_version = get_lstchain_version()

    return (
        opts.calib_file,
        opts.drs4_ped_file,
        opts.time_calib_file,
        opts.drive_log_file,
        opts.run_summary_file,
        opts.run_number,
    )


def sequencer_argparser():
    """Argument parser for sequencer script."""
    parser = ArgumentParser()
    # options which define variables
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "-m",
        "--mode",
        action="store",
        type=str,
        dest="mode",
        choices=["P"],
        help="[Deprecated] mode to run dependant sequences:\n P=parallel [default]",
    )
    # boolean options
    parser.add_argument(
        "-n",
        "--usenightsummary",
        action="store_true",
        dest="nightsum",
        default=False,
        help="rely on existing nightsumary file",
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        action="store",
        type=str,
        dest="directory",
        help="analysis output directory",
    )
    parser.add_argument(
        "-s",
        "--simulate",
        action="store_true",
        default=False,
        help="do not submit sequences as jobs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        default=False,
        help="Test locally avoiding interaction with job scheduler",
    )
    parser.add_argument(
        "--no-submit",
        action="store_true",
        default=False,
        help="Produce job files but do not submit them",
    )
    parser.add_argument(
        "--no-calib",
        action="store_true",
        default=False,
        help="Skip calibration sequence. Run data sequences assuming "
             "calibration products already produced (default False)",
    )
    parser.add_argument(
        "--no-dl2",
        action="store_true",
        default=False,
        help="Do not produce DL2 files (default False)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Increase verbosity for debugging",
    )
    parser.add_argument(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_argument(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="[Deprecated] Use input as compressed raw.gz files",
    )
    parser.add_argument(
        "--stderr",
        action="store",
        type=str,
        dest="stderr",
        help="file for standard error",
    )
    parser.add_argument(
        "--stdout",
        action="store",
        type=str,
        dest="stdout",
        help="file for standard output",
    )
    parser.add_argument(
        "tel_id",
        choices=["ST", "LST1", "LST2", "all"],
        help="telescope identifier LST1, LST2, ST or all.",
    )

    return parser


def sequencer_cli_parsing():
    # parse the command line
    opts = sequencer_argparser().parse_args()

    # set global variables
    options.configfile = opts.configfile
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.directory = opts.directory
    options.mode = opts.mode
    options.nightsummary = opts.nightsum
    options.simulate = opts.simulate
    options.test = opts.test
    options.no_submit = opts.no_submit
    options.no_calib = opts.no_calib
    options.no_dl2 = opts.no_dl2
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed
    options.tel_id = opts.tel_id

    # the standardhandle has to be declared before here,
    # since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")

    # set the default value for mode
    if not opts.mode:
        options.mode = "P"

    options.prod_id = get_prod_id()

    if cfg.get("LST1", "CALIB-PROD-ID") is not None:
        options.calib_prod_id = get_calib_prod_id()
    else:
        options.calib_prod_id = options.prod_id

    if cfg.get("LST1", "DL1-PROD-ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id

    if cfg.get("LST1", "DL2-PROD-ID") is not None:
        options.dl2_prod_id = get_dl2_prod_id()
    else:
        options.dl2_prod_id = options.prod_id

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()


def rawcopycliparsing():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default rawcopy.cfg]",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "--nocheck",
        action="store_true",
        dest="nocheck",
        default=False,
        help="Skip checking if the daily activity is set over",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_argument(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_argument(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="compress output into raw.gz files",
    )
    parser.add_argument(
        "--stderr",
        action="store",
        type=str,
        dest="stderr",
        help="file for standard error",
    )
    parser.add_argument(
        "--stdout",
        action="store",
        type=str,
        dest="stdout",
        help="file for standard output",
    )

    # parse the command line
    (opts, args) = parser.parse_args()

    # set global variables
    options.configfile = opts.configfile
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.nocheck = opts.nocheck
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed

    # the standardhandle has to be declared here,
    # since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # mapping the telescope argument to an option
    # parameter (it might become an option in the future)
    if len(args) != 1:
        log.error("incorrect number of arguments, type -h for help")
    elif args[0] == "ST":
        log.error("not yet ready for telescope ST")
    elif args[0] not in ["LST1", "LST2"]:
        log.error("wrong telescope id, use 'LST1', 'LST2' or 'ST'")
    options.tel_id = args[0]

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()

    return args


def provprocess_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-f",
        "--filter",
        action="store",
        dest="filter",
        default="",
        help="filter by process granularity [r0_to_dl1 or dl1_to_dl2]",
    )
    parser.add_argument(
        "-q",
        action="store_true",
        dest="quit",
        help="use this flag to reset session and remove log file",
    )
    parser.add_argument(
        "run", help="Number of the run whose provenance is to be extracted"
    )
    parser.add_argument(
        "date", action="store", type=str, help="Observation starting date YYYYMMDD"
    )
    parser.add_argument("prod_id", action="store", type=str, help="Production ID")

    return parser


def provprocessparsing():

    # parse the command line
    opts = provprocess_argparser().parse_args()

    # checking arguments
    if opts.filter not in ["r0_to_dl1", "dl1_to_dl2", ""]:
        log.error("incorrect value for --filter argument, type -h for help")

    # set global variables
    options.run = opts.run
    options.date = opts.date
    options.prod_id = get_prod_id()
    options.configfile = os.path.abspath(opts.configfile)
    options.filter = opts.filter
    options.quit = opts.quit
    options.lstchain_version = get_lstchain_version()

    if cfg.get("LST1", "DL1-PROD-ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id

    if cfg.get("LST1", "DL2-PROD-ID") is not None:
        options.dl2_prod_id = get_dl2_prod_id()
    else:
        options.dl2_prod_id = options.prod_id


def simproc_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument(
        "-p", action="store_true", dest="provenance", help="produce provenance files"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        dest="force",
        help="force overwrite provenance files",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        dest="append",
        help="append provenance capture to existing prov.log file",
    )
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])

    return parser


def simprocparsing():
    opts = simproc_argparser().parse_args()

    # set global variables
    options.prod_id = get_prod_id()
    options.tel_id = opts.tel_id
    options.date = opts.date
    options.configfile = opts.configfile
    options.provenance = opts.provenance
    options.force = opts.force
    options.append = opts.append


def copy_datacheck_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--date",
        action="store",
        type=str,
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])
    return parser


def copy_datacheck_parsing():

    # parse the command line
    opts = copy_datacheck_argparser().parse_args()

    # set global variables
    options.date = opts.date
    options.tel_id = opts.tel_id
    options.configfile = opts.configfile
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()
    options.prod_id = get_prod_id()

    if cfg.get("LST1", "DL1-PROD-ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y_%m_%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'."
        raise ArgumentTypeError(msg)


def sequencer_webmaker_argparser():
    parser = ArgumentParser(
        description="Script to make an xhtml from LSTOSA sequencer output"
    )
    parser.add_argument("-d", "--date", help="Date - format YYYY_MM_DD", type=valid_date)
    parser.add_argument(
        "-c",
        "--config-file",
        dest="osa_config_file",
        default="cfg/sequencer.cfg",
        help="OSA config file.",
    )
    options.tel_id = "LST1"
    options.prod_id = get_prod_id()

    return parser


def set_default_date_if_needed():
    if is_defined(options.date):
        return options.date

    return getcurrentdate(cfg.get("LST", "DATESEPARATOR"))


def set_default_directory_if_needed():
    if is_defined(options.directory):
        return options.directory

    return night_directory()
