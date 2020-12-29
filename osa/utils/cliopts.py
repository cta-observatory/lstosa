import logging
from argparse import ArgumentParser
from optparse import OptionParser
from os.path import abspath, basename, dirname, join

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import (
    get_calib_prod_id,
    get_dl1_prod_id,
    get_dl2_prod_id,
    get_lstchain_version,
    get_prod_id,
    getcurrentdate2,
    getnightdirectory,
    is_defined,
)

log = logging.getLogger(__name__)


def closer_argparser():
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
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])

    return parser


def closercliparsing():

    # parse the command line
    opts = closer_argparser().parse_args()

    # set global variables
    options.configfile = opts.configfile
    options.stderr = opts.stderr
    options.stdout = opts.stdout
    options.date = opts.date
    options.directory = opts.directory
    options.nightsummary = opts.nightsum
    options.noninteractive = opts.noninteractive
    options.simulate = opts.simulate
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.reason = opts.reason
    options.seqtoclose = opts.seqtoclose
    options.tel_id = opts.tel_id

    log.debug(f"the options are {opts}")

    # setting the default date and directory if needed
    # options.configfile = set_default_configfile_if_needed('closer.py')
    options.configfile = set_default_configfile_if_needed("sequencer.py")
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


def pedestalsequencecliparsing(command):
    message = (
        "usage: %prog [-vw] [-c CONFIGFILE] [-d DATE] [-o OUTPUTDIR] [-z] <PED_RUN_ID> <TEL_ID>"
    )
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_option(
        "-o",
        "--outputdir",
        action="store",
        type="string",
        dest="directory",
        help="write files to output directory",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_option(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_option(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_option(
        "--stderr",
        action="store",
        type="string",
        dest="stderr",
        help="file for standard error",
    )
    parser.add_option(
        "--stdout",
        action="store",
        type="string",
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
    options.directory = opts.directory
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed

    # the standardhandle has to be declared here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # mapping the telescope argument to an option parameter (it might become an option in the future)
    if len(args) != 2:
        log.error("incorrect number of arguments, type -h for help")
    elif args[1] == "ST":
        log.error(f"not yet ready for telescope ST")
    elif args[1] not in ["LST1", "LST2"]:
        log.error("wrong telescope id, use 'LST1', 'LST2' or 'ST'")

    options.tel_id = args[1]

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed(command)

    return args


def calibrationsequencecliparsing(command):
    message = "usage: %prog [-vw] [-c CONFIGFILE] [-d DATE] [-o OUTPUTDIR] [-z] [prod-id]" \
              "<pedoutfile> <caloutfile> <CAL_RUN_ID> <PED_RUN_ID> <TEL_ID>"
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_option(
        "-o",
        "--outputdir",
        action="store",
        type="string",
        dest="directory",
        help="write files to output directory",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_option(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_option(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_option(
        "--stderr",
        action="store",
        type="string",
        dest="stderr",
        help="file for standard error",
    )
    parser.add_option(
        "--stdout",
        action="store",
        type="string",
        dest="stdout",
        help="file for standard output",
    )
    parser.add_option(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )
    # parse the command line
    (opts, args) = parser.parse_args()

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

    # the standardhandle has to be declared here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # mapping the telescope argument to an option parameter (it might become an option in the future)
    if len(args) != 5:
        log.error("incorrect number of arguments, type -h for help")
    elif args[4] == "ST":
        log.error(f"not yet ready for telescope ST")
    elif args[4] not in ["LST1", "LST2"]:
        log.error("wrong telescope id, use 'LST1', 'LST2' or 'ST'")

    options.tel_id = args[4]

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed(command)
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()
    if cfg.get("LST1", "CALIB-PROD-ID") is not None:
        options.calib_prod_id = get_calib_prod_id()
    else:
        options.calib_prod_id = options.prod_id

    return args


def datasequencecliparsing(command):
    message = "usage: %prog  [-vw] [--stderr=FILE] [--stdout=FILE]" \
              "[-c CONFIGFILE] [-d DATE] [-o OUTPUTDIR] [-z] [--prod-id]\
              <calibrationfile> <pedestalfile> <drivefile> <timecalibration>" \
              "<ucts_t0_dragon> <dragon_counter0> <ucts_t0_tib> <tib_counter>" \
              "<RUN> <TEL_ID>"
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_option(
        "-o",
        "--outputdir",
        action="store",
        type="string",
        dest="directory",
        help="analysis working directory",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_option(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_option(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_option(
        "--stderr",
        action="store",
        type="string",
        dest="stderr",
        help="file for standard error",
    )
    parser.add_option(
        "--stdout",
        action="store",
        type="string",
        dest="stdout",
        help="file for standard output",
    )
    parser.add_option(
        "-s",
        "--simulate",
        action="store_true",
        dest="simulate",
        default=False,
        help="do not submit sequences as jobs",
    )
    parser.add_option(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )

    # parse the command line
    (opts, args) = parser.parse_args()

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

    # the standardhandle has to be declared here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # checking arguments
    if len(args) != 10:
        log.error("incorrect number of argumentss, type -h for help")

    # mapping the telescope argument to an option parameter (it might become an option in the future)
    elif args[9] == "ST":
        log.error(f"not yet ready for telescope ST", 2)
    elif args[9] != "LST1" and args[9] != "LST2":
        log.error("wrong telescope id, use 'LST1', 'LST2' or 'ST'")
    options.tel_id = args[9]

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed(command)
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

    return args


def stereosequencecliparsing(command):
    message = "usage: %prog  [-vw] [--stderr=FILE] [--stdout=FILE] [-c CONFIGFILE] [-d DATE] [-o OUTPUTDIR] [-z] <RUN>"
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_option(
        "-o",
        "--outputdir",
        action="store",
        type="string",
        dest="directory",
        help="analysis output directory",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_option(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_option(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="Use input as compressed raw.gz files",
    )
    parser.add_option(
        "--stderr",
        action="store",
        type="string",
        dest="stderr",
        help="file for standard error",
    )
    parser.add_option(
        "--stdout",
        action="store",
        type="string",
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
    options.directory = opts.directory
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed

    # the standardhandle has to be declared here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # checking arguments
    if len(args) != 1:
        log.error("incorrect number of arguments, type -h for help")

    # mapping the telescope argument to an option parameter (it might become an option in the future)
    options.tel_id = "ST"

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed(command)
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()

    return args


def sequencer_argparser():
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
        choices=["P", "S", "T"],
        help="mode to run dependant sequences:\n P=parallel [default], S=Sequential, T=temperature-aware",
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
        dest="simulate",
        default=False,
        help="do not submit sequences as jobs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        dest="test",
        default=False,
        help="test LSTOSA locally outside the CTA-N IT container",
    )
    parser.add_argument(
        "--nocalib",
        action="store_true",
        dest="nocalib",
        default=False,
        help="Skip calibration sequence. Run only data sequences",
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
        help="Use input as compressed raw.gz files, compulsory if using -n and raw.gz files",
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


def sequencercliparsing():

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
    options.nocalib = opts.nocalib
    options.verbose = opts.verbose
    options.warning = opts.warning
    options.compressed = opts.compressed
    options.tel_id = opts.tel_id

    # the standardhandle has to be declared before here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")

    # set the default value for mode
    if not opts.mode:
        options.mode = "P"

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed("sequencer.py")
    options.date = set_default_date_if_needed()


def rawcopycliparsing(command):
    message = (
        "usage: %prog [-vw] [--stderr=FILE] [--stdout=FILE] [-c CONFIGFILE] [-d DATE] [-z] <TEL_ID>"
    )
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default=None,
        help="use specific config file [default rawcopy.cfg]",
    )
    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        dest="date",
        help="observation ending date YYYY_MM_DD [default today]",
    )
    parser.add_option(
        "--nocheck",
        action="store_true",
        dest="nocheck",
        default=False,
        help="Skip checking if the daily activity is set over",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="make lots of noise for debugging",
    )
    parser.add_option(
        "-w",
        "--warnings",
        action="store_true",
        dest="warning",
        default=False,
        help="show useful warnings",
    )
    parser.add_option(
        "-z",
        "--rawzip",
        action="store_true",
        dest="compressed",
        default=False,
        help="compress output into raw.gz files",
    )
    parser.add_option(
        "--stderr",
        action="store",
        type="string",
        dest="stderr",
        help="file for standard error",
    )
    parser.add_option(
        "--stdout",
        action="store",
        type="string",
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

    # the standardhandle has to be declared here, since verbose and warnings are options from the cli
    log.debug(f"the options are {opts}")
    log.debug(f"the argument is {args}")

    # mapping the telescope argument to an option parameter (it might become an option in the future)
    if len(args) != 1:
        log.error("incorrect number of arguments, type -h for help")
    elif args[0] == "ST":
        log.error(f"not yet ready for telescope ST")
    elif args[0] not in ["LST1", "LST2"]:
        log.error("wrong telescope id, use 'LST1', 'LST2' or 'ST'")
    options.tel_id = args[0]

    # setting the default date and directory if needed
    options.configfile = set_default_configfile_if_needed(command)
    options.date = set_default_date_if_needed()

    return args


def provprocessparsing():
    message = "usage: %prog [-c CONFIGFILE] [-f PROCESS] <RUN_NUMBER> <DATEFOLDER> <SUBFOLDER>"
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option(
        "-f",
        "--filter",
        action="store",
        dest="filter",
        default="",
        help="filter by process granularity [r0_to_dl1 or dl1_to_dl2]",
    )
    parser.add_option(
        "-q",
        action="store_true",
        dest="quit",
        help="use this flag to reset session and remove log file",
    )

    # parse the command line
    (opts, args) = parser.parse_args()

    # checking arguments
    if len(args) != 3:
        log.error("incorrect number of arguments, type -h for help")
    if opts.filter not in ["r0_to_dl1", "dl1_to_dl2", ""]:
        log.error("incorrect value for --filter argument, type -h for help")

    # set global variables
    options.run = args[0]
    options.date = args[1]
    options.prod_id = args[2]
    options.configfile = opts.configfile
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


def simprocparsing():
    message = (
        "Usage: %prog [-c CONFIGFILE] [-p] [--force] [--append] <YYYY_MM_DD> <vX.X.X_vXX> <TEL_ID>\n"
        "Run script from OSA root folder.\n\n"
        "Arguments:\n"
        "<YYYY_MM_DD> date analysis folder name for derived datasets\n"
        "<vX.X.X_vXX> software version and prod subfolder name\n"
        "<TEL_ID>     telescope ID (i.e. LST1, ST,..)\n"
    )
    parser = OptionParser(usage=message)
    parser.add_option(
        "-c",
        "--config",
        action="store",
        dest="configfile",
        default="cfg/sequencer.cfg",
        help="use specific config file [default cfg/sequencer.cfg]",
    )
    parser.add_option("-p", action="store_true", dest="provenance", help="produce provenance files")
    parser.add_option(
        "--force",
        action="store_true",
        dest="force",
        help="force overwrite provenance files",
    )
    parser.add_option(
        "--append",
        action="store_true",
        dest="append",
        help="append provenance capture to existing prov.log file",
    )

    # parse the command line
    (opts, args) = parser.parse_args()

    # checking arguments
    if len(args) != 3:
        log.error("incorrect number of arguments, type -h for help")

    # set global variables
    options.date = args[0]
    options.prod_id = args[1]
    options.tel_id = args[2]
    options.configfile = opts.configfile
    options.provenance = opts.provenance
    options.force = opts.force
    options.append = opts.append


def copy_datacheck_parsing():
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
    parser.add_argument(
        "tel_id",
        choices=["ST", "LST1", "LST2", "all"],
        help="telescope identifier LST1, LST2, ST or all.",
    )

    # parse the command line
    opts = parser.parse_args()

    # set global variables
    options.date = opts.date
    options.tel_id = opts.tel_id
    options.configfile = opts.configfile

    options.configfile = set_default_configfile_if_needed("sequencer.py")
    options.date = set_default_date_if_needed()
    options.directory = set_default_directory_if_needed()
    options.prod_id = get_prod_id()


def set_default_date_if_needed():
    if is_defined(options.date):
        return options.date
    else:
        return getcurrentdate2(cfg.get("LST", "DATESEPARATOR"))


def set_default_directory_if_needed():
    if is_defined(options.directory):
        return options.directory
    else:
        return getnightdirectory()


def set_default_configfile_if_needed(command):

    # the default config will be the name of the program, with suffix .cfg
    # and present in the cfg subdir, trivial, isn't it?

    log.debug(f"Command is {command}")
    command_dirname = dirname(abspath(command))
    command_basename = basename(command)

    if not options.configfile:
        config_basename = command_basename.replace(".py", ".cfg")
        options.configfile = join(command_dirname, "cfg", config_basename)
    else:
        # relative to absolute path conversion
        options.configfile = join(command_dirname, options.configfile)

    log.debug(f"Setting default config file to {options.configfile}")
    return options.configfile
