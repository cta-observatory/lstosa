"""Command line argument parser for lstosa scripts."""

import datetime
import logging
from argparse import ArgumentParser
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.paths import analysis_path, DEFAULT_CFG
from osa.utils.logging import myLogger
from osa.utils.utils import (
    get_dl1_prod_id,
    get_dl2_prod_id,
    get_prod_id,
    is_defined,
    set_prod_ids,
    YESTERDAY,
)

__all__ = [
    "closer_argparser",
    "closercliparsing",
    "copy_datacheck_parsing",
    "data_sequence_cli_parsing",
    "data_sequence_argparser",
    "provprocessparsing",
    "sequencer_argparser",
    "sequencer_cli_parsing",
    "set_default_date_if_needed",
    "simprocparsing",
    "sequencer_webmaker_argparser",
    "valid_date",
    "get_prod_id",
    "get_dl1_prod_id",
    "get_dl2_prod_id",
    "calibration_pipeline_cliparsing",
    "calibration_pipeline_argparser",
    "autocloser_cli_parser",
    "common_parser",
]

log = myLogger(logging.getLogger(__name__))


def valid_date(string):
    """Check if the string is a valid date and return a datetime object."""
    return datetime.datetime.strptime(string, "%Y-%m-%d")


common_parser = ArgumentParser(add_help=False)
common_parser.add_argument(
    "-c",
    "--config",
    type=Path,
    default=DEFAULT_CFG,
    help="Use specific config file [default configs/sequencer.cfg]",
)
common_parser.add_argument(
    "-d",
    "--date",
    help="Date (YYYY-MM-DD) of the start of the night",
    type=valid_date,
)
common_parser.add_argument(
    "-s",
    "--simulate",
    action="store_true",
    default=False,
    help="Do not run, just simulate what would happen",
)
common_parser.add_argument(
    "-t",
    "--test",
    action="store_true",
    default=False,
    help="Avoid interaction with SLURM",
)
common_parser.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    default=False,
    help="Activate debugging mode",
)
# TODO: add here the tel_id common option


def closer_argparser():
    parser = ArgumentParser(parents=[common_parser])

    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        dest="noninteractive",
        default=False,
        help="assume yes to all questions",
    )
    parser.add_argument(
        "--seq",
        action="store",
        type=str,
        dest="seqtoclose",
        help="If you only want to close a certain sequence",
    )
    parser.add_argument(
        "--no-dl2",
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
    set_common_globals(opts)
    options.seqtoclose = opts.seqtoclose
    options.no_dl2 = opts.no_dl2
    options.noninteractive = opts.noninteractive

    log.debug(f"the options are {opts}")

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = analysis_path(options.tel_id)
    set_prod_ids()


def calibration_pipeline_argparser():
    """Command line parser for the calibration pipeline."""
    parser = ArgumentParser(parents=[common_parser])
    parser.add_argument(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )
    parser.add_argument("--drs4-pedestal-run", type=int, help="DRS4 pedestal run number")
    parser.add_argument("--pedcal-run", type=int, help="Calibration run number")

    parser.add_argument("tel_id", choices=["LST1"])

    return parser


def calibration_pipeline_cliparsing():
    """
    Set the global variables and parse the command
    line arguments for the calibration pipeline
    """
    opts = calibration_pipeline_argparser().parse_args()

    # set global variables
    options.configfile = opts.config
    options.date = opts.date
    options.verbose = opts.verbose
    options.prod_id = opts.prod_id
    options.tel_id = opts.tel_id
    options.simulate = opts.simulate

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = analysis_path(options.tel_id)

    return opts.drs4_pedestal_run, opts.pedcal_run


def data_sequence_argparser():
    parser = ArgumentParser(parents=[common_parser])

    parser.add_argument(
        "--prod-id",
        action="store",
        type=str,
        dest="prod_id",
        help="Set the prod ID to define data directories",
    )
    parser.add_argument(
        "--no-dl2",
        action="store_true",
        default=False,
        help="Do not produce DL2 files (default False)",
    )
    parser.add_argument("--pedcal-file", type=Path, help="Path of the calibration file")
    parser.add_argument("--drs4-pedestal-file", type=Path, help="Path of the DRS4 pedestal file")
    parser.add_argument("--time-calib-file", type=Path, help="Path of the time calibration file")
    parser.add_argument(
        "--systematic-correction-file",
        type=Path,
        help="Path of the systematic correction factor file",
    )
    parser.add_argument(
        "--drive-file", type=Path, help="Path of drive log file with pointing information"
    )
    parser.add_argument(
        "--run-summary",
        type=Path,
        help="Path of run summary file with time reference information",
    )
    parser.add_argument(
        "--pedestal-ids-file",
        type=Path,
        help="Path to a file containing the ids of the interleaved pedestal events",
    )
    parser.add_argument("run_number", help="Number of the run to be processed")
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])
    return parser


def data_sequence_cli_parsing():

    # parse the command line
    opts = data_sequence_argparser().parse_args()

    # set global variables
    options.configfile = opts.config.resolve()
    options.date = opts.date
    options.verbose = opts.verbose
    options.simulate = opts.simulate
    options.prod_id = opts.prod_id
    options.no_dl2 = opts.no_dl2
    options.tel_id = opts.tel_id

    log.debug(f"The options and arguments are {opts}")

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = analysis_path(options.tel_id)

    set_prod_ids()

    return (
        opts.pedcal_file,
        opts.drs4_pedestal_file,
        opts.time_calib_file,
        opts.systematic_correction_file,
        opts.drive_file,
        opts.run_summary,
        opts.pedestal_ids_file,
        opts.run_number,
    )


def sequencer_argparser():
    """Argument parser for sequencer script."""
    parser = ArgumentParser(
        description="Build the jobs for each run and process them for a given date",
        parents=[common_parser],
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
        "tel_id",
        choices=["ST", "LST1", "LST2", "all"],
        help="telescope identifier LST1, LST2, ST or all.",
    )

    return parser


def sequencer_cli_parsing():
    # parse the command line
    opts = sequencer_argparser().parse_args()

    # set global variables
    set_common_globals(opts)
    options.no_submit = opts.no_submit
    options.no_calib = opts.no_calib
    options.no_dl2 = opts.no_dl2

    log.debug(f"the options are {opts}")

    set_prod_ids()

    # setting the default date and directory if needed
    options.date = set_default_date_if_needed()
    options.directory = analysis_path(options.tel_id)


def provprocess_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        type=Path,
        default=DEFAULT_CFG,
        help="use specific config file [default configs/sequencer.cfg]",
    )
    parser.add_argument(
        "-f",
        "--filter",
        action="store",
        dest="filter",
        default="",
        help="filter by process granularity [calibration, r0_to_dl1 or dl1_to_dl2]",
    )
    parser.add_argument(
        "-q",
        action="store_true",
        dest="quit",
        help="use this flag to reset session and remove log file",
    )
    parser.add_argument(
        "--no-dl2",
        action="store_true",
        default=False,
        help="Do not produce DL2 files (default False)",
    )
    parser.add_argument(
        "drs4_pedestal_run_id", help="Number of the drs4_pedestal used in the calibration"
    )
    parser.add_argument("pedcal_run_id", help="Number of the used pedcal used in the calibration")
    parser.add_argument("run", help="Number of the run whose provenance is to be extracted")
    parser.add_argument("date", action="store", type=str, help="Observation starting date YYYYMMDD")
    parser.add_argument("prod_id", action="store", type=str, help="Production ID")

    return parser


def provprocessparsing():

    # parse the command line
    opts = provprocess_argparser().parse_args()

    # checking arguments
    if opts.filter not in ["calibration", "r0_to_dl1", "dl1_to_dl2", ""]:
        log.error("incorrect value for --filter argument, type -h for help")

    # set global variables
    options.drs4_pedestal_run_id = opts.drs4_pedestal_run_id
    options.pedcal_run_id = opts.pedcal_run_id
    options.run = opts.run
    options.date = opts.date
    options.configfile = opts.config.resolve()
    options.filter = opts.filter
    options.quit = opts.quit
    options.no_dl2 = opts.no_dl2
    set_prod_ids()


def simproc_argparser():
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        default=DEFAULT_CFG,
        help="use specific config file",
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
    # parser.add_argument(
    #     "-d",
    #     "--date",
    #     action="store",
    #     type=str,
    #     dest="date",
    #     help="observation ending date YYYY-MM-DD [default today]",
    # )
    # parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])

    return parser


def simprocparsing():
    opts = simproc_argparser().parse_args()

    # set global variables
    options.prod_id = get_prod_id()
    options.configfile = opts.config
    options.provenance = opts.provenance
    options.force = opts.force
    options.append = opts.append


def copy_datacheck_argparser():
    parser = ArgumentParser(parents=[common_parser])
    parser.add_argument("tel_id", choices=["ST", "LST1", "LST2"])
    return parser


def copy_datacheck_parsing():

    # parse the command line
    opts = copy_datacheck_argparser().parse_args()

    # set global variables
    options.date = opts.date
    options.tel_id = opts.tel_id
    options.configfile = opts.config
    options.date = set_default_date_if_needed()
    options.directory = analysis_path(options.tel_id)
    options.prod_id = get_prod_id()

    if cfg.get("LST1", "DL1_PROD_ID") is not None:
        options.dl1_prod_id = get_dl1_prod_id()
    else:
        options.dl1_prod_id = options.prod_id


def sequencer_webmaker_argparser():
    parser = ArgumentParser(
        description="Script to make an xhtml from LSTOSA sequencer output", parents=[common_parser]
    )
    options.tel_id = "LST1"
    options.prod_id = get_prod_id()

    return parser


def set_default_date_if_needed():
    """Check if the date is set, if not set it to yesterday."""
    return options.date if is_defined(options.date) else YESTERDAY


def set_common_globals(opts):
    """Define common global variables using options module."""
    options.configfile = opts.config.resolve()
    options.date = opts.date
    options.simulate = opts.simulate
    options.test = opts.test
    options.verbose = opts.verbose
    options.tel_id = opts.tel_id


def autocloser_cli_parser():
    """Define the command line parser for the autocloser."""
    parser = ArgumentParser(
        description="Automatic job completion check and sequence closer.", parents=[common_parser]
    )
    parser.add_argument("--ignore-cronlock", action="store_true", help='Ignore "cron.lock"')
    parser.add_argument(
        "-f", "--force", action="store_true", help="Force the autocloser to close the day"
    )
    parser.add_argument(
        "--no-dl2",
        action="store_true",
        default=False,
        help="Disregard the production of DL2 files",
    )
    parser.add_argument("-r", "--runwise", action="store_true", help="Close the day run-wise.")
    parser.add_argument("-l", "--log", type=Path, default=None, help="Write log to a file.")
    parser.add_argument("tel_id", type=str, choices=["LST1"])
    return parser
