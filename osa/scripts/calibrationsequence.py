import subprocess
import sys
from os.path import join

from osa.configs.config import cfg
from osa.jobs.job import historylevel
from osa.reports.report import history
from osa.configs import options
from osa.utils.cliopts import calibrationsequencecliparsing
from osa.utils.standardhandle import error, gettag, stringify, verbose
from osa.utils.utils import lstdate_to_dir


def calibrationsequence(args):

    # this is the python interface to run sorcerer with the -c option for calibration
    # args: <RUN>
    pedestal_output_file = args[0]
    calibration_output_file = args[1]
    run_ped = args[2]
    run_cal = args[3]

    historyfile = join(options.directory, f"sequence_{options.tel_id }_{run_cal}.history")
    level, rc = historylevel(historyfile, "CALIBRATION")
    verbose(tag, f"Going to level {level}")
    print("historyfile:", historyfile, run_ped)
    print("PEDESTAL directory:", options.directory, options.tel_id)
    print("level & rc:", level, rc)
    # sys.exit()
    if level == 2:
        rc = drs4_pedestal(run_ped, pedestal_output_file, historyfile)
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 1:
        rc = calibrate(run_cal, pedestal_output_file, calibration_output_file, historyfile)
        level -= 1
        verbose(tag, f"Going to level {level}")
    if level == 0:
        verbose(tag, f"Job for sequence {run_ped} finished without fatal errors")
    return rc


def drs4_pedestal(run_ped, pedestal_output_file, historyfile):

    # sequencetextfile = join(options.directory, "sequence_" + options.tel_id + "_" + run_ped + ".txt")
    # bindir = cfg.get("LSTOSA", "LSTCHAINDIR")
    # daqdir = cfg.get(options.tel_id, "RAWDIR")
    # carddir = cfg.get('LSTOSA', 'CARDDIR')
    # configcard = join(carddir, inputcard)
    inputcard = cfg.get(options.tel_id, "CALIBRATIONCONFIGCARD")
    nightdir = lstdate_to_dir(options.date)
    input_file = join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{run_ped}.0000{cfg.get("LSTOSA", "R0SUFFIX")}',
    )
    max_events = cfg.get("LSTOSA", "MAX_PED_EVENTS")
    commandargs = [
        cfg.get("PROGRAM", "PEDESTAL"),
        "--input-file=" + input_file,
        "--output-file=" + pedestal_output_file,
        "--max-events=" + max_events,
    ]
    commandconcept = "drs4_pedestal"
    # pedestalfile = "drs4_pedestal"
    print("COMMAND for pedestal:", commandargs)

    # error handling, for now no nonfatal errors are implemented for CALIBRATION
    try:
        verbose(tag, f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    # except OSError as (ValueError, NameError):
    except OSError as ValueError:
        history(run_ped, commandconcept, pedestal_output_file, inputcard, ValueError, historyfile)
        error(tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(run_ped, commandconcept, pedestal_output_file, inputcard, rc, historyfile)

    if rc != 0:
        sys.exit(rc)
    return rc


def calibrate(calibration_run_id, pedestal_file, calibration_output_file, historyfile):

    # sequencetextfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run + '.txt')
    # bindir = cfg.get("LSTOSA", "LSTCHAINDIR")
    # daqdir = cfg.get(options.tel_id, "RAWDIR")
    # carddir = cfg.get('LSTOSA', 'CARDDIR')
    # configcard = join(carddir, inputcard)
    inputcard = cfg.get(options.tel_id, "CALIBRATIONCONFIGCARD")
    nightdir = lstdate_to_dir(options.date)
    calibration_data_file = join(
        cfg.get("LST1", "RAWDIR"),
        nightdir,
        f'{cfg.get("LSTOSA", "R0PREFIX")}.Run{calibration_run_id}.0000{cfg.get("LSTOSA", "R0SUFFIX")}',
    )
    calib_config_file = cfg.get("LSTOSA", "CALIBCONFIGFILE")
    flat_field_sample_size = cfg.get("LSTOSA", "FLATFIELDCALCULATORSAMPLESIZE")
    pedestal_cal_sample_size = cfg.get("LSTOSA", "PEDESTALCALCULATORSAMPLESIZE")
    event_source_max_events = cfg.get("LSTOSA", "EVENTSOURCEMAXEVENTS")
    # calibration_version = cfg.get('LSTOSA', 'CALIBRATION_VERSION')  #def: 0
    # n_events_statistics = cfg.get('LSTOSA', 'STATISTICS')  #def: 10000
    # calibration_base_dir = cfg.get('LSTOSA', 'CALIB_BASE_DIRECTORY')  #def: '/fefs/aswg/data/real'
    # calculate_time_run = cfg.get('LSTOSA', 'CALCULATE_TIME_RUN')  #def: '1625'
    commandargs = [
        cfg.get("PROGRAM", "CALIBRATION"),
        "--input_file=" + calibration_data_file,
        "--output_file=" + calibration_output_file,
        "--pedestal_file=" + pedestal_file,
        "--FlatFieldCalculator.sample_size=" + flat_field_sample_size,
        "--PedestalCalculator.sample_size=" + pedestal_cal_sample_size,
        "--EventSource.max_events=" + event_source_max_events,
        "--config=" + calib_config_file,
    ]
    # FIXME: Include time calibration!
    # optional.add_argument('--ff_calibration', help="Perform the charge calibration (yes/no)",type=str, default='yes')
    # optional.add_argument('--tel_id', help="telescope id. Default = 1", type=int, default=1)
    commandconcept = "calibration"
    calibrationfile = "new_calib"
    print("COMAND for calib:", commandargs)

    # error handling, for now no nonfatal errors are implemented for CALIBRATION
    try:
        verbose(tag, f"Executing {stringify(commandargs)}")
        rc = subprocess.call(commandargs)
    # except OSError as (ValueError, NameError):
    except OSError as ValueError:
        history(calibration_run_id, commandconcept, calibrationfile, inputcard, ValueError, historyfile)
        error(tag, f"Could not execute {stringify(commandargs)}, {ValueError}", ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(calibration_run_id, commandconcept, calibrationfile, inputcard, rc, historyfile)

    if rc != 0:
        sys.exit(rc)
    return rc


if __name__ == "__main__":

    tag = gettag()
    # set the options through cli parsing
    args = calibrationsequencecliparsing(sys.argv[0])
    # run the routine
    rc = calibrationsequence(args)
    sys.exit(rc)
