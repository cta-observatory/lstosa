from optparse import OptionParser
from ..capture import trace
import shutil
import sys
from osa.utils import options
from pathlib import Path, PurePath


@trace
def r0_to_dl1(
        calibrationfile,
        pedestalfile,
        time_calibration,
        drivefile,
        ucts_t0_dragon,
        dragon_counter0,
        ucts_t0_tib,
        tib_counter0,
        run_str,
        historyfile
):
    pass


def select_config(tmp_path):
    
    config_file = str(Path("cfg")/"sequencer.cfg")
    in_config_arg = False
    for args in sys.argv:
        if in_config_arg:
            config_file = args
            in_config_arg = False
        if args.startswith("-c") or args.startswith("--config"):
            in_config_arg = True

    config_filename = PurePath(config_file).name
    temp_config_path = tmp_path / config_filename
    shutil.copy(config_file, str(temp_config_path))
    options.configfile = str(temp_config_path)



    options.configfile = "cfg/sequencer.cfg"
    args = (
    "/Users/jer/fefs/aswg/data/real/calibration/20200218/v00/calibration.Run2006.0000.hdf5",
    "/Users/jer/fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run2005.0000.fits",
    "/Users/jer/fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run1625.0000.hdf5",
    "/Users/jer/fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt",
    "ucts_t0_dragon",
    "dragon_counter0",
    "ucts_t0_tib",
    "tib_counter0",
    "02006.0002",
    "/Users/jer/fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/sequence_LST1_02006.0001.history"
    )
def test_trace_r0_to_dl1(tmp_path):

    # config
    select_config(tmp_path)
    # track prov
    r0_to_dl1(*args)
