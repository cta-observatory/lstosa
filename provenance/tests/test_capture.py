import shutil
import sys
from pathlib import Path, PurePath

from osa.utils import options
from provenance.capture import trace
from provenance.io import *


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
    historyfile,
):
    pass


def select_config(tmp_path):

    config_file = str(Path("cfg") / "sequencer.cfg")
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


def make_args_r0_to_dl1():

    args = (
        "calibration.Run2006.0000.hdf5",
        "drs4_pedestal.Run2005.0000.fits",
        "time_calibration",
        "drivefile",
        "ucts_t0_dragon",
        "dragon_counter0",
        "ucts_t0_tib",
        "tib_counter0",
        "02006.0002",
        "DL1/20200218/v0.4.3_v00/historyfile/sequence_LST1_02006.0001.history",
    )
    return args


def test_trace_r0_to_dl1(tmp_path):

    # config
    select_config(tmp_path)
    args = make_args_r0_to_dl1()

    # track prov
    r0_to_dl1(*args)

    # make json
    json_filepath = tmp_path / "prov.json"
    provdoc = provlist2provdoc(read_prov(filename="prov.log"))
    provdoc.serialize(str(json_filepath), indent=4)

    # make graph
    png_filepath = tmp_path / "prov.png"
    provdoc2png(provdoc, str(png_filepath))

    try:
        Path("prov.log").unlink()
    except FileNotFoundError:
        pass
