import shutil
import sys
from pathlib import Path, PurePath

from osa.configs import options
from osa.configs.config import cfg
from osa.provenance.capture import trace
from osa.provenance.io import read_prov, provlist2provdoc, provdoc2json, provdoc2graph


@trace
def r0_to_dl1(
    calibrationfile: Path,
    pedestalfile: Path,
    time_calibration: Path,
    drivefile: Path,
    runsummary: Path,
    run_str: str,
    historyfile: Path,
):
    pass


@trace
def dl1_to_dl2(
    run_str: str,
    historyfile: Path,
):
    pass


def select_config(tmp_path):
    config_file = (
        Path(__file__).resolve().parent / ".." / ".." / ".." / options.configfile
    )
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


def make_args_r0_to_dl1():

    return (
        Path("calibration.Run02006.0000.hdf5"),
        Path("drs4_pedestal.Run02005.0000.fits"),
        Path("time_calibration"),
        Path("drivefile"),
        Path("RunSummary_20200101.ecsv"),
        "02006.0002",
        Path(
            "/fefs/aswg/data/real/running_analysis/20200218/"
            "v0.4.3_v00/sequence_LST1_02006.0000.history"
        ),
    )


def make_args_dl1_to_dl2():

    return (
        "02006.0002",
        Path(
            "/fefs/aswg/data/real/running_analysis/20200218/"
            "v0.4.3_v00/sequence_LST1_02006.0000.history"
        ),
    )


def test_trace_r0_to_dl2(tmp_path):

    # config
    select_config(tmp_path)
    path_logfile = Path(__file__).resolve().parent

    args_dl1 = make_args_r0_to_dl1()
    args_dl2 = make_args_dl1_to_dl2()

    options.date = "2020_01_17"
    options.prod_id = cfg.get("LST1", "PROD_ID")
    options.calib_prod_id = cfg.get("LST1", "CALIB_PROD_ID")
    options.dl1_prod_id = cfg.get("LST1", "DL1_PROD_ID")
    options.dl2_prod_id = cfg.get("LST1", "DL2_PROD_ID")

    # track prov
    r0_to_dl1(*args_dl1)
    dl1_to_dl2(*args_dl2)

    prov_filepath = path_logfile / "prov.log"

    # make json
    json_filepath = tmp_path / "prov.json"
    provdoc = provlist2provdoc(read_prov(filename=prov_filepath))
    provdoc2json(provdoc, str(json_filepath))

    # make graph
    pdf_filepath = tmp_path / "prov.pdf"
    provdoc2graph(provdoc, str(pdf_filepath), "pdf")

    assert prov_filepath.exists()
    assert json_filepath.exists()
    assert pdf_filepath.exists()

    try:
        Path(path_logfile / "prov.log").unlink()
    except FileNotFoundError:
        pass
