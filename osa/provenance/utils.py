"""Utility functions for OSA pipeline provenance."""

import logging
import os
import re
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import lstdate_to_dir

__all__ = ["parse_variables", "get_log_config", "store_conda_env_export"]


def parse_variables(class_instance):
    """Parse variables needed in model"""
    # datasequence.py
    # -c cfg/sequencer.cfg
    # -d 2020_02_18
    # -o /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/
    # --prod-id v0.4.3_v00
    # /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run02006.0000.hdf5
    # /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run02005.0000.fits
    # /fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run01625.0000.hdf5
    # /fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt
    # /fefs/aswg/data/real/monitoring/RunSummary/RunSummary_20200101.ecsv
    # 02006.0000
    # LST1

    configfileDL1 = cfg.get("lstchain", "dl1ab_config")
    configfileDL2 = cfg.get("lstchain", "dl2_config")
    rawdir = cfg.get("LST1", "R0_DIR")
    rf_models_directory = cfg.get("lstchain", "RF_MODELS")
    calib_dir = cfg.get("LST1", "CALIB_DIR")
    dl1_dir = cfg.get("LST1", "DL1_DIR")
    dl2_dir = cfg.get("LST1", "DL2_DIR")
    nightdir = lstdate_to_dir(options.date)
    analysis_dir = Path(cfg.get("LST1", "ANALYSIS_DIR")) / nightdir / options.prod_id
    outdir_dl1 = Path(dl1_dir) / nightdir / options.prod_id / options.dl1_prod_id
    outdir_dl2 = Path(dl2_dir) / nightdir / options.prod_id / options.dl2_prod_id

    if class_instance.__name__ == "r0_to_dl1":
        # calibrationfile   [0] /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run02006.0000.hdf5
        # pedestalfile      [1] /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run02005.0000.fits
        # time_calibration  [2] /fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run01625.0000.hdf5
        # drivefile         [3] /fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt
        # runsummary_file   [4] /fefs/aswg/data/real/monitoring/RunSummary/RunSummary_20200101.ecsv
        # run_str           [5] 02006.0000
        # historyfile       [6] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.history

        class_instance.ObservationRun = class_instance.args[5].split(".")[0]
        class_instance.ObservationDate = nightdir
        class_instance.SoftwareVersion = options.lstchain_version
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = options.configfile

        calibration_filename = os.path.basename(class_instance.args[0])
        pedestal_filename = os.path.basename(class_instance.args[1])
        timecalibration_filename = os.path.basename(class_instance.args[2])
        calibration_path = Path(calib_dir) / nightdir / options.calib_prod_id
        class_instance.R0SubrunDataset = (
            f"{rawdir}/{nightdir}/LST-1.1.Run{class_instance.args[5]}.fits.fz"
        )
        class_instance.CoefficientsCalibrationFile = str(
            calibration_path / calibration_filename
        )
        class_instance.PedestalFile = str(
            calibration_path / pedestal_filename
        )
        class_instance.TimeCalibrationFile = str(
            calibration_path / timecalibration_filename
        )
        class_instance.PointingFile = class_instance.args[3]
        class_instance.RunSummaryFile = os.path.basename(class_instance.args[4])
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[5]}.h5"
        )

    if class_instance.__name__ == "dl1ab":
        # run_str       [0] 02006.0000
        # historyfile   [1] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.txt

        class_instance.AnalysisConfigFileDL1 = configfileDL1
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.ObservationDate = nightdir
        class_instance.SoftwareVersion = options.lstchain_version
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = options.configfile

        class_instance.PedestalCleaning = "True"
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")

        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )

    if class_instance.__name__ == "dl1_datacheck":
        # run_str       [0] 02006.0000
        # historyfile   [1] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.txt

        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.ObservationDate = nightdir
        class_instance.SoftwareVersion = options.lstchain_version
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = options.configfile

        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/tailcut84/dl1_LST-1.Run02006.0001.h5
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/muons_LST-1.Run02006.0001.fits
        class_instance.MuonsSubrunDataset = (
            f"{analysis_dir}/muons_LST-1.Run{class_instance.args[0]}.fits"
        )
        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/tailcut84/datacheck_dl1_LST-1.Run06269.0021.h5
        class_instance.DL1CheckSubrunDataset = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        # /fefs/aswg/data/real/DL1/20210913/v0.7.5/tailcut84/datacheck_dl1_LST-1.Run06269.h5
        class_instance.DL1CheckHDF5File = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.h5"
        )
        # /fefs/aswg/data/real/DL1/20210913/v0.7.5/tailcut84/datacheck_dl1_LST-1.Run06269.pdf
        class_instance.DL1CheckPDFFile = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.pdf"
        )

    if class_instance.__name__ == "dl1_to_dl2":
        # run_str       [0] 02006.0000
        # historyfile   [1] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.txt

        class_instance.AnalysisConfigFileDL2 = configfileDL2
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.ObservationDate = nightdir
        class_instance.SoftwareVersion = options.lstchain_version
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = options.configfile

        class_instance.RFModelEnergyFile = str(Path(rf_models_directory) / "reg_energy.sav")
        class_instance.RFModelDispFile = str(Path(rf_models_directory) / "reg_disp_norm.sav")
        class_instance.RFModelGammanessFile = str(Path(rf_models_directory) / "cls_gh.sav")
        
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL2SubrunDataset = (
            f"{outdir_dl2}/dl2_LST-1.Run{class_instance.args[0]}.h5"
        )
        # /fefs/aswg/data/real/DL2/20200218/v0.4.3_v00/tailcut84/dl2_LST-1.Run02006.h5
        class_instance.DL2MergedFile = f"{outdir_dl2}/dl2_LST-1.Run{class_instance.ObservationRun}.h5"

    return class_instance


def get_log_config():
    """Get logging configuration from an OSA config file."""
    # Default config filename value
    config_file = Path(__file__).resolve().parent / ".." / ".." / options.configfile
    std_logger_file = Path(__file__).resolve().parent / "config" / "logger.yaml"

    # fetch config filename value from args
    in_config_arg = False
    for arg in sys.argv:
        if in_config_arg:
            config_file = arg
            in_config_arg = False
        if arg in ["-c", "--config"]:
            in_config_arg = True

    # parse configuration
    log_config = ""
    in_prov_section = False
    str_path_tests = str(Path(__file__).resolve().parent / "tests" / "prov.log")
    try:
        with open(config_file, "r") as f:
            for line in f.readlines():
                if "pytest" in sys.modules and in_prov_section:
                    line = re.sub(r"filename:(.*)$", f"filename: {str_path_tests}", line)
                if in_prov_section:
                    log_config += line
                if "[PROVENANCE]" in line:
                    in_prov_section = True
    except FileNotFoundError:
        log = logging.getLogger(__name__)
        log.warning(f"{config_file} not found, using {std_logger_file} instead.")

    # use default logger.yaml if no prov config info found
    if log_config == "":
        log_config = std_logger_file.read_text()

    return log_config


def store_conda_env_export():
    """Store file with `conda env export` output to log the packages versions used."""
    analysis_log_dir = Path(options.directory) / "log"
    analysis_log_dir.mkdir(parents=True, exist_ok=True)
    conda_env_file = analysis_log_dir / "conda_env.yml"
    subprocess.run(
        ["conda", "env", "export", "--file", str(conda_env_file)],
        check=True
    )
