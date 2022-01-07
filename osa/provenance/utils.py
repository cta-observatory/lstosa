"""Utility functions for OSA pipeline provenance."""

import logging
import os
import re
import subprocess
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.job import get_time_calibration_file
from osa.utils.utils import get_lstchain_version, lstdate_to_dir

__all__ = ["parse_variables", "get_log_config", "store_conda_env_export"]

REDUCTION_TASKS = ["r0_to_dl1", "dl1ab", "dl1_datacheck", "dl1_to_dl2"]


def parse_variables(class_instance):
    """Parse variables needed in model"""

    # calibration_pipeline.py
    # -c cfg/sequencer.cfg
    # -d 2020_02_18
    # --drs4-pedestal-run 01804
    # --pedcal-run 01805
    # LST1

    # datasequence.py
    # -c cfg/sequencer.cfg
    # -d 2020_02_18
    # --prod-id v0.4.3_v00
    # --pedcal-file ../calibration/20200218/v00/calibration.Run02006.0000.hdf5
    # --drs4-pedestal-file ../calibration/20200218/v00/drs4_pedestal.Run02005.0000.fits
    # --time-calib-file ../calibration/20191124/v00/time_calibration.Run01625.0000.hdf5
    # --drive-file ../lapp/DrivePositioning/drive_log_20_02_18.txt
    # --run-summary ../monitoring/RunSummary/RunSummary_20200101.ecsv
    # 02006.0000
    # LST1

    flat_date = lstdate_to_dir(options.date)
    configfile_dl1 = cfg.get("lstchain", "dl1ab_config")
    configfile_dl2 = cfg.get("lstchain", "dl2_config")
    raw_dir = cfg.get("LST1", "R0_DIR")
    rf_models_directory = cfg.get("lstchain", "RF_MODELS")
    dl1_dir = cfg.get("LST1", "DL1_DIR")
    dl2_dir = cfg.get("LST1", "DL2_DIR")
    calib_dir = cfg.get("LST1", "CALIB_DIR")
    pedestal_dir = cfg.get("LST1", "PEDESTAL_DIR")
    # summary_dir = cfg.get("LST1", "RUN_SUMMARY_DIR")
    # calib_base_dir = cfg.get("LST1", "CALIB_BASE_DIR")
    # sys_dir = calib_base_dir / "ffactor_systematics"
    class_instance.SoftwareVersion = get_lstchain_version()
    class_instance.ProcessingConfigFile = options.configfile
    class_instance.ObservationDate = flat_date
    if class_instance.__name__ in REDUCTION_TASKS:
        muon_dir = Path(dl1_dir) / flat_date / options.prod_id
        outdir_dl1 = Path(dl1_dir) / flat_date / options.prod_id / options.dl1_prod_id
        outdir_dl2 = Path(dl2_dir) / flat_date / options.prod_id / options.dl2_prod_id

    if class_instance.__name__ in ["drs4_pedestal", "calibrate_charge"]:
        # drs4_pedestal_run_id  [0] 01804
        # pedcal_run_id         [1] 01805
        # history_file           [2] .../20210913/v0.7.5/sequence_LST1_01805.0000.history
        class_instance.PedestalRun = class_instance.args[0]
        class_instance.CalibrationRun = class_instance.args[1]

        pro = "pro"
        # TODO - massive reprocessing vs. next day processing

        # according to code in onsite scripts in lstchain
        #
        class_instance.RawObservationFilePedestal = (
            f"{raw_dir}/{flat_date}/LST-1.1.Run{class_instance.args[0]}.fits.fz"
        )
        class_instance.RawObservationFileCalibration = (
            f"{raw_dir}/{flat_date}/LST-1.1.Run{class_instance.args[1]}.fits.fz"
        )
        pedestal_plot = f"drs4_pedestal.Run{class_instance.args[0]}.0000.pdf"
        class_instance.PedestalCheckPlot = Path(pedestal_dir) / flat_date / pro / "log" / pedestal_plot
        calibration_plot = f"calibration_filters_52.Run{class_instance.args[1]}.0000.pdf"
        class_instance.CalibrationCheckPlot = Path(calib_dir) / flat_date / pro / "log" / calibration_plot
        # according to code in sequence_calibration_filenames from job.py
        #
        drs4_pedestal_file = f"drs4_pedestal.Run{class_instance.args[0]}.0000.h5"
        class_instance.PedestalFile = Path(pedestal_dir) / flat_date / pro / drs4_pedestal_file
        calibration_file = f"calibration_filters_52.Run{class_instance.args[1]}.0000.h5"
        class_instance.CoefficientsCalibrationFile = Path(calib_dir) / flat_date / pro / calibration_file
        class_instance.TimeCalibrationFile = get_time_calibration_file(int(class_instance.args[1]))

    if class_instance.__name__ == "r0_to_dl1":
        # calibrationfile   [0] .../20200218/v00/calibration.Run02006.0000.hdf5
        # pedestalfile      [1] .../20200218/v00/drs4_pedestal.Run02005.0000.fits
        # timecalibfile     [2] .../20191124/v00/time_calibration.Run01625.0000.hdf5
        # drivefile         [3] .../DrivePositioning/drive_log_20_02_18.txt
        # runsummaryfile    [4] .../RunSummary/RunSummary_20200101.ecsv
        # run_str           [5] 02006.0000

        class_instance.ObservationRun = class_instance.args[5].split(".")[0]
        # use realpath to resolve symbolic links and return abspath
        calibration_file = os.path.realpath(class_instance.args[0])
        pedestal_file = os.path.realpath(class_instance.args[1])
        timecalibration_file = os.path.realpath(class_instance.args[2])
        class_instance.R0SubrunDataset = (
            f"{raw_dir}/{flat_date}/LST-1.1.Run{class_instance.args[5]}.fits.fz"
        )
        class_instance.CoefficientsCalibrationFile = calibration_file
        class_instance.PedestalFile = pedestal_file
        class_instance.TimeCalibrationFile = timecalibration_file
        class_instance.PointingFile = str(class_instance.args[3])
        class_instance.RunSummaryFile = str(class_instance.args[4])
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[5]}.h5"
        )
        class_instance.MuonsSubrunDataset = (
            f"{muon_dir}/muons_LST-1.Run{class_instance.args[5]}.fits"
        )

    if class_instance.__name__ == "dl1ab":
        # run_str       [0] 02006.0000

        class_instance.Analysisconfigfile_dl1 = configfile_dl1
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.PedestalCleaning = "True"
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )

    if class_instance.__name__ == "dl1_datacheck":
        # run_str       [0] 02006.0000

        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.MuonsSubrunDataset = (
            f"{muon_dir}/muons_LST-1.Run{class_instance.args[0]}.fits"
        )
        class_instance.DL1CheckSubrunDataset = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL1CheckHDF5File = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.h5"
        )
        class_instance.DL1CheckPDFFile = (
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.pdf"
        )

    if class_instance.__name__ == "dl1_to_dl2":
        # run_str       [0] 02006.0000

        class_instance.Analysisconfigfile_dl2 = configfile_dl2
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.RFModelEnergyFile = str(
            Path(rf_models_directory) / "reg_energy.sav"
        )
        class_instance.RFModelDispNormFile = str(
            Path(rf_models_directory) / "reg_disp_norm.sav"
        )
        class_instance.RFModelDispSignFile = str(
            Path(rf_models_directory) / "reg_disp_sign.sav"
        )
        class_instance.RFModelGammanessFile = str(
            Path(rf_models_directory) / "cls_gh.sav"
        )
        class_instance.DL1SubrunDataset = (
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL2SubrunDataset = (
            f"{outdir_dl2}/dl2_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL2MergedFile = (
            f"{outdir_dl2}/dl2_LST-1.Run{class_instance.ObservationRun}.h5"
        )

    if class_instance.__name__ in REDUCTION_TASKS:
        class_instance.session_name = class_instance.ObservationRun

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
    subprocess.run(["conda", "env", "export", "--file", str(conda_env_file)], check=True)
