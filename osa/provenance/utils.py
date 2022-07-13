"""Utility functions for OSA pipeline provenance."""

import logging
import os
import re
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger
from osa.utils.utils import get_lstchain_version, date_to_dir

__all__ = ["parse_variables", "get_log_config"]

REDUCTION_TASKS = ["r0_to_dl1", "dl1ab", "dl1_datacheck", "dl1_to_dl2"]


def parse_variables(class_instance):
    """Parse variables needed in model"""

    # calibration_pipeline.py
    # -c sequencer.cfg
    # -d 2020_02_18
    # --drs4-pedestal-run 01804
    # --pedcal-run 01805
    # LST1

    # datasequence.py
    # -c sequencer.cfg
    # -d 2020_02_18
    # --prod-id v0.4.3_v00
    # --pedcal-file .../20200218/pro/calibration_filters_52.Run02006.0000.h5
    # --drs4-pedestal-file .../20200218/pro/drs4_pedestal.Run02005.0000.h5
    # --time-calib-file .../20191124/pro/time_calibration.Run01625.0000.h5
    # --drive-file .../lapp/DrivePositioning/drive_log_20_02_18.txt
    # --run-summary .../monitoring/RunSummary/RunSummary_20200101.ecsv
    # 02006.0000
    # LST1

    flat_date = date_to_dir(options.date)
    configfile_dl1b = cfg.get("lstchain", "dl1b_config")
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
        # drs4_pedestal_run_id  [0] 1804
        # pedcal_run_id         [1] 1805
        # history_file          [2] .../20210913/v0.7.5/sequence_LST1_01805.0000.history
        class_instance.PedestalRun = f"{class_instance.args[0]:05d}"
        class_instance.CalibrationRun = f"{class_instance.args[1]:05d}"

        pro = "pro"
        # TODO - massive reprocessing vs. next day processing

        # according to code in onsite scripts in lstchain
        class_instance.RawObservationFilePedestal = os.path.realpath(
            f"{raw_dir}/{flat_date}/LST-1.1.Run{class_instance.args[0]:05d}.fits.fz"
        )
        class_instance.RawObservationFileCalibration = os.path.realpath(
            f"{raw_dir}/{flat_date}/LST-1.1.Run{class_instance.args[1]:05d}.fits.fz"
        )
        class_instance.PedestalCheckPlot = os.path.realpath(
            f"{pedestal_dir}/{flat_date}/{pro}/"
            f"log/drs4_pedestal.Run{class_instance.args[0]:05d}.0000.pdf"
        )
        class_instance.CalibrationCheckPlot = os.path.realpath(
            f"{calib_dir}/{flat_date}/{pro}/"
            f"log/calibration_filters_52.Run{class_instance.args[1]:05d}.0000.pdf"
        )

        # according to code in sequence_calibration_files from paths.py
        class_instance.PedestalFile = os.path.realpath(
            f"{pedestal_dir}/{flat_date}/{pro}/"
            f"drs4_pedestal.Run{class_instance.args[0]:05d}.0000.h5"
        )
        class_instance.CoefficientsCalibrationFile = os.path.realpath(
            f"{calib_dir}/{flat_date}/{pro}/"
            f"calibration_filters_52.Run{class_instance.args[1]:05d}.0000.h5"
        )

    if class_instance.__name__ == "r0_to_dl1":
        # calibration_file   [0] .../20200218/pro/calibration_filters_52.Run02006.0000.h5
        # drs4_pedestal_file [1] .../20200218/pro/drs4_pedestal.Run02005.0000.h5
        # time_calib_file    [2] .../20191124/pro/time_calibration.Run01625.0000.h5
        # systematic_corr    [3] .../20200101/pro/no_sys_corrected_calib_20210514.0000.h5
        # drive_file         [4] .../DrivePositioning/drive_log_20_02_18.txt
        # run_summary_file   [5] .../RunSummary/RunSummary_20200101.ecsv
        # pedestal_ids_file  [6] .../path/to/interleaved/pedestal/events.h5
        # run_str            [7] 02006.0000

        run_subrun_id = class_instance.args[7]
        class_instance.ObservationRun = run_subrun_id.split(".")[0]
        # use realpath to resolve symbolic links and return abspath
        calibration_file = os.path.realpath(class_instance.args[0])
        pedestal_file = os.path.realpath(class_instance.args[1])
        timecalibration_file = os.path.realpath(class_instance.args[2])
        systematic_correction_file = os.path.realpath(class_instance.args[3])
        class_instance.R0SubrunDataset = os.path.realpath(
            f"{raw_dir}/{flat_date}/LST-1.1.Run{run_subrun_id}.fits.fz"
        )
        class_instance.CoefficientsCalibrationFile = calibration_file
        class_instance.PedestalFile = pedestal_file
        class_instance.TimeCalibrationFile = timecalibration_file
        class_instance.SystematicCorrectionFile = systematic_correction_file
        class_instance.PointingFile = os.path.realpath(class_instance.args[4])
        class_instance.RunSummaryFile = os.path.realpath(class_instance.args[5])
        class_instance.DL1SubrunDataset = os.path.realpath(
            f"{outdir_dl1}/dl1_LST-1.Run{run_subrun_id}.h5"
        )
        class_instance.MuonsSubrunDataset = os.path.realpath(
            f"{muon_dir}/muons_LST-1.Run{run_subrun_id}.fits"
        )
        class_instance.InterleavedPedestalEventsFile = None
        if class_instance.args[6] is not None:
            class_instance.InterleavedPedestalEventsFile = os.path.realpath(class_instance.args[6])

    if class_instance.__name__ == "dl1ab":
        # run_str       [0] 02006.0000

        class_instance.Analysisconfigfile_dl1 = os.path.realpath(configfile_dl1b)
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")
        class_instance.DL1SubrunDataset = os.path.realpath(
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )

    if class_instance.__name__ == "dl1_datacheck":
        # run_str       [0] 02006.0000

        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.DL1SubrunDataset = os.path.realpath(
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.MuonsSubrunDataset = os.path.realpath(
            f"{muon_dir}/muons_LST-1.Run{class_instance.args[0]}.fits"
        )
        class_instance.DL1CheckSubrunDataset = os.path.realpath(
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL1CheckHDF5File = os.path.realpath(
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.h5"
        )
        class_instance.DL1CheckPDFFile = os.path.realpath(
            f"{outdir_dl1}/datacheck_dl1_LST-1.Run{class_instance.ObservationRun}.pdf"
        )

    if class_instance.__name__ == "dl1_to_dl2":
        # run_str       [0] 02006.0000

        class_instance.Analysisconfigfile_dl2 = configfile_dl2
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.RFModelEnergyFile = os.path.realpath(f"{rf_models_directory}/reg_energy.sav")
        class_instance.RFModelDispNormFile = os.path.realpath(
            f"{rf_models_directory}/reg_disp_norm.sav"
        )
        class_instance.RFModelDispSignFile = os.path.realpath(
            f"{rf_models_directory}/reg_disp_sign.sav"
        )
        class_instance.RFModelGammanessFile = os.path.realpath(f"{rf_models_directory}/cls_gh.sav")
        class_instance.DL1SubrunDataset = os.path.realpath(
            f"{outdir_dl1}/dl1_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL2SubrunDataset = os.path.realpath(
            f"{outdir_dl2}/dl2_LST-1.Run{class_instance.args[0]}.h5"
        )
        class_instance.DL2MergedFile = os.path.realpath(
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
        log = myLogger(logging.getLogger(__name__))
        log.warning(f"{config_file} not found, using {std_logger_file} instead.")

    # use default logger.yaml if no prov config info found
    if log_config == "":
        log_config = std_logger_file.read_text()

    return log_config
