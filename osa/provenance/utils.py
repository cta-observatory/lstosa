"""
Utility functions for OSA pipeline provenance
"""
import logging
import re
import sys
from pathlib import Path

from osa.configs.config import cfg
from osa.utils import options


__all__ = ["parse_variables", "get_log_config"]


def parse_variables(class_instance):
    """Parse variables needed in model"""
    # datasequence.py
    # -c cfg/sequencer.cfg
    # -d 2020_02_18
    # -o /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/
    # --prod_id v0.4.3_v00
    # /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run02006.0000.hdf5
    # /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run02005.0000.fits
    # /fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run01625.0000.hdf5
    # /fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt
    # ucts_t0_dragon
    # dragon_counter0
    # ucts_t0_tib
    # tib_counter0
    # 02006.0000
    # LST1

    pythondir = cfg.get("LSTOSA", "PYTHONDIR")
    configfile = cfg.get("LSTOSA", "CONFIGFILE")
    rawdir = cfg.get("LST1", "RAWDIR")
    dl1dir = cfg.get("LST1", "DL1DIR")
    fits = cfg.get("LSTOSA", "FITSSUFFIX")
    fz = cfg.get("LSTOSA", "COMPRESSEDSUFFIX")
    h5 = cfg.get("LSTOSA", "H5SUFFIX")
    r0_prefix = cfg.get("LSTOSA", "R0PREFIX")
    dl1_prefix = cfg.get("LSTOSA", "DL1PREFIX")
    dl2_prefix = cfg.get("LSTOSA", "DL2PREFIX")
    rf_models_directory = cfg.get("LSTOSA", "RF-MODELS-DIR")

    if class_instance.__name__ == "r0_to_dl1":
        # calibrationfile   [0] /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run02006.0000.hdf5
        # pedestalfile      [1] /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run02005.0000.fits
        # time_calibration  [2] /fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run01625.0000.hdf5
        # drivefile         [3] /fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt
        # ucts_t0_dragon
        # dragon_counter0
        # ucts_t0_tib
        # tib_counter0
        # run_str           [8] 02006.0000
        # historyfile       [9] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.history

        class_instance.AnalysisConfigFile = configfile
        class_instance.CoefficientsCalibrationFile = class_instance.args[0]
        class_instance.PedestalFile = class_instance.args[1]
        class_instance.TimeCalibrationFile = class_instance.args[2]
        class_instance.PointingFile = class_instance.args[3]
        class_instance.ObservationRun = class_instance.args[8].split(".")[0]
        class_instance.ObservationSubRun = class_instance.args[8].split(".")[1]
        class_instance.ObservationDate = re.findall(r"running_analysis/(\d{8})/", class_instance.args[9])[0]
        class_instance.SoftwareVersion = re.findall(r"running_analysis/\d{8}/(v.*)_v", class_instance.args[9])[0]
        class_instance.ProdID = re.findall(r"running_analysis/\d{8}/v.*_v(.*)/", class_instance.args[9])[0]
        class_instance.CalibrationRun = str(re.findall(r"Run(\d{4}).", class_instance.args[0])[0]).zfill(5)
        class_instance.PedestalRun = str(re.findall(r"Run(\d{4}).", class_instance.args[1])[0]).zfill(5)
        running_analysis_dir = re.findall(r"(.*)sequence", class_instance.args[9])[0]
        outdir_dl1 = running_analysis_dir.replace("running_analysis", "DL1")
        # as of lstchain v0.5.0 /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/dl1_LST-1.Run02006.0001.h5
        class_instance.DL1SubrunDataset = f"{outdir_dl1}{dl1_prefix}.Run{class_instance.args[8]}{h5}"
        # /fefs/aswg/data/real/R0/20200218/LST1.1.Run02006.0001.fits.fz
        class_instance.R0SubrunDataset = f"{rawdir}/{class_instance.ObservationDate}/{r0_prefix}.Run{class_instance.args[8]}{fits}{fz}"
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = f"{pythondir}/{options.configfile}"

    if class_instance.__name__ == "dl1_to_dl2":
        # run_str       [0] 02006.0000
        # historyfile   [1] /fefs/aswg/data/real/running_analysis/20200218/v0.4.3_v00/sequence_LST1_02006.0000.txt

        class_instance.AnalysisConfigFile = configfile
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.ObservationSubRun = class_instance.args[0].split(".")[1]
        class_instance.ObservationDate = re.findall(r"running_analysis/(\d{8})/", class_instance.args[1])[0]
        class_instance.SoftwareVersion = re.findall(r"running_analysis/\d{8}/(v.*)_v", class_instance.args[1])[0]
        class_instance.DL1ProdID = re.findall(r"running_analysis/\d{8}/v.*_v(.*)/", class_instance.args[1])[0]
        class_instance.RFModelEnergyFile = str(Path(rf_models_directory) / "reg_energy.sav")
        class_instance.RFModelDispFile = str(Path(rf_models_directory) / "reg_disp_vector.sav")
        class_instance.RFModelGammanessFile = str(Path(rf_models_directory) / "cls_gh.sav")
        running_analysis_dir = re.findall(r"(.*)sequence", class_instance.args[1])[0]
        outdir_dl1 = running_analysis_dir.replace("running_analysis", "DL1")
        outdir_dl2 = outdir_dl1.replace("DL1", "DL2")
        # as of lstchain v0.5.0 /fefs/aswg/data/real/DL2/20200218/v0.4.3_v00/dl2_LST-1.Run02006.0001.h5
        class_instance.DL2SubrunDataset = f"{outdir_dl2}{dl2_prefix}.Run{class_instance.args[0]}{h5}"
        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/dl1_LST-1.Run02006.0001.h5
        class_instance.DL1SubrunDataset = f"{outdir_dl1}{dl1_prefix}.Run{class_instance.args[0]}{h5}"
        class_instance.session_name = class_instance.ObservationRun
        class_instance.ProcessingConfigFile = f"{pythondir}/{options.configfile}"

    return class_instance


def get_log_config():
    """Get logging configuration from an OSA config file"""

    # default config filename value
    config_file = Path(__file__).resolve().parent / ".." / ".." / "cfg" / "sequencer.cfg"
    std_logger_file = Path(__file__).resolve().parent / "config" / "logger.yaml"

    # fetch config filename value from args
    in_config_arg = False
    for arg in sys.argv:
        if in_config_arg:
            config_file = arg
            in_config_arg = False
        if arg == "-c" or arg == "--config":
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
