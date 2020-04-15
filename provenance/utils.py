"""
Utility functions for OSA pipeline provenance
"""

import re

__all__ = ["parse_variables"]


def parse_variables(class_instance):
    """Parse variables needed in model"""
    # datasequence.py
    # -c cfg/sequencer.cfg
    # -d 2020_02_18
    # -o /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/
    # /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run2006.0000.hdf5
    # /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run2005.0000.fits
    # /fefs/aswg/data/real/calibration/20191124/v00/time_calibration.Run1625.0000.hdf5
    # /fefs/home/lapp/DrivePositioning/drive_log_20_02_18.txt
    # ucts_t0_dragon
    # dragon_counter0
    # ucts_t0_tib
    # tib_counter0
    # 02006.0000
    # LST1

    from osa.configs.config import cfg

    configfile = cfg.get("LSTOSA", "CONFIGFILE")

    if class_instance.__name__ == "r0_to_dl1":
        class_instance.AnalysisConfigFile = configfile
        class_instance.CoefficientsCalibrationFile = class_instance.args[0]
        class_instance.PedestalFile = class_instance.args[1]
        class_instance.TimeCalibrationFile = class_instance.args[2]
        class_instance.PointingFile = class_instance.args[3]
        class_instance.ObservationRun = class_instance.args[8].split(".")[0]
        class_instance.ObservationSubRun = class_instance.args[8].split(".")[1]
        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/sequence_LST1_02006.0000.txt
        class_instance.ObservationDate = re.findall(r"DL1/(\d{8})/", class_instance.args[9])[0]
        class_instance.SoftwareVersion = re.findall(r"DL1/\d{8}/(v.*)_v", class_instance.args[9])[0]
        class_instance.ProdID = re.findall(r"DL1/\d{8}/v.*_v(.*)/", class_instance.args[9])[0]
        # /fefs/aswg/data/real/calibration/20200218/v00/calibration.Run2006.0000.hdf5
        class_instance.CalibrationRun = re.findall(r"Run(\d{4}).", class_instance.args[0])[0]
        # /fefs/aswg/data/real/calibration/20200218/v00/drs4_pedestal.Run2005.0000.fits
        class_instance.PedestalRun = re.findall(r"Run(\d{4}).", class_instance.args[1])[0]
        # /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00/dl1_LST1.1Run2006.0001.fits.fz.h5
        fits = cfg.get("LSTOSA", "FITSSUFFIX")
        fz = cfg.get("LSTOSA", "COMPRESSEDSUFFIX")
        outdir = re.findall(r"(.*)sequence", class_instance.args[9])[0]
        class_instance.DL1SubrunDataset = f"{outdir}dl1_LST1.1Run{class_instance.args[8]}{fits}{fz}.h5"
        # /fefs/aswg/data/real/R0/20200218/LST1.1Run2006.0001.fits.fz
        rawdir = cfg.get("LST1", "RAWDIR")
        class_instance.R0SubrunDataset = f"{rawdir}/{class_instance.ObservationDate}/LST1.1Run{class_instance.args[8]}{fits}{fz}"

    return class_instance
