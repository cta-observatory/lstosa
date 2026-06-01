"""Utility functions for OSA pipeline provenance."""

from importlib.resources import files
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import date_to_dir, get_lstchain_version

__all__ = ["parse_variables", "get_log_config"]

REDUCTION_TASKS = ["r0_to_dl1", "catB_calibration", "dl1ab", "dl1_datacheck", "dl1_to_dl2"]


def parse_variables(class_instance):
    """Parse variables needed in model"""

    flat_date = date_to_dir(options.date)
    configfile_dl1b = cfg.get("lstchain", "dl1b_config")
    configfile_dl2 = cfg.get("lstchain", "dl2_config")
    raw_dir = Path(cfg.get("LST1", "R0_DIR"))
    rf_models_directory = Path(cfg.get("LST1", "RF_MODELS"))
    dl1_dir = Path(cfg.get("LST1", "DL1_DIR"))
    dl2_dir = Path(cfg.get("LST1", "DL2_DIR"))
    calib_dir = Path(cfg.get("LST1", "CAT_A_CALIB_DIR"))
    pedestal_dir = Path(cfg.get("LST1", "CAT_A_PEDESTAL_DIR"))

    class_instance.SoftwareVersion = get_lstchain_version()
    class_instance.ProcessingConfigFile = str(options.configfile)
    class_instance.ObservationDate = flat_date

    if class_instance.__name__ in REDUCTION_TASKS:
        muon_dir = dl1_dir / flat_date / options.prod_id / "muons"

    # ============================================
    # BLOQUE CORREGIDO PARA r0_to_dl1
    # ============================================
    if class_instance.__name__ == "r0_to_dl1":

        run_subrun = class_instance.args[7]
        run = run_subrun.split(".")[0]
        class_instance.ObservationRun = run

        outdir_dl1 = dl1_dir / flat_date / options.prod_id

        # CALIBRATION FILES (pueden ser None)
        calibration_file = (
            str(Path(class_instance.args[0]).resolve())
            if class_instance.args[0] is not None else None
        )

        pedestal_file = (
            str(Path(class_instance.args[1]).resolve())
            if class_instance.args[1] is not None else None
        )

        timecalibration_file = (
            str(Path(class_instance.args[2]).resolve())
            if class_instance.args[2] is not None else None
        )

        systematic_correction_file = (
            str(Path(class_instance.args[3]).resolve())
            if class_instance.args[3] is not None else None
        )

        # DATA INPUT
        class_instance.R0SubrunDataset = str((
            raw_dir / flat_date / f"LST-1.1.Run{run_subrun}.fits.fz"
        ).resolve())

        # sin str() extra)
        class_instance.CoefficientsCalibrationFile = calibration_file
        class_instance.PedestalFile = pedestal_file
        class_instance.TimeCalibrationFile = timecalibration_file
        class_instance.SystematicCorrectionFile = systematic_correction_file

        # DRIVE y RUN SUMMARY pueden ser None
        class_instance.PointingFile = (
            str(Path(class_instance.args[4]).resolve())
            if class_instance.args[4] is not None else None
        )

        class_instance.RunSummaryFile = (
            str(Path(class_instance.args[5]).resolve())
            if class_instance.args[5] is not None else None
        )

        # OUTPUTS
        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )

        class_instance.MuonsSubrunDataset = str((
            muon_dir / f"muons_LST-1.Run{run_subrun}.fits"
        ).resolve())

        class_instance.InterleavedPedestalEventsFile = None
        if class_instance.args[6] is not None:
            class_instance.InterleavedPedestalEventsFile = str(Path(class_instance.args[6]))

    if class_instance.__name__ == "catB_calibration":
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]

    if class_instance.__name__ == "dl1ab":
        outdir_dl1 = dl1_dir / flat_date / options.prod_id / class_instance.args[2]
        class_instance.Analysisconfigfile_dl1 = str(Path(configfile_dl1b))
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")
        class_instance.DL1SubrunDataset = str((
            outdir_dl1 / f"dl1_LST-1.Run{class_instance.args[0]}.h5"
        ).resolve())

    if class_instance.__name__ == "dl1_datacheck":
        run_subrun = class_instance.args[0]
        run = run_subrun.split(".")[0]

        outdir_dl1 = dl1_dir / flat_date / options.prod_id / class_instance.args[1]
        class_instance.ObservationRun = run
        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )

        class_instance.MuonsSubrunDataset = str((
            muon_dir / f"muons_LST-1.Run{run_subrun}.fits"
        ).resolve())

        class_instance.DL1CheckSubrunDataset = str((
            outdir_dl1 / f"datacheck_dl1_LST-1.Run{run_subrun}.h5"
        ).resolve())

        class_instance.DL1CheckHDF5File = str((
            outdir_dl1 / f"datacheck_dl1_LST-1.Run{run}.h5"
        ).resolve())

        class_instance.DL1CheckPDFFile = str((
            outdir_dl1 / f"datacheck_dl1_LST-1.Run{run}.pdf"
        ).resolve())

    if class_instance.__name__ == "dl1_to_dl2":
        run_subrun = class_instance.args[0]
        run = run_subrun.split(".")[0]

        outdir_dl1 = dl1_dir / flat_date / options.prod_id / class_instance.args[2]
        outdir_dl2 = dl2_dir / flat_date / options.prod_id / class_instance.args[3]

        class_instance.Analysisconfigfile_dl2 = configfile_dl2
        class_instance.ObservationRun = run

        class_instance.RFModelEnergyFile = str((rf_models_directory / "reg_energy.sav").resolve())
        class_instance.RFModelDispNormFile = str((rf_models_directory / "reg_disp_norm.sav").resolve())
        class_instance.RFModelDispSignFile = str((rf_models_directory / "reg_disp_sign.sav").resolve())
        class_instance.RFModelGammanessFile = str((rf_models_directory / "cls_gh.sav").resolve())

        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )
        class_instance.DL2SubrunDataset = str(
            (outdir_dl2 / f"dl2_LST-1.Run{run_subrun}.h5").resolve()
        )
        class_instance.DL2MergedFile = str(
            (outdir_dl2 / f"dl2_LST-1.Run{run}.h5").resolve()
        )

    if class_instance.__name__ in REDUCTION_TASKS:
        class_instance.session_name = class_instance.ObservationRun

    return class_instance


def get_log_config():
    std_logger_file = files("osa.provenance") / "config/logger.yaml"
    return std_logger_file.read_text()
