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
    input_state = getattr(options, "input_state", "legacy_raw")

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

    # =========================
    # CALIBRATION PIPELINE
    # =========================
    if class_instance.__name__ in ["drs4_pedestal", "calibrate_charge"]:
        class_instance.PedestalRun = f"{class_instance.args[0]:05d}"
        class_instance.CalibrationRun = f"{class_instance.args[1]:05d}"

        version = get_lstchain_version()

        class_instance.RawObservationFilePedestal = str((
            raw_dir / flat_date / f"LST-1.1.Run{class_instance.args[0]:05d}.fits.fz"
        ).resolve())

        class_instance.RawObservationFileCalibration = str((
            raw_dir / flat_date / f"LST-1.1.Run{class_instance.args[1]:05d}.fits.fz"
        ).resolve())

        class_instance.PedestalCheckPlot = str((
            pedestal_dir / flat_date / version /
            f"log/drs4_pedestal.Run{class_instance.args[0]:05d}.0000.pdf"
        ).resolve())

        class_instance.CalibrationCheckPlot = str((
            calib_dir / flat_date / version /
            f"log/calibration_filters_52.Run{class_instance.args[1]:05d}.0000.pdf"
        ).resolve())

        class_instance.PedestalFile = str((
            pedestal_dir / flat_date / version /
            f"drs4_pedestal.Run{class_instance.args[0]:05d}.0000.h5"
        ).resolve())

        class_instance.CoefficientsCalibrationFile = str((
            calib_dir / flat_date / version /
            f"calibration_filters_52.Run{class_instance.args[1]:05d}.0000.h5"
        ).resolve())

    # =========================
    # R0 → DL1
    # =========================
    if class_instance.__name__ == "r0_to_dl1":

        # calibration_file   [0] .../20200218/v0.4.3/calibration_filters_52.Run02006.0000.h5
        # drs4_pedestal_file [1] .../20200218/v0.4.3/drs4_pedestal.Run02005.0000.h5
        # time_calib_file    [2] .../20191124/v0.4.3/time_calibration.Run01625.0000.h5
        # systematic_corr    [3] .../20200101/v0.4.3/no_sys_corrected_calib_20210514.0000.h5
        # drive_file         [4] .../DrivePositioning/DrivePosition_20200218.txt
        # run_summary_file   [5] .../RunSummary/RunSummary_20200101.ecsv
        # pedestal_ids_file  [6] .../path/to/interleaved/pedestal/events.h5
        # run_str            [7] 02006.0000



        run_subrun = class_instance.args[7]
        run = run_subrun.split(".")[0]
        class_instance.ObservationRun = run

        outdir_dl1 = dl1_dir / flat_date / options.prod_id

        calibration_file = Path(class_instance.args[0]).resolve() if class_instance.args[0] else None
        pedestal_file = Path(class_instance.args[1]).resolve() if class_instance.args[1] else None
        timecalibration_file = Path(class_instance.args[2]).resolve() if class_instance.args[2] else None
        systematic_correction_file = Path(class_instance.args[3]).resolve() if class_instance.args[3] else None
        class_instance.R0SubrunDataset = str((
            raw_dir / flat_date / f"LST-1.1.Run{run_subrun}.fits.fz"
        ).resolve())
        class_instance.CoefficientsCalibrationFile = str(calibration_file) if calibration_file else None
        class_instance.PedestalFile = str(pedestal_file) if pedestal_file else None
        class_instance.TimeCalibrationFile = str(timecalibration_file) if timecalibration_file else None
        class_instance.SystematicCorrectionFile = str(systematic_correction_file) if systematic_correction_file else None
        class_instance.PointingFile = str(Path(class_instance.args[4]).resolve()) if class_instance.args[4] else None
        class_instance.RunSummaryFile = str(Path(class_instance.args[5]).resolve()) if class_instance.args[5] else None


        # input data
        class_instance.R0SubrunDataset = str((
            raw_dir / flat_date / f"LST-1.1.Run{run_subrun}.fits.fz"
        ).resolve())

        # None-safe paths
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

        # aplicar lógica de input_state
        if input_state == "catA_calibrated":
            class_instance.CoefficientsCalibrationFile = None
            class_instance.PedestalFile = None
            class_instance.TimeCalibrationFile = None
            class_instance.SystematicCorrectionFile = None
        else:
            class_instance.CoefficientsCalibrationFile = calibration_file
            class_instance.PedestalFile = pedestal_file
            class_instance.TimeCalibrationFile = timecalibration_file
            class_instance.SystematicCorrectionFile = systematic_correction_file

        # opcionales seguros
        class_instance.PointingFile = (
            str(Path(class_instance.args[4]).resolve())
            if class_instance.args[4] is not None else None
        )

        class_instance.RunSummaryFile = (
            str(Path(class_instance.args[5]).resolve())
            if class_instance.args[5] is not None else None
        )


        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )

        class_instance.MuonsSubrunDataset = str((
            muon_dir / f"muons_LST-1.Run{run_subrun}.fits"
        ).resolve())

        class_instance.InterleavedPedestalEventsFile = (
            str(Path(class_instance.args[6]))
            if class_instance.args[6] is not None else None
        )

    # =========================
    # CAT-B
    # =========================
    if class_instance.__name__ == "catB_calibration":
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]

    # =========================
    # DL1AB
    # =========================
    if class_instance.__name__ == "dl1ab":

        outdir_dl1 = dl1_dir / flat_date / options.prod_id / class_instance.args[2]

        class_instance.Analysisconfigfile_dl1 = str(Path(configfile_dl1b))
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")

        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{class_instance.args[0]}.h5").resolve()
        )

    # =========================
    # DL1 DATACHECK
    # =========================
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

        class_instance.DL1CheckSubrunDataset = str(
            (outdir_dl1 / f"datacheck_dl1_LST-1.Run{run_subrun}.h5").resolve()
        )

        class_instance.DL1CheckHDF5File = str(
            (outdir_dl1 / f"datacheck_dl1_LST-1.Run{run}.h5").resolve()
        )

        class_instance.DL1CheckPDFFile = str(
            (outdir_dl1 / f"datacheck_dl1_LST-1.Run{run}.pdf").resolve()
        )

    # =========================
    # DL2
    # =========================
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
