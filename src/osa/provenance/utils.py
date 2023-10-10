"""Utility functions for OSA pipeline provenance."""


from importlib.resources import files
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import date_to_dir, get_lstchain_version

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
    # --prod-id v0.4
    # --pedcal-file .../20200218/v0.4.3/calibration_filters_52.Run02006.0000.h5
    # --drs4-pedestal-file .../20200218/v0.4.3/drs4_pedestal.Run02005.0000.h5
    # --time-calib-file .../20191124/pro/time_calibration.Run01625.0000.h5
    # --systematic_correction_file .../20200725/pro/ffactor_systematics_20200725.h5
    # --drive-file .../DrivePositioning/DrivePosition_20200218.txt
    # --run-summary .../monitoring/RunSummary/RunSummary_20200101.ecsv
    # 02006.0000
    # LST1

    flat_date = date_to_dir(options.date)
    configfile_dl1b = cfg.get("lstchain", "dl1b_config")
    configfile_dl2 = cfg.get("lstchain", "dl2_config")
    raw_dir = Path(cfg.get("LST1", "R0_DIR"))
    rf_models_directory = Path(cfg.get("lstchain", "RF_MODELS"))
    dl1_dir = Path(cfg.get("LST1", "DL1_DIR"))
    dl2_dir = Path(cfg.get("LST1", "DL2_DIR"))
    calib_dir = Path(cfg.get("LST1", "CALIB_DIR"))
    pedestal_dir = Path(cfg.get("LST1", "PEDESTAL_DIR"))

    class_instance.SoftwareVersion = get_lstchain_version()
    class_instance.ProcessingConfigFile = str(options.configfile)
    class_instance.ObservationDate = flat_date
    if class_instance.__name__ in REDUCTION_TASKS:
        muon_dir = dl1_dir / flat_date / options.prod_id / "muons"
        outdir_dl1 = dl1_dir / flat_date / options.prod_id / options.dl1_prod_id
        outdir_dl2 = dl2_dir / flat_date / options.prod_id / options.dl2_prod_id

    if class_instance.__name__ in ["drs4_pedestal", "calibrate_charge"]:
        # drs4_pedestal_run_id  [0] 1804
        # pedcal_run_id         [1] 1805
        # history_file          [2] .../20210913/v0.7.5/sequence_LST1_01805.0000.history
        class_instance.PedestalRun = f"{class_instance.args[0]:05d}"
        class_instance.CalibrationRun = f"{class_instance.args[1]:05d}"

        version = get_lstchain_version()

        # according to code in onsite scripts in lstchain
        class_instance.RawObservationFilePedestal = str((
            raw_dir / flat_date / f"LST-1.1.Run{class_instance.args[0]:05d}.fits.fz"
        ).resolve())
        class_instance.RawObservationFileCalibration = str((
            raw_dir / flat_date / f"LST-1.1.Run{class_instance.args[1]:05d}.fits.fz"
        ).resolve())
        class_instance.PedestalCheckPlot = str((
            pedestal_dir
            / flat_date
            / version
            / f"log/drs4_pedestal.Run{class_instance.args[0]:05d}.0000.pdf"
        ).resolve())
        class_instance.CalibrationCheckPlot = str((
            calib_dir
            / flat_date
            / version
            / f"log/calibration_filters_52.Run{class_instance.args[1]:05d}.0000.pdf"
        ).resolve())

        # according to code in sequence_calibration_files from paths.py
        class_instance.PedestalFile = str((
            pedestal_dir
            / flat_date
            / version
            / f"drs4_pedestal.Run{class_instance.args[0]:05d}.0000.h5"
        ).resolve())
        class_instance.CoefficientsCalibrationFile = str((
            calib_dir
            / flat_date
            / version
            / f"calibration_filters_52.Run{class_instance.args[1]:05d}.0000.h5"
        ).resolve())

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

        calibration_file = Path(class_instance.args[0]).resolve()
        pedestal_file = Path(class_instance.args[1]).resolve()
        timecalibration_file = Path(class_instance.args[2]).resolve()
        systematic_correction_file = Path(class_instance.args[3]).resolve()
        class_instance.R0SubrunDataset = str((
            raw_dir / flat_date / f"LST-1.1.Run{run_subrun}.fits.fz"
        ).resolve())
        class_instance.CoefficientsCalibrationFile = str(calibration_file)
        class_instance.PedestalFile = str(pedestal_file)
        class_instance.TimeCalibrationFile = str(timecalibration_file)
        class_instance.SystematicCorrectionFile = str(systematic_correction_file)
        class_instance.PointingFile = str(Path(class_instance.args[4]).resolve())
        class_instance.RunSummaryFile = str(Path(class_instance.args[5]).resolve())
        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )
        class_instance.MuonsSubrunDataset = str((
            muon_dir / f"muons_LST-1.Run{run_subrun}.fits"
        ).resolve())
        class_instance.InterleavedPedestalEventsFile = None
        if class_instance.args[6] is not None:
            class_instance.InterleavedPedestalEventsFile = str(Path(class_instance.args[6]))

    if class_instance.__name__ == "dl1ab":
        # run_str       [0] 02006.0000

        class_instance.Analysisconfigfile_dl1 = str(Path(configfile_dl1b))
        class_instance.ObservationRun = class_instance.args[0].split(".")[0]
        class_instance.StoreImage = cfg.getboolean("lstchain", "store_image_dl1ab")
        class_instance.DL1SubrunDataset = str((
            outdir_dl1 / f"dl1_LST-1.Run{class_instance.args[0]}.h5"
        ).resolve())

    if class_instance.__name__ == "dl1_datacheck":
        # run_str       [0] 02006.0000
        run_subrun = class_instance.args[0]
        run = run_subrun.split(".")[0]

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
        # run_str       [0] 02006.0000
        run_subrun = class_instance.args[0]
        run = run_subrun.split(".")[0]

        class_instance.Analysisconfigfile_dl2 = configfile_dl2
        class_instance.ObservationRun = run
        class_instance.RFModelEnergyFile = str((rf_models_directory / "reg_energy.sav").resolve())
        class_instance.RFModelDispNormFile = str(
            (rf_models_directory / "reg_disp_norm.sav").resolve()
        )
        class_instance.RFModelDispSignFile = str(
            (rf_models_directory / "reg_disp_sign.sav").resolve()
        )
        class_instance.RFModelGammanessFile = str((rf_models_directory / "cls_gh.sav").resolve())
        class_instance.DL1SubrunDataset = str(
            (outdir_dl1 / f"dl1_LST-1.Run{run_subrun}.h5").resolve()
        )
        class_instance.DL2SubrunDataset = str(
            (outdir_dl2 / f"dl2_LST-1.Run{run_subrun}.h5").resolve()
        )
        class_instance.DL2MergedFile = str((outdir_dl2 / f"dl2_LST-1.Run{run}.h5").resolve())

    if class_instance.__name__ in REDUCTION_TASKS:
        class_instance.session_name = class_instance.ObservationRun

    return class_instance


def get_log_config():
    """Get logging configuration from provenance logger config file."""
    std_logger_file = files("osa.provenance") / "config/logger.yaml"
    return std_logger_file.read_text()
