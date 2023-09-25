"""
Manage the stages of the analysis workflow.

Build lstchain commands, run them, clean up their output of a given step and
retry in case of failure, and keep track of the history of the stages.
"""

import logging
import subprocess as sp
from pathlib import Path
from typing import List, Union

from tenacity import retry, stop_after_attempt
import lstchain

from osa.configs import options
from osa.configs.config import cfg
from osa.report import history
from osa.utils.logging import myLogger
from osa.utils.utils import stringify, date_to_dir
from osa.paths import get_run_date

log = myLogger(logging.getLogger(__name__))


class AnalysisStage:
    """Run a given analysis stage keeping track of checkpoints in a history file.

    Parameters
    ----------
    run: str
        Run number
    command_args: List[str]
        Complete command line arguments to be executed in the shell as a list
    config_file: str, optional
        Path to the config file used for the stage
    """

    def __init__(
        self,
        run: str,
        command_args: List[str],
        config_file: Union[str, None] = None,
    ):
        self.run = run
        self.command_args = command_args
        self.config_file = config_file
        self.command = self.command_args[0]
        self.rc = None
        self.history_file = (
            Path(options.directory) / f"sequence_{options.tel_id}_{self.run}.history"
        )

    @retry(stop=stop_after_attempt(int(cfg.get("lstchain", "max_tries"))))
    def execute(self):
        """Run the program and retry if it fails."""
        log.info(f"Executing {stringify(self.command_args)}")
        output = sp.run(self.command_args, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8")
        self.rc = output.returncode
        self._write_checkpoint()

        # If fails, remove products from the directory for subsequent trials
        if self.rc != 0:
            self._clean_up()
            raise ValueError(f"{self.command} failed with output: \n {output.stdout}")

    def show_command(self):
        """Show the command to be executed."""
        return stringify(self.command_args)

    def _clean_up(self):
        """
        Clean up the output files created at a given analysis stage that exited with a
        non-zero return code, provided the output file exists. In this way, they can be
        reproduced in subsequent trials.
        """

        if self.command == "lstchain_data_r0_to_dl1":
            self._remove_dl1a_output()
        elif self.command == "lstchain_dl1ab":
            self._remove_dl1b_output('dl1_LST-1.Run')
        elif self.command == "lstchain_check_dl1":
            self._remove_dl1b_output('datacheck_dl1_LST-1.Run')
        elif self.command == "onsite_create_calibration_file":
            self._remove_calibration()
        elif self.command == "onsite_create_drs4_pedestal_file":
            self._remove_drs4_baseline()

    def _remove_drs4_baseline(self):
        drs4_pedestal_basedir = Path(cfg.get("LST1", "PEDESTAL_DIR"))
        date = date_to_dir(get_run_date(self.run))
        drs4_pedestal_dir = drs4_pedestal_basedir / date / lstchain.__version__
        file = drs4_pedestal_dir / "drs4_pedestal.Run{self.run}.0000.h5"
        file.unlink(missing_ok=True)
        # Also remove the link to "pro" directory
        drs4_pedestal_dir_pro = drs4_pedestal_basedir / date / "pro"
        drs4_pedestal_dir_pro.unlink(missing_ok=True)

    def _remove_calibration(self):
        calib_basedir = Path(cfg.get("LST1", "CALIB_DIR"))
        date = date_to_dir(get_run_date(self.run))
        calib_dir = file = calib_basedir / date / lstchain.__version__
        file = calib_dir / f"calibration_filters_{options.filters}.Run{self.run}.0000.h5"
        file.unlink(missing_ok=True)
        # Also remove the link to "pro" directory
        calib_dir_pro = file = calib_basedir / date / "pro"
        calib_dir_pro.unlink(missing_ok=True)

    def _remove_dl1a_output(self):
        dl1_output_file = options.directory / f"dl1_LST-1.Run{self.run}.h5"
        muon_output_file = options.directory / f"muons_LST-1.Run{self.run}.fits"
        interleaved_output_file = options.directory / f"interleaved_LST-1.Run{self.run}.h5"
        dl1_output_file.unlink(missing_ok=True)
        muon_output_file.unlink(missing_ok=True)
        interleaved_output_file.unlink(missing_ok=True)

    def _remove_dl1b_output(self, file_prefix):
        dl1ab_subdirectory = options.directory / options.dl1_prod_id
        output_file = dl1ab_subdirectory / f"{file_prefix}{self.run}.h5"
        output_file.unlink(missing_ok=True)

    def _write_checkpoint(self):
        """Write the checkpoint in the history file."""
        history(
            run=self.run,
            prod_id=options.prod_id,
            stage=self.command,
            return_code=self.rc,
            history_file=self.history_file,
            config_file=self.config_file,
        )


class DRS4PedestalStage(AnalysisStage):
    """
    Class inheriting from AnalysisStage for the first part of the calibration procedure,
    i.e. the pedestal subtraction.

    Notes
    -----
    The history file is the same for both calibration stages and carries the name of
    the pedcal run.
    """

    def __init__(
        self,
        run: str,
        run_pedcal: str,
        command_args: List[str],
        config_file: Union[str, None] = None,
    ):
        super().__init__(run, command_args, config_file)
        self.run_pedcal = run_pedcal
        self.history_file = (
            Path(options.directory) / f"sequence_{options.tel_id}_{self.run_pedcal}.history"
        )

    def _write_checkpoint(self):
        """Write the checkpoint in the history file."""
        history(
            run=self.run,
            prod_id=options.prod_id,
            stage=self.command,
            return_code=self.rc,
            history_file=self.history_file,
        )


class ChargeCalibrationStage(AnalysisStage):
    """Class inheriting from AnalysisStage for the second part of the calibration procedure."""

    def __init__(
        self,
        run: str,
        command_args: List[str],
        config_file: Union[str, None] = None,
    ):
        super().__init__(run, command_args, config_file)
        self.history_file = (
            Path(options.directory) / f"sequence_{options.tel_id}_{self.run}.history"
        )

    def _write_checkpoint(self):
        """Write the checkpoint in the history file."""
        history(
            run=self.run,
            prod_id=options.prod_id,
            stage=self.command,
            return_code=self.rc,
            history_file=self.history_file,
        )
