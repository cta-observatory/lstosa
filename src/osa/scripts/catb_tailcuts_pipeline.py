"""
Worker script to run CatB calibration and tailcuts finder for a single run.

This is analogous to `datasequence.py`, but run-wise and without any SLURM
submission inside. It is meant to be executed from a pilot script submitted
with `sbatch` by `sequencer_catB_tailcuts.py`.
"""

import argparse
import logging
import subprocess as sp
import sys
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import get_last_pedcalib
from osa.paths import analysis_path, catB_calibration_file_exists, get_major_version
from osa.utils.cliopts import valid_date
from osa.utils.logging import myLogger
from osa.utils.utils import get_calib_filters, get_lstchain_version

log = myLogger(logging.getLogger(__name__))


def _catb_command_args(run_id: int) -> list[str]:
    command = cfg.get("lstchain", "catB_calibration")

    if cfg.getboolean("lstchain", "use_lstcam_env_for_CatB_calib"):
        base_cmd = ["conda", "run", "-n", "lstcam-env"] + command.split()
    else:
        base_cmd = command.split()

    filters = get_calib_filters(run_id)
    base_dir = Path(cfg.get(options.tel_id, "BASE")).resolve()
    r0_dir = Path(cfg.get(options.tel_id, "R0_DIR")).resolve()
    catA_calib_run = get_last_pedcalib(options.date)
    lstchain_version = get_major_version(get_lstchain_version())
    analysis_dir = cfg.get("LST1", "ANALYSIS_DIR")

    args = base_cmd + [
        "-r",
        f"{run_id:05d}",
        f"--catA_calibration_run={catA_calib_run}",
        "-b",
        str(base_dir),
        f"--r0-dir={r0_dir}",
        f"--filters={filters}",
    ]

    if command == "onsite_create_cat_B_calibration_file":
        args.append(f"--interleaved-dir={analysis_dir}")
    elif command == "lstcam_calib_onsite_create_cat_B_calibration_file":
        args.append(f"--dl1-dir={analysis_dir}")
        args.append(f"--lstchain-version={lstchain_version[1:]}")

    if options.overwrite_catB:
        args.append("--yes")

    return args


def _tailcuts_command_args(run_id: int) -> list[str]:
    command = cfg.get("lstchain", "tailcuts_finder")
    input_dir = Path(options.directory)
    output_dir = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR"))

    return command.split() + [
        f"--input-dir={input_dir}",
        f"--run={run_id}",
        f"--output-dir={output_dir}",
    ]


def _tailcuts_config_file(run_id: int) -> Path:
    return Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, default=None)
    p.add_argument("--date", required=True, type=valid_date, help="Night in YYYY-MM-DD format")
    p.add_argument("--overwrite-catB", action="store_true", default=False)
    p.add_argument("--overwrite-tailcuts", action="store_true", default=False)
    p.add_argument("--simulate", action="store_true", default=False)
    p.add_argument("--verbose", action="store_true", default=False)

    # Positional args at the end, like datasequence
    p.add_argument("run_id", type=int)
    p.add_argument("tel_id", choices=["ST", "LST1", "LST2", "all"])

    return p.parse_args()


def main() -> int:
    args = parse_args()

    options.tel_id = args.tel_id
    options.simulate = args.simulate
    options.overwrite_catB = args.overwrite_catB
    options.overwrite_tailcuts = args.overwrite_tailcuts

    if args.config is not None:
        options.configfile = args.config.resolve()

    options.date = args.date
    options.directory = analysis_path(options.tel_id)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    run_id = args.run_id
    catb_closed_file = Path(options.directory) / f"catB_{run_id:05d}.closed"

    # CatB calibration
    if cfg.getboolean("lstchain", "apply_catB_calibration"):
        if catB_calibration_file_exists(run_id) and not options.overwrite_catB:
            log.info(f"CatB calibration already exists for run {run_id:05d}")
        else:
            cmd = _catb_command_args(run_id)
            log.info(f"Running CatB calibration for run {run_id:05d}")
            log.debug(f"Command: {' '.join(cmd)}")
            if not options.simulate:
                rc = sp.run(cmd).returncode
                if rc != 0:
                    return rc
                # CatB finished successfully, create .closed flag for sequencer
                catb_closed_file.touch()

    # Tailcuts finder
    if not cfg.getboolean("lstchain", "apply_standard_dl1b_config"):
        cfg_file = _tailcuts_config_file(run_id)
        if cfg_file.exists() and not options.overwrite_tailcuts:
            log.info(f"Tailcuts config already exists for run {run_id:05d}: {cfg_file.name}")
        else:
            cmd = _tailcuts_command_args(run_id)
            log.info(f"Running tailcuts finder for run {run_id:05d}")
            log.debug(f"Command: {' '.join(cmd)}")
            if not options.simulate:
                rc = sp.run(cmd).returncode
                if rc != 0:
                    return rc

    return 0


if __name__ == "__main__":
    sys.exit(main())

