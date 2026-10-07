import argparse
import logging
import subprocess as sp
import sys
from pathlib import Path
from datetime import datetime

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import get_last_pedcalib
from osa.paths import (
    analysis_path,
    catB_calibration_file_exists,
    get_major_version,
)
from osa.utils.cliopts import valid_date
from osa.utils.logging import myLogger
from osa.utils.utils import (
    get_calib_filters,
    get_lstchain_version,
)

# TODO:
# Import from common module instead of sequencer_catB_tailcuts
from osa.catb_utils import (
    get_catA_and_systematics,
)
from osa.job import CAT_A_DATACHECK_DIR


log = myLogger(logging.getLogger(__name__))


def _run(cmd: list[str]) -> int:
    """Log and execute a command, honouring options.simulate."""
    log.info(f"Command: {' '.join(cmd)}")
    if options.simulate:
        return 0
    return sp.run(cmd).returncode


# ---------------------------------------------------------------
# Cat-A datacheck merge
#
# The per-subrun Cat-A datacheck files are produced earlier, in the same job
# as r0->dl1 (see datasequence.py). Here we only merge them into a single
# per-run file, before running CatB and the tailcuts finder. The longterm
# datacheck is produced separately, at the end of the night's processing.
# ---------------------------------------------------------------
def _cat_a_datacheck_dir(analysis_dir: Path) -> Path:
    return analysis_dir / CAT_A_DATACHECK_DIR


def _check_cat_A_datacheck_exists(analysis_dir: Path, run_id: int, merged: bool) -> bool:
    """
    Check if datacheck files exist.

    For merged=False, require that *every* DL1 subrun has its datacheck.
    """
    output_directory = _cat_a_datacheck_dir(analysis_dir)

    if merged:
        return (output_directory / f"datacheck_dl1_LST-1.Run{run_id:05d}.h5").exists()

    subruns = {
        f.name.split(".")[-2]
        for f in analysis_dir.glob(f"dl1_LST-1.Run{run_id:05d}.*.h5")
    }
    checked = {
        f.name.split(".")[-2]
        for f in output_directory.glob(f"datacheck_dl1_LST-1.Run{run_id:05d}.*.h5")
    }

    missing = subruns - checked
    if missing:
        log.warning(
            f"Run {run_id:05d}: missing datacheck for subruns {sorted(missing)}"
        )

    return bool(subruns) and not missing


def _produce_cat_A_datacheck_merge(analysis_dir: Path, run_id: int) -> list[str]:
    """
    Build the lstchain_check_dl1 command that merges the per-subrun Cat-A
    datacheck files of a run into a single one.

    The wildcard is passed unexpanded: sp.run() does not go through a shell,
    so lstchain_check_dl1 receives the pattern literally and expands it itself.
    """
    output_directory = _cat_a_datacheck_dir(analysis_dir)
    pattern = output_directory / f"datacheck_dl1_LST-1.Run{run_id:05d}.*.h5"

    if not sorted(pattern.parent.glob(pattern.name)):
        raise FileNotFoundError(
            f"No per-subrun Cat-A datacheck files found for run {run_id:05d}"
        )

    command = cfg.get("lstchain", "check_dl1").split()

    return command + [
        f"--input-file={pattern}",
        f"--output-dir={output_directory}",
        f"--muons-dir={analysis_dir}",
        "--batch",
    ]


def _merge_cat_a_datacheck(analysis_dir: Path, run_id: int) -> int:
    """
    Merge the per-subrun Cat-A datacheck files (produced by the r0->dl1 job)
    into a single per-run file.

    Returns 0 both on success and when the merge is postponed (per-subrun
    files not ready yet); returns non-zero only on an actual failure.
    """
    if _check_cat_A_datacheck_exists(analysis_dir, run_id, merged=True):
        log.info(f"Cat-A datacheck already merged for run {run_id:05d}")
        return 0

    if not _check_cat_A_datacheck_exists(analysis_dir, run_id, merged=False):
        log.info(
            f"Per-subrun Cat-A datacheck not ready yet for run {run_id:05d}; "
            "postponing CatB and tailcuts."
        )
        return 0

    log.info(f"Merging Cat-A datacheck for run {run_id:05d}")

    try:
        cmd = _produce_cat_A_datacheck_merge(analysis_dir, run_id)
    except FileNotFoundError as err:
        log.error(str(err))
        return 1

    rc = _run(cmd)
    if rc != 0:
        return rc

    # rc == 0 does not guarantee the file is there (e.g. OOM on a subrun)
    if not options.simulate and not _check_cat_A_datacheck_exists(
        analysis_dir, run_id, merged=True
    ):
        log.error(f"Cat-A datacheck merge did not produce the merged file for run {run_id:05d}")
        return 1

    return 0


# ---------------------------------------------------------------
# Cat-B calibration and tailcuts
# ---------------------------------------------------------------
def _catb_command_args(run_id: int) -> list[str]:

    command = cfg.get(
        "lstchain",
        "catB_calibration",
    )

    if cfg.getboolean(
        "lstchain",
        "use_lstcam_env_for_CatB_calib",
    ):
        base_cmd = (
            ["conda", "run", "-n", "lstcam-env"]
            + command.split()
        )
    else:
        base_cmd = command.split()

    filters = get_calib_filters(run_id)

    base_dir = Path(
        cfg.get(options.tel_id, "BASE")
    ).resolve()

    r0_dir = Path(
        cfg.get(options.tel_id, "R0_DIR")
    ).resolve()

    lstchain_version = get_major_version(
        get_lstchain_version()
    )

    analysis_dir = cfg.get(
        options.tel_id,
        "ANALYSIS_DIR",
    )

    args = base_cmd + [
        "-r",
        f"{run_id:05d}",
        "-b",
        str(base_dir),
        f"--r0-dir={r0_dir}",
        f"--filters={filters}",
    ]

    if options.input_state == "catA_calibrated":

        catA_file, systematics_file = (
            get_catA_and_systematics(run_id)
        )

        log.info(
            f"[CatB] Using Cat-A file: {catA_file}"
        )

        log.info(
            f"[CatB] Using systematics: "
            f"{systematics_file}"
        )

        args.extend(
            [
                f"--cat_A_calibration_file={catA_file}",
                f"--systematics_file={systematics_file}",
            ]
        )

    else:

        catA_calib_run = get_last_pedcalib(
            options.date
        )

        args.append(
            f"--catA_calibration_run={catA_calib_run}"
        )

    if command == "onsite_create_cat_B_calibration_file":

        args.append(
            f"--interleaved-dir={analysis_dir}"
        )

    elif (
        command
        == "lstcam_calib_onsite_create_cat_B_calibration_file"
    ):

        args.append(
            f"--dl1-dir={analysis_dir}"
        )

        args.append(
            f"--lstchain-version={lstchain_version[1:]}"
        )

    if options.overwrite_catB:
        args.append("--yes")

    return args


def _tailcuts_command_args(run_id: int) -> list[str]:

    command = cfg.get(
        "lstchain",
        "tailcuts_finder",
    )

    input_dir = Path(options.directory)

    output_dir = Path(
        cfg.get(
            options.tel_id,
            "TAILCUTS_FINDER_DIR",
        )
    )

    return command.split() + [
        f"--input-dir={input_dir}",
        f"--run={run_id}",
        f"--output-dir={output_dir}",
    ]


def _tailcuts_config_file(run_id: int) -> Path:

    return (
        Path(
            cfg.get(
                options.tel_id,
                "TAILCUTS_FINDER_DIR",
            )
        )
        / f"dl1ab_Run{run_id:05d}.json"
    )


def parse_args() -> argparse.Namespace:

    p = argparse.ArgumentParser()

    p.add_argument(
        "--config",
        type=Path,
        default=None,
    )

    p.add_argument(
        "--date",
        required=True,
        type=valid_date,
        help="Night in YYYY-MM-DD format",
    )

    p.add_argument(
        "--input-state",
        choices=[
            "legacy_raw",
            "gain_selected",
            "catA_calibrated",
        ],
        default="legacy_raw",
    )

    p.add_argument(
        "--overwrite-catB",
        action="store_true",
        default=False,
    )

    p.add_argument(
        "--overwrite-tailcuts",
        action="store_true",
        default=False,
    )

    p.add_argument(
        "--simulate",
        action="store_true",
        default=False,
    )

    p.add_argument(
        "--verbose",
        action="store_true",
        default=False,
    )

    p.add_argument(
        "run_id",
        type=int,
    )

    p.add_argument(
        "tel_id",
        choices=[
            "LST1",
            "LST2",
        ],
    )

    return p.parse_args()


def _write_history(run_id: int, exit_code: int) -> None:

    history_file = (
        Path(options.directory)
        / f"sequence_{options.tel_id}_{run_id:05d}.history"
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M"
    )

    version = get_major_version(
        get_lstchain_version()
    )

    with open(history_file, "a") as history:

        history.write(
            f"{run_id:05d} "
            f"catb_tailcuts_pipeline "
            f"{version} "
            f"{timestamp} "
            f"None "
            f"None "
            f"{exit_code}\n"
        )


def main() -> int:

    args = parse_args()

    options.tel_id = args.tel_id
    options.simulate = args.simulate

    options.overwrite_catB = args.overwrite_catB
    options.overwrite_tailcuts = args.overwrite_tailcuts
    options.input_state = args.input_state

    if args.config is not None:
        options.configfile = args.config.resolve()

    options.date = args.date

    options.directory = analysis_path(options.tel_id)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    run_id = args.run_id

    analysis_dir = Path(options.directory)

    catb_closed_file = analysis_dir / f"catB_{run_id:05d}.closed"

    #
    # Merge the per-subrun Cat-A datacheck files (produced by the r0->dl1 job)
    #
    rc = _merge_cat_a_datacheck(analysis_dir, run_id)

    if rc != 0:
        _write_history(run_id, rc)
        return rc

    # Do not touch the DL1 files until the Cat-A datacheck is merged:
    # CatB and the dl1ab re-processing overwrite them.
    if not _check_cat_A_datacheck_exists(analysis_dir, run_id, merged=True):
        return 0

    #
    # CatB calibration
    #
    if cfg.getboolean("lstchain", "apply_catB_calibration"):

        if catB_calibration_file_exists(run_id) and not options.overwrite_catB:

            log.info(
                f"CatB calibration already exists for run {run_id:05d}"
            )

        else:

            cmd = _catb_command_args(run_id)

            log.info(f"Running CatB calibration for run {run_id:05d}")

            rc = _run(cmd)

            if rc != 0:
                _write_history(run_id, rc)
                return rc

    #
    # Tailcuts finder
    #
    if not cfg.getboolean("lstchain", "apply_standard_dl1b_config"):

        cfg_file = _tailcuts_config_file(run_id)

        if cfg_file.exists() and not options.overwrite_tailcuts:

            log.info(
                f"Tailcuts config already exists for run {run_id:05d}: "
                f"{cfg_file.name}"
            )

        else:

            cmd = _tailcuts_command_args(run_id)

            log.info(f"Running tailcuts finder for run {run_id:05d}")

            rc = _run(cmd)

            if rc != 0:
                _write_history(run_id, rc)
                return rc

    #
    # Everything finished successfully
    #
    if not options.simulate:

        _write_history(run_id, 0)

        catb_closed_file.touch()

        log.info(f"Created {catb_closed_file.name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
