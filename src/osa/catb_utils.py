import logging
import re
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))


BASE_SERVICE = Path(
    "/fefs/onsite/data/lst-pipe/LSTN-01/service/PixelCalibration/Cat-A"
)


def load_calibration_table() -> str:
    if not cfg.has_option(options.tel_id, "TABLE_CATB"):
        raise RuntimeError(
            f"Missing TABLE_CATB option in [{options.tel_id}] config section"
        )
    table_file = Path(cfg.get(options.tel_id, "TABLE_CATB"))
    if not table_file.exists():
        raise FileNotFoundError(f"Cat-B calibration table not found: {table_file}")

    log.info(f"Using Cat-B calibration table: {table_file}")

    return table_file.read_text()


def parse_calibration_table(table_text: str) -> list[dict]:
    periods: list[dict] = []

    for line in table_text.splitlines():

        if "since" not in line:
            continue

        match = re.search(r"since\s+(\d{8})\s+\(r(\d+)\)", line)
        if not match:
            continue

        since_run = int(match.group(2))

        calib_matches = re.findall(r"(\d{8})\s+\(r(\d+)\)", line)

        if len(calib_matches) < 3:
            continue

        calib_date, calibration_run = calib_matches[1]
        ffactor_date, ffactor_run = calib_matches[-2]

        periods.append(
            {
                "since_run": since_run,
                "calib_date": calib_date,
                "calibration_run": int(calibration_run),
                "ffactor_date": ffactor_date,
                "ffactor_run": int(ffactor_run),
            }
        )

    return sorted(periods, key=lambda x: x["since_run"], reverse=True)


def find_period_for_run(run_id: int, periods: list[dict]) -> dict:

    for period in periods:
        if run_id >= period["since_run"]:
            return period

    log.warning(
        f"Run {run_id} prior to the first period in calibration table, using fallback"
    )

    return periods[-1]


def find_catA_file(calib_date: str, calibration_run: int) -> str:
    base_service = Path(
        cfg.get(options.tel_id, "CAT_A_CALIB_BASE", fallback=str(BASE_SERVICE))
    )
    path = base_service / "calibration" / calib_date / "pro"
    files = sorted(path.glob(f"*Run{calibration_run:05d}*.fits*"))

    if not files:
        raise RuntimeError(f"No Cat-A file for run {calibration_run} in {path}")

    return str(files[0])


def find_systematics_file(calib_date: str) -> str:

    base_service = Path(
        cfg.get(options.tel_id, "CAT_A_CALIB_BASE", fallback=str(BASE_SERVICE))
    )
    base_dir = base_service / "ffactor_systematics" / calib_date

    files = sorted(base_dir.rglob("*.h5"))

    if not files:
        raise RuntimeError(f"No systematics for date {calib_date} in {base_dir}")
    return str(files[0])


def get_catA_and_systematics(run_id: int) -> tuple[str, str]:

    table_text = load_calibration_table()

    periods = parse_calibration_table(table_text)
    if not periods:
        raise ValueError(
            "No valid calibration periods found in Cat-B calibration table"
        )

    period = find_period_for_run(run_id, periods)

    calib_date = period["calib_date"]
    calibration_run = period["calibration_run"]
    ffactor_date = period["ffactor_date"]

    catA_file = find_catA_file(calib_date, calibration_run)
    systematics_file = find_systematics_file(ffactor_date)

    return catA_file, systematics_file
