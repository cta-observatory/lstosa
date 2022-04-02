from datetime import datetime
from pathlib import Path

from osa.configs.config import cfg


def test_get_night_summary_file():
    from osa.nightsummary.nightsummary import get_run_summary_file

    date = datetime.fromisoformat("2020-01-01")
    summary_filename = get_run_summary_file(date)
    assert summary_filename == Path(cfg.get("LST1", "RUN_SUMMARY_DIR")) / "RunSummary_20200101.ecsv"


def test_run_summary_table(run_summary_file):
    from osa.nightsummary.nightsummary import run_summary_table

    assert run_summary_file.exists()

    date = datetime.fromisoformat("2020-01-17")
    summary = run_summary_table(date)
    assert "run_id" in summary.columns


def test_produce_run_summary_file():
    from osa.nightsummary.nightsummary import produce_run_summary_file

    date = datetime.fromisoformat("2020-01-01")
    produce_run_summary_file(date)
    summary_file = Path(cfg.get("LST1", "RUN_SUMMARY_DIR")) / "RunSummary_20200101.ecsv"
    assert summary_file.exists()
