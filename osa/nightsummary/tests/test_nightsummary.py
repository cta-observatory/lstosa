from pathlib import Path


def test_get_night_summary_file():
    from osa.nightsummary.nightsummary import get_run_summary_file
    from osa.configs.config import cfg

    cfg.get("LST1", "RUN_SUMMARY_DIR")
    summary_filename = get_run_summary_file("2020_01_01")
    assert summary_filename == Path(cfg.get("LST1", "RUN_SUMMARY_DIR")) /\
           "RunSummary_20200101.ecsv"


def test_run_summary_table(run_summary_file):
    from osa.nightsummary.nightsummary import run_summary_table

    assert run_summary_file.exists()

    date = "2020_01_17"
    summary = run_summary_table(date)

    assert "run_id" in summary.columns
