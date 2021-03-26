import os


def test_get_nightsummary_file():
    from osa.nightsummary.nightsummary import get_runsummary_file
    from osa.configs.config import cfg

    cfg.get("LST1", "RUN_SUMMARY_DIR")
    summary_filename = get_runsummary_file("2020_01_01")
    assert summary_filename == os.path.join(
        cfg.get("LST1", "RUN_SUMMARY_DIR"), "RunSummary_20200101.ecsv"
    )


def test_run_summary_table():
    from osa.nightsummary.nightsummary import run_summary_table

    date = "20200117"
    summary = run_summary_table(date)

    assert "run_id" in summary.columns
