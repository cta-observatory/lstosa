def test_get_nightsummary_file():
    from osa.nightsummary.nightsummary import get_runsummary_file

    summary_filename = get_runsummary_file("2020_01_01")
    assert summary_filename == "/fefs/aswg/data/real/monitoring/RunSummary/RunSummary_20200101.ecsv"
