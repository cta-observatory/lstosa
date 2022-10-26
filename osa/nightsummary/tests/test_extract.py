from datetime import datetime


obs_date = datetime.fromisoformat("2020-01-17")


def test_source_list(
    r0_data,
    run_summary_file,
    run_catalog,
    drs4_time_calibration_files,
    systematic_correction_files,
    pedestal_ids_file,
    merged_run_summary,
):
    """Test that the list of name of sources is correct."""
    from osa.nightsummary.extract import get_source_list

    sources = get_source_list(date=obs_date)
    assert list(sources) == ["Crab", "MadeUpSource"]
    assert sources == {"Crab": [1807], "MadeUpSource": [1808]}


def test_build_sequences(sequence_list):
    """Test that building of sequences."""
    from osa.nightsummary.extract import build_sequences

    extracted_seq_list = build_sequences(date=obs_date)
    for sequence, extracted_seq in zip(sequence_list, extracted_seq_list):
        assert sequence.run == extracted_seq.run


def test_extract_runs():
    from osa.nightsummary.extract import extract_runs
    from osa.nightsummary import run_summary_table

    date = datetime.fromisoformat("2022-09-23")
    summary_table = run_summary_table(date)
    run_list = extract_runs(summary_table)

    assert run_list[1].run == 9258
    assert run_list[1].night == "2022-09-22"
