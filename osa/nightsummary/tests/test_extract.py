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


def test_no_calib_found(merged_run_summary, run_summary_file_no_calib, run_summary_file_no_calib2):
    """Test that for a day with no PEDCALIB run, the PEDCALIB
     run of another previous day is taken."""
    from osa.nightsummary.extract import extract_runs
    from osa.nightsummary.nightsummary import run_summary_table
    from osa.configs import options

    assert run_summary_file_no_calib.exists()
    assert run_summary_file_no_calib2.exists()
    assert merged_run_summary.exists()

    date = datetime.fromisoformat("2022-09-23")
    options.date = date
    summary_table = run_summary_table(date)
    run_list = extract_runs(summary_table)

    for run in run_list:
        if run.type == 'PEDCALIB':
            pedcalib_run = run

    assert pedcalib_run.run == 9258
    assert pedcalib_run.night == "2022-09-22"

    options.date = datetime.fromisoformat("2020-01-17")
