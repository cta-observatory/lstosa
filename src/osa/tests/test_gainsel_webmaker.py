def test_gain_selection_webmaker(
    run_summary,
    merged_run_summary,
    drs4_time_calibration_files,
    systematic_correction_files,
    base_test_dir,
):


    output = sp.run(
        ["gainsel_webmaker", "--test", "-d", "2020-01-17"],
        text=True,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
    )
    assert output.returncode != 0
