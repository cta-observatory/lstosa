import subprocess as sp


def test_significance(
    dl2_merged,
    run_summary_file,
    r0_data,
    run_catalog,
    drs4_time_calibration_files,
    systematic_correction_files,
    pedestal_ids_file,
    merged_run_summary,
):
    output = sp.run(
        ["theta2_significance", "-d", "2020-01-17", "-s", "LST1"],
        text=True,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
    )
    assert output.returncode == 0
    assert "Source: MadeUpSource, runs: [1808]" in output.stderr.splitlines()[-1]
