import subprocess as sp


def test_significance():
    output = sp.run(
        ["theta2_significance", "-d", "2001_02_03", "-s", "LST1"],
        text=True,
        stdout=sp.PIPE,
        stderr=sp.PIPE
    )
    assert output.returncode == 0
    assert "No runs found for this date. Nothing to do. Exiting." in output.stderr.splitlines()[-1]