import subprocess as sp


def test_dl3_stage():
    output = sp.run(
        ["dl3_stage", "-d", "2020-01-17", "-s", "LST1"], text=True, stdout=sp.PIPE, stderr=sp.PIPE
    )
    assert output.returncode == 0
    assert "Creating observation index for each source." in output.stderr.splitlines()[-1]
