from osa.configs import options


def test_conda_env_export(running_analysis_dir):
    from osa.provenance.utils import store_conda_env_export

    options.directory = running_analysis_dir
    store_conda_env_export()
    conda_export = options.directory / "log" / "conda_env.yml"
    assert conda_export.exists()
