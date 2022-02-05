import re

from setuptools import find_packages, setup

with open("osa/version.py") as f:
    __version__ = re.search('^__version__ = "(.*)"$', f.read()).group(1)

entry_points = {
    "console_scripts": [
        "sequencer = osa.scripts.sequencer:main",
        "closer = osa.scripts.closer:main",
        "copy_datacheck = osa.scripts.copy_datacheck:main",
        "datasequence = osa.scripts.datasequence:main",
        "sequencer_webmaker = osa.scripts.sequencer_webmaker:main",
        "show_run_summary = osa.scripts.show_run_summary:main",
        "provprocess = osa.scripts.provprocess:main",
        "simulate_processing = osa.scripts.simulate_processing:main",
        "calibration_pipeline = osa.scripts.calibration_pipeline:main",
        "dl3_stage = osa.workflow.dl3:main",
        "source_coordinates = osa.nightsummary.set_source_coordinates:main",
    ]
}

docs_require = [
    "sphinx",
    "sphinx_rtd_theme",
    "sphinx_automodapi",
    "sphinx_argparse",
    "sphinx-autoapi",
    "numpydoc"
]
tests_require = [
    "pytest",
    "pytest-cov",
    "pytest-runner",
    "pytest-order"
]

extras_require = {
    "all": tests_require + docs_require,
    "tests": tests_require,
    "docs": docs_require,
}

setup(
    version=__version__,
    packages=find_packages(),
    extras_require=extras_require,
    install_requires=[
        "astropy~=4.2",
        "lstchain~=0.9.0",
        "ctapipe~=0.12.0",
        "matplotlib~=3.5",
        "pyparsing~=2.4",
        "numpy<1.22.0a0",
        "pandas",
        "pyyaml",
        "prov",
        "pydot",
        "pydotplus",
        "psutil",
        "click",
        "pymongo",
        "gammapy~=0.19.0",
    ],
    package_data={
        'osa': [
            'provenance/config/definition.yaml',
            'provenance/config/logger.yaml',
        ],
    },
    entry_points=entry_points,
)
