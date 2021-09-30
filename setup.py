import re

from setuptools import find_packages, setup

with open("osa/version.py") as f:
    __version__ = re.search('^__version__ = "(.*)"$', f.read()).group(1)

entry_points = {
    "console_scripts": [
        "sequencer = osa.scripts.sequencer:main",
        "closer = osa.scripts.closer:main",
        "autocloser = osa.scripts.autocloser:main",
        "calibrationsequence = osa.scripts.calibrationsequence:main",
        "copy_datacheck = osa.scripts.copy_datacheck:main",
        "datasequence = osa.scripts.datasequence:main",
        "provprocess = osa.scripts.provprocess:main",
        "simulate_processing = osa.scripts.simulate_processing:main",
        "sequencer_webmaker = osa.scripts.sequencer_webmaker:main",
        "show_run_summary = osa.scripts.show_run_summary:main",
    ]
}

extras_require = {
    "docs": [
        "sphinx",
        "sphinx_rtd_theme",
        "sphinx_automodapi",
        "sphinx_argparse",
        "sphinx-autoapi",
    ],
    "tests": ["pytest", "pytest-cov", "pytest-runner", "pytest-order"],
}

extras_require["all"] = list(set(extras_require["tests"] + extras_require["docs"]))

setup(
    version=__version__,
    packages=find_packages(),
    extras_require=extras_require,
    install_requires=[
        "astropy~=4.2",
        "lstchain~=0.7.3",
        "matplotlib",
        "numpy",
        "pandas",
        "pyyaml",
        "prov",
        "pydot",
        "pydotplus",
    ],
    python_requires="~=3.7",
    entry_points=entry_points,
)
