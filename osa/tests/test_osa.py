import os
import sys
import tempfile
from pathlib import Path
import shutil
import re
import glob
import pytest


@pytest.mark.firt
def test_check_test_files(test_data):
    """
    Function to clean the test files created by the previous test
    """
    assert os.path.exists(test_data[0])  # Analysis directory
    assert os.path.exists(test_data[1])  # DL1 file
    assert os.path.exists(test_data[2])  # Raw directory


@pytest.mark.last
def test_clean_test_files(test_data):
    """
    Function to clean the test files created by the previous test
    """
    import shutil

    shutil.rmtree(test_data[0])
    # FIXME: produce this prov.log file inside
    if os.path.exists("prov.log"):
        os.remove("prov.log")
