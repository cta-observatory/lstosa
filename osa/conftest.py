import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def temp_dir():
    """Shared temporal directory for the tests."""
    with tempfile.TemporaryDirectory(prefix="test_lstchain") as d:
        yield Path(d)
