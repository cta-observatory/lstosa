import tempfile
from pathlib import Path

import pytest

from osa.configs import options
from osa.nightsummary.extract import extractruns, extractsubruns, extractsequences
from osa.nightsummary.nightsummary import run_summary_table


@pytest.fixture(scope="session")
def temp_dir():
    """Shared temporal directory for the tests."""
    with tempfile.TemporaryDirectory(prefix="test_lstchain") as d:
        yield Path(d)


@pytest.fixture(scope="session")
def sequence_list(temp_dir):
    """
    Creates a sequence list from a run summary file
    """
    # building the sequences
    options.directory = temp_dir
    options.date = "20200117"
    options.simulate = True
    summary = run_summary_table(options.date)
    subrun_list = extractsubruns(summary)
    run_list = extractruns(subrun_list)
    # modifies run_list by adding the seq and parent info into runs
    seq_list = extractsequences(run_list)

    return seq_list
