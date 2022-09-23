import pytest
from pymongo.errors import ConnectionFailure


def test_query():
    from osa.nightsummary import database

    with pytest.raises(ConnectionFailure):
        database.query(obs_id=1616, property_name="DriveControl_SourceName")
