from osa.nightsummary import database

def test_query():
    result = database.query(obs_id=20038)
    assert result is None



