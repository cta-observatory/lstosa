from osa.nightsummary import database

def test_query():
    result = database.query(obs_id=20038)
    assert result.source_name == "1ES_1101-232"
    assert result.ra == 165.906
    assert result.dec == -23.492


