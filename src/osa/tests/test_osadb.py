from freezegun import freeze_time

from osa.configs import options


@freeze_time("2021-12-22 01:02:03")
def test_start_processing_db(database):
    from osa.osadb import start_processing

    options.test = options.simulate = False

    start_processing("2020-01-01")

    database.execute("SELECT * FROM processing WHERE date = ?", ("2020-01-01",))
    assert database.fetchone() == ("LST1", "2020-01-01", "v0.1.0", "2021-12-22 01:02:03", None, 0)
    options.test = options.simulate = True
    assert start_processing("2020-01-01") is None


@freeze_time("2021-12-22 04:05:06")
def test_end_processing_db(database):
    from osa.osadb import end_processing

    options.test = options.simulate = False

    end_processing("2020-01-01")

    database.execute("SELECT * FROM processing WHERE date = ?", ("2020-01-01",))
    assert database.fetchone() == (
        "LST1",
        "2020-01-01",
        "v0.1.0",
        "2021-12-22 01:02:03",
        "2021-12-22 04:05:06",
        1,
    )
    # Test that there is only one entry or row corresponding to the date 2020-01-01
    # even if end_processing is called twice.
    end_processing("2020-01-01")
    database.execute("SELECT * FROM processing WHERE date = ?", ("2020-01-01",))
    query = database.execute("SELECT * FROM processing WHERE date = ?", ("2020-01-01",))
    assert len(query.fetchall()) == 1

    options.test = options.simulate = True

    assert end_processing("2020-01-01") is None


def test_no_osadb():
    from osa.osadb import open_database

    with open_database("/tmp/osa.db") as cursor:
        assert cursor is None
