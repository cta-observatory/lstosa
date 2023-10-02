"""
Interact with the OSA database.

Deal with the OSA database (observations, analysis, transfers).
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))


__all__ = ["start_processing", "end_processing", "open_database"]


@contextmanager
def open_database(filename: str):
    """Open a database connection using as a context manager."""
    if not Path(filename).exists():
        log.warning(f"Database file '{filename}' not found.")
        yield None
        return

    connection = sqlite3.connect(filename)

    try:
        yield connection.cursor()
    finally:
        connection.commit()
        connection.close()


def start_processing(date: str) -> None:
    """Indicate the start of the processing for a given date in the OSA database."""
    if options.test or options.simulate:
        return

    t_start = datetime.now()
    finished = False

    database = cfg.get("database", "path")

    with open_database(database) as cursor:
        if cursor is not None:
            cursor.execute("SELECT * FROM processing WHERE date = ?", (date,))

            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO processing (telescope, date, start, prod_id, is_finished) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        options.tel_id,
                        date,
                        t_start,
                        options.prod_id,
                        finished,
                    ),
                )


def end_processing(date: str) -> None:
    """Indicate the end of the processing for a given date in the OSA database."""
    if options.test or options.simulate:
        return

    database = cfg.get("database", "path")
    finished = True
    t_end = datetime.now()

    # Once the night processing finishes, update the last two columns ('end' and
    # 'is_finished') of the corresponding row of a given date in the processing table.
    with open_database(database) as cursor:
        if cursor is not None:
            cursor.execute(
                "UPDATE processing SET is_finished = ?, end = ? WHERE date = ?",
                (finished, t_end, date),
            )
