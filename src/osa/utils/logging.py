"""Tweaks for logging module. Custom formatter for the different logging levels."""

import logging

DEFAULT_LOGGING_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] (%(module)s.%(funcName)s): %(message)s"
)

__all__ = ["myLogger"]


class MyFormatter(logging.Formatter):
    """Customize formatter of info logging level."""

    info_fmt = "%(message)s"

    def __init__(self):
        super().__init__(fmt=DEFAULT_LOGGING_FORMAT, datefmt=None, style="%")

    def format(self, record):
        """
        Parameters
        ----------
        record: logging record

        Returns
        -------
        result: logging formatter

        """
        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.INFO:
            self._style._fmt = MyFormatter.info_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = format_orig

        return result


def myLogger(osalogger):
    """Creates a logger with a customized formatted handler."""
    fmt = MyFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    osalogger.addHandler(handler)
    osalogger.propagate = False
    return osalogger
