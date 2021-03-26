"""
Tweaks for logging module.
Custom formatter for the different logging levels.
"""

import logging

DEFAULT_LOGGING_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] (%(module)s.%(funcName)s): %(message)s"
)


class MyFormatter(logging.Formatter):
    """
    Customize formatter of info logging level.
    """

    default_fmt = DEFAULT_LOGGING_FORMAT
    info_fmt = "%(message)s"

    def __init__(self):
        super().__init__(fmt="%(levelname)d: %(message)s", datefmt=None, style="%")

    def format(self, record):
        """
        Parameters
        ----------
        record

        Returns
        -------

        """
        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.INFO:
            self._style._fmt = MyFormatter.info_fmt

        else:
            self._style._fmt = MyFormatter.default_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = format_orig

        return result
