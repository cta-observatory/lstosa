"""Open the lstosa configuration file."""

import configparser
import logging
import sys
from pathlib import Path

from importlib.resources import files

from osa.configs import options
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

__all__ = ["read_config", "cfg", "DEFAULT_CFG"]


DEFAULT_CFG = files("osa").joinpath("configs/sequencer.cfg")


def read_config():
    """
    Read cfg lstosa config file

    Returns
    -------
    config: ConfigParser
        Configuration file cfg
    """

    for idx, arg in enumerate(sys.argv):
        if arg in ["-c", "--config"]:
            options.configfile = sys.argv[idx + 1]
            break
        else:
            options.configfile = DEFAULT_CFG

    file = options.configfile

    if not Path(file).exists():
        raise FileNotFoundError(f"Configuration file {file} not found.")

    config = configparser.ConfigParser(allow_no_value=True)
    try:
        config.read(file)
    except configparser.Error as err:
        log.exception(err)
    log.debug(f"Sections of the config file are {config.sections()}")
    return config


cfg = read_config()
