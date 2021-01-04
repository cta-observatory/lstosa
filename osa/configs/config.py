import logging
import sys
from configparser import ConfigParser
from pathlib import Path

from osa.configs import options

log = logging.getLogger(__name__)


def readconf():
    """
    Read cfg lstosa config file

    Returns
    -------
    conf: configuration file cfg

    """
    for idx, arg in enumerate(sys.argv):
        if arg in ["-c", "--config"]:
            options.configfile = sys.argv[idx + 1]

    file = options.configfile

    if not Path(file).exists():
        raise FileNotFoundError(f"Configuration file {file} not found.")

    conf = ConfigParser(allow_no_value=True)
    try:
        conf.read(file)
    except ConfigParser.Error as err:
        log.exception(err)
    log.debug(f"Sections of the config file are {conf.sections()}")
    return conf


cfg = readconf()
