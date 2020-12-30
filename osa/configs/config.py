import logging
import sys
import tempfile
from configparser import ConfigParser
from os import unlink
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
    log.debug("sections of the config file are {0}".format(conf.sections()))
    return conf


def read_properties(file):
    """ To be used when config file has no header, creating a DUMMY header"""

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write("[DUMMY]\n")
        with open(file) as f:
            tf.write(f.read())
        tf.seek(0)
        conf = readconf()
    unlink(tf.name)
    return conf


cfg = readconf()
