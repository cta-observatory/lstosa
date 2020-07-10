import tempfile
import sys
from configparser import SafeConfigParser
from os import unlink
from pathlib import Path

from osa.utils import options
from osa.utils.standardhandle import error, gettag, verbose


def readconf():

    for idx, arg in enumerate(sys.argv):
        if arg == "-c" or arg == "--config":
            options.configfile = sys.argv[idx+1]
    file = options.configfile

    if not Path(file).exists():
        raise FileNotFoundError("Configuration file not found.")

    tag = gettag()
    conf = SafeConfigParser(allow_no_value=True)
    try:
        conf.read(file)
    except SafeConfigParser.Error as err:
        error(tag, err, 3)
    verbose(tag, "sections of the config file are {0}".format(conf.sections()))
    return conf


def read_properties(file):
    """ To be used when config file has no header, creating a DUMMY header"""

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write("[DUMMY]\n")
        with open(file) as f:
            tf.write(f.read())
        tf.seek(0)
        conf = readconf(tf.name)
    unlink(tf.name)
    return conf


cfg = readconf()
