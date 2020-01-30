from osa.utils.standardhandle import error, verbose, gettag, warning
from osa.utils import options
##############################################################################
#
# readconf
#
##############################################################################
def readconf(file):
    tag = gettag()
    from os.path import exists

    conf = None

    try:
        # Python 2.7
        import ConfigParser
    except ImportError as Error:
        warning(tag, "Increasing to python 3 ConfigParser")
        import configparser
        conf = configparser.SafeConfigParser(allow_no_value=True)
    else:
        conf = ConfigParser.SafeConfigParser(allow_no_value=True)

    try:
        conf.read(file)
    except ConfigParser.Error as NameError:
        error(tag, NameError, 3)

    verbose(tag, "sections of the config file are {0}".format(conf.sections()))
    return conf
##############################################################################
#
# read_properties
#
##############################################################################
def read_properties(file):
    tag = gettag()

    """ To be used when config file has no header, creating a DUMMY header""" 
    import tempfile
    from os import unlink
    fname = None
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write('[DUMMY]\n')
        fname = tf.name
        with open(file) as f:
            tf.write(f.read())
        tf.seek(0)
        conf = readconf(tf.name)
    unlink(tf.name)
    return conf
##############################################################################
#
# cfg
#
##############################################################################
cfg = readconf(options.configfile)
