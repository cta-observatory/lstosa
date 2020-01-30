# Python module used for writing in different modes to stdout and stderr
from __future__ import print_function
from . import options
##############################################################################
#
# gettag
#
##############################################################################
def gettag():
    from os.path import basename
    import inspect
    parentfile = basename(inspect.stack()[1][1])
    parentmodule = inspect.stack()[1][3]
    string = "{0}({1})".format(parentfile , parentmodule)
    return string
##############################################################################
#
# output
#
##############################################################################
def output(block, message):
    import sys
    try:
        if options.stdout:            
            sys.stdout = open(options.stdout, 'a')
    finally:
        filehandle = sys.stdout
        print(message, file=filehandle)
        sys.stdout.flush()
##############################################################################
#
# printinfo
#
##############################################################################
def printinfo(filehandle, concept, block, message):
    print("{0} [{1}]: {2}".format(concept, block, message), file=filehandle)
##############################################################################
#
# verbose
#
##############################################################################
def verbose(block, message):
    import sys
    if options.verbose:
        try:
            if options.stdout:
                sys.stdout = open(options.stdout, 'a')
        finally:
            filehandle = sys.stdout
            printinfo(filehandle, 'VERBOSE', block, message)
            sys.stdout.flush()
##############################################################################
#
# warning
#
##############################################################################
def warning(block, message):
    import sys
    if options.warning:
        try:
            if options.stderr:
                sys.stderr = open(options.stderr, 'a')
        finally:
            filehandle = sys.stderr
            printinfo(filehandle, 'WARNING', block, message)
            sys.stderr.flush()
##############################################################################
#
# errornonfatal
#
##############################################################################
def errornonfatal(block, message):
    import sys
    try:
        if options.stderr:
            sys.stderr = open(options.stderr, 'a')
    finally:
        filehandle = sys.stderr
        printinfo(filehandle, 'ERROR', block, message)
        sys.stderr.flush()
##############################################################################
#
# error
#
##############################################################################
def error(block, message, ValueError):
    from sys import exit
    errornonfatal(block, message)
    exit(ValueError)
##############################################################################
#
# stringify
#
##############################################################################
def stringify(args):
    s = ' '.join(map(str, args))
    return s
