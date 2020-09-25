"""Python module used for writing in different modes to stdout and stderr"""
import inspect
import sys
from datetime import datetime
from os.path import basename

from osa.configs import options


def gettag():
    parentfile = basename(inspect.stack()[1][1])
    parentmodule = inspect.stack()[1][3]
    string = f"{parentfile}({parentmodule})"
    return string


def output(block, message):
    try:
        if options.stdout:
            sys.stdout = open(options.stdout, "a")
    finally:
        filehandle = sys.stdout
        print(message, file=filehandle)
        sys.stdout.flush()


def printinfo(filehandle, concept, block, message):

    # dd/mm/YY H:M:S
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    print("{0} {1} [{2}]: {3}".format(timestamp, concept, block, message), file=filehandle)


def verbose(block, message):
    if options.verbose:
        try:
            if options.stdout:
                sys.stdout = open(options.stdout, "a")
        finally:
            filehandle = sys.stdout
            printinfo(filehandle, "VERBOSE", block, message)
            sys.stdout.flush()


def warning(block, message):
    if options.warning:
        try:
            if options.stderr:
                sys.stderr = open(options.stderr, "a")
        finally:
            filehandle = sys.stderr
            printinfo(filehandle, "WARNING", block, message)
            sys.stderr.flush()


def errornonfatal(block, message):
    try:
        if options.stderr:
            sys.stderr = open(options.stderr, "a")
    finally:
        filehandle = sys.stderr
        printinfo(filehandle, "ERROR", block, message)
        sys.stderr.flush()


def error(block, message, ValueError):
    errornonfatal(block, message)
    sys.exit(ValueError)


def stringify(args):
    s = " ".join(map(str, args))
    return s
