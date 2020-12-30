import filecmp
import logging
from os import remove, rename
from os.path import exists, isfile

from osa.configs import options

log = logging.getLogger(__name__)


def readfromfile(file):
    if exists(file):
        if isfile(file):
            try:
                with open(file, "r") as f:
                    return f.read()
            except (IOError, OSError) as e:
                log.exception(f"{e.strerror} {e.filename}")
        else:
            log.error(f"{file} is not a file")
    else:
        log.error(f"File does not exists {file}")


def writetofile(f, content):
    ftemp = f + ".tmp"
    try:
        with open(ftemp, "w") as filehandle:
            filehandle.write(f"{content}")
    except (IOError, OSError) as e:
        log.exception(f"{e.strerror} {e.filename}")

    if exists(f):
        if filecmp.cmp(f, ftemp):
            remove(ftemp)
            return False
        else:
            if options.simulate:
                remove(ftemp)
                log.debug(f"SIMULATE File {ftemp} would replace {f}. Deleting {ftemp}")
            else:
                try:
                    rename(ftemp, f)
                except (IOError, OSError) as e:
                    log.exception(f"{e.strerror} {e.filename}")
    else:
        if options.simulate:
            log.debug(f"SIMULATE File {ftemp} would be written as {f}. Deleting {ftemp}")
        else:
            rename(ftemp, f)
    return True


def appendtofile(f, content):
    if exists(f) and isfile(f):
        if options.simulate:
            log.debug(f"SIMULATE File {f} would be appended")
        else:
            with open(f, "a") as filehandle:
                try:
                    filehandle.write(content)
                except IOError as e:
                    log.exception(f"{e.strerror} {e.filename}")
    else:
        writetofile(f, content)
    return True


def sedsi(pattern, replace, file):
    old_content = readfromfile(file)
    new_content = old_content.replace(pattern, replace)
    writetofile(file, new_content)
