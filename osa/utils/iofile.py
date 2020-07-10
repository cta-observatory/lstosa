import filecmp
from os import remove, rename
from os.path import exists, isfile

from osa.utils import options
from osa.utils.standardhandle import error, gettag, verbose


def readfromfile(file):
    tag = gettag()
    if exists(file):
        if isfile(file):
            try:
                with open(file, "r") as f:
                    return f.read()
            except (IOError, OSError) as e:
                error(tag, f"{e.strerror} {e.filename}", e.errno)
        else:
            error(tag, f"{file} is not a file", 1)
    else:
        error(tag, f"File does not exists {file}", 1)


def writetofile(f, content):
    tag = gettag()
    ftemp = f + ".tmp"
    try:
        with open(ftemp, "w") as filehandle:
            filehandle.write(f"{content}")
    except (IOError, OSError) as e:
        error(tag, f"{e.strerror} {e.filename}", e.errno)

    if exists(f):
        if filecmp.cmp(f, ftemp):
            remove(ftemp)
            return False
        else:
            if options.simulate:
                remove(ftemp)
                verbose(tag, f"SIMULATE File {ftemp} would replace {f}. Deleting {ftemp}")
            else:
                try:
                    rename(ftemp, f)
                except (IOError, OSError) as e:
                    error(tag, f"{e.strerror} {e.filename}", e.errno)
    else:
        if options.simulate:
            verbose(tag, f"SIMULATE File {ftemp} would be written as {f}. Deleting {ftemp}")
        else:
            rename(ftemp, f)
    return True


def appendtofile(f, content):
    tag = gettag()
    if exists(f) and isfile(f):
        if options.simulate:
            verbose(tag, f"SIMULATE File {f} would be appended")
        else:
            with open(f, "a") as filehandle:
                try:
                    filehandle.write(content)
                except IOError as e:
                    error(tag, f"{e.strerror} {e.filename}", e.errno)
    else:
        writetofile(f, content)
    return True


def sedsi(pattern, replace, file):
    old_content = readfromfile(file)
    new_content = old_content.replace(pattern, replace)
    writetofile(file, new_content)
