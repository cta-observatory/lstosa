import filecmp
from os import remove, rename
from os.path import exists, isfile

from osa.utils import options
from osa.utils.standardhandle import error, gettag, verbose


##############################################################################
#
# readfromfile
#
##############################################################################
def readfromfile(file):
    tag = gettag()
    if exists(file):
        if isfile(file):
            try:
                with open(file, 'r') as f:
                    return f.read()
            except (IOError, OSError) as e:
                error(tag, "{0} {1}".format(e.strerror, e.filename), e.errno)
        else:
           error(tag, "{0} is not a file".format(file), 1)
    else:
        error(tag, "File does not exists {0}".format(file), 1)
##############################################################################
#
# writetofile
#
##############################################################################
def writetofile(f, content):
    tag = gettag()
    ftemp= f + '.tmp'
    try:
        with open(ftemp, 'w') as filehandle:
            filehandle.write("{0}".format(content))
    except (IOError, OSError) as e:
        error(tag, "{0} {1}".format(e.strerror, e.filename), e.errno)
    
    if exists(f):
        if filecmp.cmp(f, ftemp):
            remove(ftemp)
            return False
        else:
            if options.simulate:
                remove(ftemp)
                verbose(tag, "SIMULATE File {0} would replace {1}. Deleting {0}".format(ftemp, f))
            else:
                try:
                    rename(ftemp, f)
                except (IOError, OSError) as e:
                    error(tag, "{0} {1}".\
                     format(e.strerror, e.filename), e.errno)
    else:
        if options.simulate:
            verbose(tag, "SIMULATE File {0} would be written as {1}. Deleting {0}".format(ftemp, f))
        else:
            rename(ftemp, f)
    return True
##############################################################################
#
# appendtofile
#
##############################################################################
def appendtofile(f, content):
    tag = gettag()
    if exists(f) and isfile(f):
        if options.simulate:
            verbose(tag, "SIMULATE File {0} would be appended".format(f)) 
        else:
            with open(f, 'a') as filehandle:
                try:
                    filehandle.write(content)
                except IOError as e:
                    error(tag, "{0} {1}".\
                     format(e.strerror, e.filename), e.errno)
    else:
        writetofile(f, content)
    return True
##############################################################################
#
# sedsi (an equivalent to sed s///g -i)
#
##############################################################################
def sedsi(pattern, replace, file):
    tag = gettag()
    old_content = readfromfile(file)
    new_content = old_content.replace(pattern, replace)
    writetofile(file, new_content)
