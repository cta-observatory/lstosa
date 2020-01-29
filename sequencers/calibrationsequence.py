#!/usr/bin/env python2.7
from standardhandle import output, verbose, warning, error, stringify, gettag

__all__ = ["calibrationsequence"]

def calibrationsequence(args):
    tag = gettag()
    # This is the python interface to run sorcerer with the -c option for calibration
    # args: <RUN>    
    import sys
    import subprocess
    from os.path import join
    import options, cliopts
    import report
    from job import historylevel

    run = args[0]
    historyfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run + '.history')
    level, rc = historylevel(historyfile, 'CALIBRATION')
    verbose(tag, "Going to level {0}".format(level))
    if level == 2:
        rc = drs4_pedestal(run, historyfile)
        level -=1
        verbose(tag, "Going to level {0}".format(level))
    if level == 1:
        rc = calibrate(run, historyfile)
        level -=1
        verbose(tag, "Going to level {0}".format(level))
    if level == 0:
        verbose(tag, "Job for sequence {0} finished without fatal errors".format(run))
    return rc

##############################################################################
#
# DRS4 pedestal
#
##############################################################################
def drs4_pedestal(run, historyfile):
    tag = gettag()
    from sys import exit
    from os.path import join
    import subprocess
    from config import cfg
    from report import history
    from register import register_run_concept_files

    sequencetextfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run + '.txt')
    bindir = cfg.get('LSTOSA', 'LSTCHAINDIR')
    daqdir = cfg.get(options.tel_id, 'RAWDIR')
    carddir = cfg.get('LSTOSA', 'CARDDIR')
    inputcard = cfg.get(options.tel_id, 'CALIBRATIONCONFIGCARD')
    configcard = join(carddir, inputcard)
    commandargs = [join(bindir, cfg.get('PROGRAM', 'PEDESTAL'))]
    commandargs.append('-r' + predestal_run_id)
    commandargs.append('-v' + version_lstchain )
    print("COMAND for pedestal:",commandargs)
    commandconcept = 'drs4_pedestal'
    pedestalfile = 'drs4_pedestal'
    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except OSError as (ValueError, NameError):
        history(run, commandconcept, pedestalfile, inputcard, ValueError, historyfile)
        error(tag, "Could not execute \"{0}\", {1}".format(stringify(commandargs), NameError), ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(run, commandconcept, pedestalfile, inputcard, rc, historyfile)

    """ Error handling, for now no nonfatal errors are implemented for CALIBRATION """
  
    if rc != 0:
        exit(rc)
    return rc

##############################################################################
#
# calibrate 
#
##############################################################################
def calibrate(run, historyfile):
    tag = gettag()
    from sys import exit
    from os.path import join
    import subprocess
    from config import cfg
    from report import history
    from register import register_run_concept_files

    sequencetextfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run + '.txt')
    bindir = cfg.get('LSTOSA', 'LSTCHAINDIR')
    daqdir = cfg.get(options.tel_id, 'RAWDIR')
    carddir = cfg.get('LSTOSA', 'CARDDIR')
    inputcard = cfg.get(options.tel_id, 'CALIBRATIONCONFIGCARD')
    configcard = join(carddir, inputcard)
    commandargs = [join(bindir, cfg.get('PROGRAM', 'CALIBRATION'))]
    commandargs.append('-r' + calibration_run_id)
    commandargs.append('-p' + predestal_run_id)
    commandargs.append('-v' + version_lstchain )
    print("COMAND for calib:",commandargs)
    commandconcept = 'calibration'
    calibrationfile = 'new_calib'
    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except OSError as (ValueError, NameError):
        history(run, commandconcept, calibrationfile, inputcard, ValueError, historyfile)
        error(tag, "Could not execute \"{0}\", {1}".format(stringify(commandargs), NameError), ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(run, commandconcept, calibrationfile, inputcard, rc, historyfile)

    """ Error handling, for now no nonfatal errors are implemented for CALIBRATION """
    if rc != 0:
        exit(rc)
    return rc
   
##############################################################################
#
# MAIN
#
##############################################################################
if __name__ == '__main__':
    tag = gettag()
    import sys
    import options, cliopts
    # Set the options through cli parsing
    args = cliopts.calibrationsequencecliparsing(sys.argv[0])
    # Run the routine
    rc = calibrationsequence(args)
    sys.exit(rc)
