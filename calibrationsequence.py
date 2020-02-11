#!/usr/bin/env python2.7
from osa.utils.standardhandle import output, verbose, warning, error, stringify, gettag

__all__ = ["calibrationsequence"]

def calibrationsequence(args):
    tag = gettag()
    # This is the python interface to run sorcerer with the -c option for calibration
    # args: <RUN>    
    import sys
    import subprocess
    from os.path import join
    from osa.utils import options, cliopts
    from osa.reports import report
    from osa.jobs.job import historylevel

    ped_output_file = args[0]
    run_ped = args[1]
    run_cal = args[2]


    historyfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run_cal + '.history')
    level, rc = historylevel(historyfile, 'CALIBRATION')
    verbose(tag, "Going to level {0}".format(level))
    print("historyfile:",historyfile, run_ped)
    print("PEDESTAL directory:",options.directory, options.tel_id)
    print("level & rc:",level, rc)
    if level == 1:
        rc = drs4_pedestal(run_ped, ped_output_file, historyfile)
        level -=1
        verbose(tag, "Going to level {0}".format(level))
#    if level == 1:
#        rc = calibrate(run_cal, historyfile)
#        level -=1
#        verbose(tag, "Going to level {0}".format(level))
    if level == 0:
        verbose(tag, "Job for sequence {0} finished without fatal errors".format(run))
    return rc

##############################################################################
#
# DRS4 pedestal
#
##############################################################################
def drs4_pedestal(run_ped, ped_output_file, historyfile):
    tag = gettag()
    from sys import exit
    from os.path import join
    import subprocess
    from osa.configs.config import cfg
    from osa.reports.report import history
    from register import register_run_concept_files
    from osa.utils.utils import lstdate_to_dir

    sequencetextfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run_ped+ '.txt')
    bindir = cfg.get('LSTOSA', 'LSTCHAINDIR')
    daqdir = cfg.get(options.tel_id, 'RAWDIR')
    #carddir = cfg.get('LSTOSA', 'CARDDIR')
    inputcard = cfg.get(options.tel_id, 'CALIBRATIONCONFIGCARD')
    #configcard = join(carddir, inputcard)
    commandargs = [cfg.get('PROGRAM', 'PEDESTAL')]
   
    nightdir = lstdate_to_dir(options.date)
 
    input_file = join(cfg.get('LST1','RAWDIR'),nightdir,
            'LST-1.1.Run{0}.'.format(run_ped)+'0000{1}{2}'.format(run_ped,cfg.get('LSTOSA','FITSSUFFIX'),cfg.get('LSTOSA','COMPRESSEDSUFFIX')))
    
    
    max_events = cfg.get('LSTOSA', 'MAX_PED_EVENTS')

    commandargs.append('--input_file=' +  input_file)
#    commandargs.append( input_file)
    commandargs.append('--output_file=' + ped_output_file )
 #   commandargs.append( ped_output_file )
    commandargs.append('--max_events='+ max_events)

    print("COMAND for pedestal:",commandargs)
    commandconcept = 'drs4_pedestal'
    pedestalfile = 'drs4_pedestal'
    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    #except OSError as (ValueError, NameError):
    except OSError as ValueError:
        history(run_ped, commandconcept, ped_output_file, inputcard, ValueError, historyfile)
        error(tag, "Could not execute \"{0}\", {1}".format(stringify(commandargs), NameError), ValueError)
    except subprocess.CalledProcessError as Error:
        error(tag, Error, rc)
    else:
        history(run_ped, commandconcept, pedestalfile, inputcard, rc, historyfile)

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
    from osa.configs.config import cfg
    from osa.reports.report import history
    from register import register_run_concept_files

    sequencetextfile = join(options.directory, 'sequence_' + options.tel_id + '_' + run + '.txt')
    bindir = cfg.get('LSTOSA', 'LSTCHAINDIR')
    daqdir = cfg.get(options.tel_id, 'RAWDIR')
    #carddir = cfg.get('LSTOSA', 'CARDDIR')
    inputcard = cfg.get(options.tel_id, 'CALIBRATIONCONFIGCARD')
    #configcard = join(carddir, inputcard)
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
    #except OSError as (ValueError, NameError):
    except OSError as ValueError:
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
    from osa.utils import options, cliopts
    # Set the options through cli parsing
    args = cliopts.calibrationsequencecliparsing(sys.argv[0])
    # Run the routine
    rc = calibrationsequence(args)
    sys.exit(rc)
