#!/usr/bin/env python
##############################################################################
#
# datasequence.py
# Date: 12th January 2020
# Authors: L. Saha (lab.saha@gmail.com), D. Morcuende, I. Aguado
#           A. Baquero, J. L. Contrera
# Last changes made on: 
# Credits: This script is written and modified following scripts from  MAGIC OSA. Hence, a big portion
# of the credits goes to the authors of MAGIC OSA.
##############################################################################
from osa.utils.standardhandle import output, verbose, warning, error, stringify, gettag
from sys import exit

__all__ = ["datasequence", "r0_to_dl1", 'dl1_to_dl2']


def datasequence(args):
    tag = gettag()
    import subprocess
    from os.path import join
    from osa.jobs.job import historylevel
    from osa.configs.config import cfg

    
    calibrationfile = args[0]
    pedestalfile = args[1]
    time_calibration = args[2]
    drivefile = args[3]
    run_str = args[4]

    textsuffix = cfg.get('LSTOSA', 'TEXTSUFFIX')
    historysuffix = cfg.get('LSTOSA', 'HISTORYSUFFIX')
    sequenceprebuild = join(options.directory,
                            'sequence_{0}_{1}'.format(options.tel_id, run_str))
    sequencefile = sequenceprebuild + textsuffix
    historyfile = sequenceprebuild + historysuffix
    print("HistoryFile:",historyfile)
    print("OPTION Directory:",options.directory)
    print("sequence file:",sequencefile)
    level, rc = historylevel(historyfile, 'DATA')
    print("LEVEL:",level,rc)
    verbose(tag, "Going to level {0}".format(level))
    if level == 3:
        rc = r0_to_dl1(calibrationfile,
                       pedestalfile,
                       time_calibration,
                       drivefile,
                       run_str,
                       sequencefile,
                       historyfile) 
        level -= 1
        verbose(tag, "Going to level {0}".format(level))
    if level == 2:
        rc = dl1_to_dl2(run_str,
                       sequencefile,
                       historyfile)
        level -= 1
        verbose(tag, "Going to level {0}".format(level))
    if level == 0:
       verbose(tag, "Job for sequence {0} finished without fatal errors"
                .format(run_str))    
    return rc


def r0_to_dl1(calibrationfile, pedestalfile, time_calibration, drivefile, run_str, sequencefile, historyfile):
    
    import sys
    import os
    import subprocess
    from os.path import join, dirname, basename
    from glob import glob
    from osa.configs.config import cfg
    from osa.rawcopy import raw
    from register import register_run_concept_files
    from osa.jobs.job import historylevel
    from osa.reports import report
    from osa.utils.utils import lstdate_to_dir


    configfile = cfg.get('LSTOSA','CONFIGFILE')

    '''
    TODO: Copy lstchain config file to log directory.
    Now it is done in sequencer.py
    ''' 

    pythondir = cfg.get('LSTOSA', 'PYTHONDIR')
    lstchaincommand = cfg.get('LSTOSA', 'R0-DL1')
    python = os.path.join(cfg.get('ENV', 'PYTHONBIN'), 'python')
    nightdir = lstdate_to_dir(options.date)
#    fullcommand = join(pythondir, lstchaincommand)
    fullcommand = lstchaincommand
    print("Run_str", run_str)
    datafile = join(cfg.get('LST1','RAWDIR'),nightdir,
            'LST-1.1.Run{0}{1}{2}'.format(run_str,cfg.get('LSTOSA','FITSSUFFIX'),cfg.get('LSTOSA','COMPRESSEDSUFFIX'))) 

#    commandargs = [python,fullcommand]
    commandargs = [fullcommand]
    commandargs.append('-f')
    commandargs.append(datafile)
    commandargs.append('-o')
    commandargs.append(options.directory)
    commandargs.append('-pedestal')
    commandargs.append(pedestalfile)
    commandargs.append('-calib')
    commandargs.append(calibrationfile)
    commandargs.append('-conf')
    commandargs.append( configfile)
    commandargs.append('-time_calib')
    commandargs.append(time_calibration)
    commandargs.append('-pointing')
    commandargs.append(drivefile)
#    commandargs.append('--drive=' + drivefile)
    
    print("fullcommand",python,commandargs)    
    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    #except OSError as (ValueError, NameError):
    except OSError as ValueError:
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        report.history(run_str, basename(fullcommand),\
         basename(calibrationfile), basename(pedestalfile), rc, historyfile)
        return rc


def dl1_to_dl2(run_str, sequencefile, historyfile):
    """
    Apply already trained RFs models to DL1 files.
    It identifies the primary particle adn reconstructs the energy and position.
    """
    
    import sys
    import os
    import subprocess
    from os.path import join, dirname, basename
    from glob import glob
    from osa.configs.config import cfg
    from osa.rawcopy import raw
    from register import register_run_concept_files
    from osa.jobs.job import historylevel
    from osa.reports import report
    from osa.utils.utils import lstdate_to_dir

    configfile = cfg.get('LSTOSA','CONFIGFILE')
    rf_models_directory = cfg.get('LSTOSA','RF-MODELS-DIR')

    pythondir = cfg.get('LSTOSA', 'PYTHONDIR')
    lstchaincommand = cfg.get('LSTOSA', 'DL1-DL2')
    python = os.path.join(cfg.get('ENV', 'PYTHONBIN'), 'python')
    nightdir = lstdate_to_dir(options.date)
#    fullcommand = join(pythondir, lstchaincommand)
    fullcommand = lstchaincommand
    print("Run_str", run_str)
    datafile = join(cfg.get('LST1','ANALYSISDIR'), nightdir, options.prod_id,
                    cfg.get('LSTOSA','R0-DL1PREFIX') +
                    'LST-1.1.Run{0}{1}{2}'.format(run_str, 
                                                  cfg.get('LSTOSA','FITSSUFFIX'),
                                                  cfg.get('LSTOSA','HDF5SUFFIX'),
                                                  )
                    )
    
    dl2_directory = join(cfg.get('LST1','DL2-DIR'),
                         nightdir,
                         options.prod_id)

    print(datafile)

#    commandargs = [python,fullcommand]
    commandargs = [fullcommand]
    commandargs.append('--datafile')
    commandargs.append(datafile)
    commandargs.append('--outdir')
    commandargs.append(dl2_directory)
    commandargs.append('--pathmodels')
    commandargs.append(rf_models_directory)
    commandargs.append('--config_file')
    commandargs.append(configfile)

    print("fullcommand", python, commandargs)    

    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    #except OSError as (ValueError, NameError):
    except OSError as ValueError:
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        report.history(run_str, basename(fullcommand),\
         basename(calibrationfile), basename(pedestalfile), rc, historyfile)
        return rc


if __name__ == '__main__':
    tag = gettag
    import sys
    from osa.utils import options, cliopts
    # Set the options through cli parsing
    args = cliopts.datasequencecliparsing(sys.argv[0])
    # Run the routine
    rc = datasequence(args)
    sys.exit(rc)
