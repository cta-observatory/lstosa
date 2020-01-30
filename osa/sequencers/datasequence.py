#!/usr/bin/env python2.7
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
from standardhandle import output, verbose, warning, error, stringify, gettag

__all__ = ["datasequence", "r0_to_dl1"]
##############################################################################
#
# datasequence
#
#################################
def datasequence(args):
    tag = gettag()
    import subprocess
    from os.path import join
    from job import historylevel
    from config import cfg

     

    calibrationfile = args[0]
    pedestalfile = args[1]
    drivefile = args[2]
    run_str = args[3]

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
       rc = r0_to_dl1(calibrationfile, pedestalfile, drivefile, run_str, sequencefile, historyfile) 
    if level == 0:
       verbose(tag, "Job for sequence {0} finished without fatal errors"
                .format(run_str))    
    return rc

#=========================================================================================
#  
#       R0-to-DL1
#
#=========================================================================================
def r0_to_dl1(calibrationfile, pedestalfile, drivefile, run_str, sequencefile, historyfile):
    
    import sys
    import os
    import subprocess
    from os.path import join, dirname, basename
    from glob import glob
    from config import cfg
    import raw
    from register import register_run_concept_files
    from job import historylevel
    import report

    configfile = cfg.get('LSTOSA','CONFIGFILE')
    
    pythondir = cfg.get('LSTOSA', 'PYTHONDIR')
    lstchaincommand = cfg.get('LSTOSA', 'R0-DL1')
    python = os.path.join(cfg.get('ENV', 'PYTHONBIN'), 'python')
    fullcommand = join(pythondir, lstchaincommand)
    datafile = join(cfg.get('LST1','RAWDIR'),
            'LST-1.1.Run{0}.'+'*'+'{1}{2}'.format(run_str,cfg.get('LSTOSA','FITSSUFFIX'),cfg.get('LSTOSA','COMPRESSEDSUFFIX'))) 

    commandargs = [python,fullcommand]
    commandargs.append('-f=' + datafile)
    commandargs.append('-o=' + options.directory)
    commandargs.append('--pedestal_drs4 =' + pedestalfile)
    commandargs.append('--calibration_coeff=' + calibrationfile)
    commandargs.append('--config=' + configfile )
    commandargs.append('--pointing' )
    commandargs.append('--drive=' + drivefile)
    
    print("fullcommand",python,commandargs)    
    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    except OSError as (ValueError, NameError):
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        report.history(run_str, basename(fullcommand),\
         basename(calibrationfile), basename(pedestalfile), rc, historyfile)
        return rc

##############################################################################
#
# MAIN
#
##############################################################################
if __name__ == '__main__':
    tag = gettag
    import sys
    import options, cliopts
    # Set the options through cli parsing
    args = cliopts.datasequencecliparsing(sys.argv[0])
    # Run the routine
    rc = datasequence(args)
    sys.exit(rc)
