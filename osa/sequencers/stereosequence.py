#!/usr/bin/env python2.7
##############################################################################
#
# stereosequence.py
#
# Copyright 2012 Alejandro Lorca <alejandro.lorca@fis.ucm.es>
#
# Based on the work of the On-Site-Analysis crew for the MAGIC telescope:
#   R. de los Reyes <reyes@gae.ucm.es>, J.L. Contreras <contrera@gae.ucm.es>,
#   I. Oya <oya@gae.ucm.es>, D. Nieto <nieto@gae.ucm.es>,
#   S. Pardo <spardo@gae.ucm.es>, K. Satalecka <satalk@gae.ucm.es>
#
##############################################################################
from standardhandle import output, verbose, warning, error, stringify, gettag

__all__ = ["stereosequence"]
##############################################################################
#
# stereosequence
#
##############################################################################
def stereosequence(args):
    tag = gettag()

    """ The stereosequence processes a whole data run """
    from os.path import join
    from job import historylevel
    from config import cfg

    run_str = args[0]

    history_suffix = cfg.get('LSTOSA', 'HISTORYSUFFIX')
    history_nosuffix = join(options.directory, "sequence_{0}_{1}".\
     format(options.tel_id, run_str))
    history_file = history_nosuffix + history_suffix
    level, rc = historylevel(history_file, 'STEREO')
    verbose(tag, "Going to level {0}".format(level))
    if level == 3:
        rc = superstar(run_str, history_file)
        level -= 1
        verbose(tag, "Going to level {0}".format(level))
    if level == 2:
        rc = melibea(run_str, history_file)
        level -= 1
        verbose(tag, "Going to level {0}".format(level))
    if level == 1:
        rc = odie(run_str, history_file)
        level -=1
        verbose(tag, "Going to level {0}".format(level))
    if level == 0:
        verbose(tag, "Job for sequence {0} finished without fatal errors"\
         .format(run_str))
    return rc
##############################################################################
#
# sorcerer
#
##############################################################################
def superstar(run_str, history_file):
    tag = gettag()
#  superstar -f -b -q --config=superstar.rc --ind1=m1_dir\20*.root --ind2=m2.dir\20*.root --out=./ --log=superstar.log
    """ Wrapper for running superstar with specific config elements """
    import sys
    from os.path import join, dirname
    from glob import glob
    from config import cfg 
    from register import register_run_concept_files
    from utils import magicdate_to_dir
    night_subdir = magicdate_to_dir(options.date)
    mars_dir = cfg.get('LSTOSA', 'MARSDIR')
    mars_command = cfg.get('LSTOSA', 'SUPERSTAR')
    full_mars_command = join(mars_dir, mars_command)
    root_suffix = cfg.get('LSTOSA', 'ROOTSUFFIX')
    input_card = join(cfg.get('LSTOSA', 'CARDDIR'),\
     cfg.get(options.tel_id, 'SUPERSTARCONFIGCARD'))
    star_dir_LST1 = cfg.get('LST1', 'STARDIR')
    star_dir_LST2 = cfg.get('LST2', 'STARDIR')
    superstarhistogramprefix = cfg.get('LSTOSA', 'SUPERSTARHISTOGRAMPREFIX')

    star_ind_LST1 = join(star_dir_LST1, night_subdir, "*{0}*{1}*{2}".\
     format('LST1', run_str, root_suffix))
    star_ind_LST2 = join(star_dir_M2, night_subdir, "*{0}*{1}*{2}".\
     format('M2', run_str, root_suffix))
    output_basename = "{0}{1}{2}".\
     format(superstarhistogramprefix, run_str, root_suffix)
    rc = superstar_specific(full_mars_command, input_card, run_str, star_ind_LST1,\
     star_ind_M2, output_basename, history_file)

    """ Error handling """
    try:    nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS','SUPERSTAR').split(",")]
    except: nonfatalrcs = [0]
    if rc not in nonfatalrcs:
        sys.exit(rc)
    return rc
    # We leave the registering of the files to the closer
    # else:
    #     #Start registering product files
    #     register_run_concept_files(run_str, 'SUPERSTAR')
    #     register_run_concept_files(run_str, 'SUPERSTARHISTOGRAM')
##############################################################################
#
# superstar_specific 
#
##############################################################################
def superstar_specific(full_mars_command, input_card, run_str, star_ind_LST1,\
 star_ind_M2, output_basename, history_file):
    tag = gettag()
    import subprocess
    from os.path import join, basename, exists
    from os import remove
    from glob import glob
    import datetime
    import iofile
    import report
    from utils import is_empty_root_file
    commandargs = [full_mars_command]
    commandargs.append('-f')
    commandargs.append('-q')
    commandargs.append('-b')
    commandargs.append('--config=' + input_card)
    commandargs.append('--ind1=' + star_ind_LST1)
    commandargs.append('--ind2=' + star_ind_M2)
    commandargs.append('--out=' + options.directory)
    commandargs.append('--outname=' + output_basename)

    LST1_star_files = glob(star_ind_LST1)
    if len(LST1_star_files) == 0:
        error(tag, "No LST1 star file of the form {0} exists yet".\
         format(star_ind_LST1), 1)
    M2_star_files = glob(star_ind_M2)
    if len(M2_star_files) == 0:
        error(tag, "No M2 star file of the form {0} exists yet".\
         format(star_ind_M2), 1)        

    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    except OSError as (ValueError, NameError):
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        outputf = glob("{0}/20*{1}*_S_*.root".
                format(options.directory, run_str))
        if len(outputf) != 1:
            error(tag, "No output or multiple output files found: ".
                    format(stringify(outputf)), 2)
        if is_empty_root_file(outputf[0]):
            verbose(tag, "{0} is empty, i will delete it!".format(outputf[0]))
            remove(outputf[0])
            rc = 3

        report.history(run_str, basename(full_mars_command),\
         None, basename(input_card), rc, history_file)
	return rc
##############################################################################
#
# melibea
#
##############################################################################
def melibea(run_str, history_file):
    tag = gettag()
# melibea -q -f --config=melibea_stereo.rc --stereo \
#    --ind="InputDIR/*_S_*.root" \
#    --out="OutputDIR/." \
#    --rf --rftree=RF.root \
#    --calc-disp-rf --rfdisptree=LST1/DispRF.root \
#    --calc-disp2-rf --rfdisp2tree=M2/DispRF.root \
#    --calcstereodisp --disp-rf-sstrained \
#    -erec --etab=Energy_Table.root 
    """ Wrapper for running melibea with specific config elements """
    import sys
    from os.path import join, dirname
    from glob import glob
    from config import cfg 
    from register import register_run_concept_files
    from utils import magicdate_to_dir
    night_subdir = magicdate_to_dir(options.date)
    mars_dir = cfg.get('LSTOSA', 'MARSDIR')
    mars_command = cfg.get('LSTOSA', 'MELIBEA')
    full_mars_command = join(mars_dir, mars_command)
    root_suffix = cfg.get('LSTOSA', 'ROOTSUFFIX')
    input_card = join(cfg.get('LSTOSA', 'CARDDIR'),\
     cfg.get(options.tel_id, 'MELIBEACONFIGCARD'))
    rftree = join(cfg.get('ST', 'RANDOMFORESTMATRIXDIR'),\
     cfg.get('ST', 'RANDOMFORESTMATRIX'))
    disp1 = join(cfg.get('ST', 'DISPLST1MATRIXDIR'),\
     cfg.get('ST', 'DISPMATRIX'))
    disp2 = join(cfg.get('ST', 'DISPM2MATRIXDIR'),\
     cfg.get('ST', 'DISPMATRIX'))
    energy_table = join(cfg.get('ST', 'ENERGYMATRIXDIR'),\
     cfg.get('ST', 'ENERGYTABLE'))
    analysis_dir = cfg.get('ST', 'ANALYSISDIR')
    input_pattern = cfg.get('LSTOSA', 'SUPERSTARPATTERN')
    melibeahistogramprefix = cfg.get('LSTOSA', 'MELIBEAHISTOGRAMPREFIX')

    ind = join(analysis_dir, night_subdir, "*{0}{1}*{2}".\
     format(run_str, input_pattern, root_suffix)) 
    out = join(analysis_dir, night_subdir)

    """ The right output_basename should be this, but Melibea has a bug,
        resulting in file naming of the form melibeamelibea05024715.root.root 
        If it gets fixed in Mars, please change accordingly"""   
#    output_basename = "{0}{1}{2}".\
#         format(melibeahistogramprefix, run_str, root_suffix)
    output_basename = "{1}".\
         format(melibeahistogramprefix, run_str, root_suffix)


    rc = melibea_specific(full_mars_command, input_card, run_str, ind, out,\
     rftree, disp1, disp2, energy_table, output_basename, history_file)

    """ Error handling """
    try:    nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS','MELIBEA').split(",")]
    except: nonfatalrcs = [0]
    if rc not in nonfatalrcs:
        sys.exit(rc)
    return rc
    # We leave the registering of the files to the closer
    # else:
    #     #Start registering product files
    #     register_run_concept_files(run_str, 'MELIBEA')
    #     register_run_concept_files(run_str, 'MELIBEAHISTOGRAM')
##############################################################################
#
# melibea_update
#
##############################################################################
def melibea_specific(full_mars_command, input_card, run_str, ind, out,\
    rftree, disp1, disp2, energy_table, output_basename, history_file):
    tag = gettag()
# melibea -q -f --config=melibea_stereo.rc --stereo \
#    --ind=InputDIR/*_S_*.root \
#    --out=OutputDIR/. \
#    --rf --rftree=RF.root \
#    --calc-disp-rf --rfdisptree=LST1/DispRF.root \
#    --calc-disp2-rf --rfdisp2tree=M2/DispRF.root \
#    --calcstereodisp --disp-rf-sstrained \
#    -erec --etab=Energy_Table.root

    import subprocess
    from os.path import join, basename, exists
    from glob import glob
    import datetime
    import iofile
    import report
    commandargs = [full_mars_command]
    commandargs.append('-f')
    commandargs.append('-q')
    commandargs.append('-b')
    commandargs.append('--config=' + input_card)
    commandargs.append('--stereo')
    commandargs.append('--ind=' + ind)
    commandargs.append('--out=' + out)
    commandargs.append('--rf')
    commandargs.append('--rftree=' + rftree)
    commandargs.append('--calc-disp-rf')
    commandargs.append('--rfdisptree=' + disp1)
    commandargs.append('--calc-disp2-rf')
    commandargs.append('--rfdisp2tree=' + disp2)
    commandargs.append('--calcstereodisp')
    commandargs.append('--disp-rf-sstrained')
    commandargs.append('-erec')
    commandargs.append('--etab=' + energy_table)
    commandargs.append('--outname=' + output_basename)

    superstar_files = glob(ind)
    if len(superstar_files) == 0:
        error(tag, "No superstar files of the form {0} exists yet".\
         format(ind), 1)

    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    except OSError as (ValueError, NameError):
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        report.history(run_str, basename(full_mars_command),\
         None, basename(input_card), rc, history_file)
    return rc
##############################################################################
#
# odie
#
##############################################################################
def odie(run_str, history_file):
    return(0)
    tag = gettag()
# Usage of odie:
#   odie [flag options] --config=odie.rc
# 
# lag Options:
#   -s:                stack the plots in the files Odie.dataName
#                           (see odie_stack.rc for relevant options)
#   -h, --help:        show this help
#   -b:                Batch mode (no graphical output to screen)
#   -q:                quit after finishing
#   --ind=path/file(s) Path where to search for the melibea data files (wildcards allowed)
#   --outname=name     Name of the output container
    """ Wrapper for running odie with specific config elements """
    import sys
    from os.path import join, dirname
    from glob import glob
    from config import cfg 
    from register import register_run_concept_files
    from utils import magicdate_to_dir
    from utils import magicdate_to_number
    night_subdir = magicdate_to_dir(options.date)
    night_number = magicdate_to_number(options.date)
    mars_dir = cfg.get('LSTOSA', 'MARSDIR')
    mars_command = cfg.get('LSTOSA', 'ODIE')
    full_mars_command = join(mars_dir, mars_command)
    root_suffix = cfg.get('LSTOSA', 'ROOTSUFFIX')
    input_card = join(cfg.get('LSTOSA', 'CARDDIR'),\
     cfg.get(options.tel_id, 'ODIECONFIGCARD'))
    analysis_dir = cfg.get('ST', 'ANALYSISDIR')
    input_pattern = cfg.get('LSTOSA', 'MELIBEAPATTERN')
    odieprefix = cfg.get('LSTOSA', 'ODIEPREFIX')
    odiepattern = cfg.get('LSTOSA', 'ODIEPATTERN')

    ind = join(analysis_dir, night_subdir, "*{0}{1}*{2}".\
     format(run_str, input_pattern, root_suffix)) 
    out = join(analysis_dir, night_subdir)
    
    # Trick to get the src name from the run number and filename
    melibea_files = glob(ind)
    src_name = melibea_files[0].split(input_pattern)[-1]
    # Remove the wobble pattern
    for offset in xrange(3):
        src_name = src_name.split("-W%d." %offset)[0]

    output_basename = "{0}_{1}{2}{3}{4}".\
         format(night_number, run_str, odiepattern, src_name, root_suffix)

    rc = odie_specific(full_mars_command, input_card, run_str, ind, out,\
     output_basename, history_file)

    return(0)
    """ Error handling """
    try:    nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS','ODIE').split(",")]
    except: nonfatalrcs = [0]
    if rc not in nonfatalrcs:
        sys.exit(rc)
    return rc
    # We leave the registering of the files to the closer
    # else:
    #     #Start registering product files
    #     register_run_concept_files(run_str, 'ODIE')

##############################################################################
#
# odie_specific
#
##############################################################################
def odie_specific(full_mars_command, input_card, run_str, ind, out,\
    output_basename, history_file):
    tag = gettag()
# Usage of odie:
#   odie [flag options] --config=odie.rc
# 
# lag Options:
#   -s:                stack the plots in the files Odie.dataName
#                           (see odie_stack.rc for relevant options)
#   -h, --help:        show this help
#   -b:                Batch mode (no graphical output to screen)
#   -q:                quit after finishing
#   --ind=path/file(s) Path where to search for the melibea data files (wildcards allowed)
#   --outname=name     Name of the output container

    import subprocess
    from os.path import join, basename, exists
    from glob import glob
    import datetime
    import iofile
    import report
    energy_band="LE"
    commandargs = [full_mars_command]
    commandargs.append('-q')
    commandargs.append('-b')
    commandargs.append('--config=' + input_card.replace("EBAND",energy_band))
    commandargs.append('--ind=' + ind)
    commandargs.append('--outname=' + output_basename)

    melibea_files = glob(ind)
    if len(melibea_files) == 0:
        error(tag, "No melibea files of the form {0} exists yet".\
         format(ind), 1)

    try:
        verbose(tag, "Executing \"{0}\"".format(stringify(commandargs)))
        rc = subprocess.call(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, "{0}".format(Error), rc)
    except OSError as (ValueError, NameError):
        error(tag, "Command \"{0}\" failed, {1}"\
         .format(stringify(commandargs), NameError), ValueError)
    else:
        report.history(run_str, basename(full_mars_command),\
         None, basename(input_card), rc, history_file)
    return rc
##############################################################################
#
# caspar
#
##############################################################################
def caspar(run_str, history_file):
    tag = gettag()
    caspar_specific()
    pass
##############################################################################
#
# caspar_specific
#
##############################################################################
def caspar_specific():
    tag = gettag()
    pass
##############################################################################
#
# flute
#
##############################################################################
def flute(run_str, history_file):
    tag = gettag()
    flute_specific()
    pass
##############################################################################
#
# flute_specific
#
##############################################################################
def flute_specific():
    tag = gettag()
    pass
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
    args = cliopts.stereosequencecliparsing(sys.argv[0])
    # Run the routine
    rc = stereosequence(args)
    sys.exit(rc)
