#!/usr/bin/env python2.7
##############################################################################
#
# sequencer.py
# Date: 12th January 2020
#   Authors
#   L. Saha <lab.saha@gmail.com>, D. Morcuende <dmorcuen@ucm.es>
#   A. Baquero <>, I. Aguado<>
#   J. L. Contrera <>
# Last modified on:
# Credits: This script is written and modified following scripts from 
# MAGIC OSA. Hence, a #big portion
# of the credits goes to the authors of MAGIC OSA.
##############################################################################
# from utils import standardhandle
# from .. import utils 
# import sys
# sys.path.append("..")
import os
from osa.utils.standardhandle import output, verbose, \
      warning, error, stringify, gettag
from osa.utils import options, cliopts
# report import start
# Only these functions will be considered when building the docs
__all__ = ["sequencer", "single_process", "stereo_process"]
##############################################################################
#
# sequencer
#
# This is the main script to be called in crontab
##############################################################################
# def sequencer():

def main():

    """

    Runs the sequencer

    """

    tag = gettag()
    from osa.reports.report import start
    from osa.configs import config
    process_mode = None
    single_array = ['LST1', 'LST2']
    start(tag)
    if options.tel_id in single_array: 
        process_mode = 'single'
        single_process(options.tel_id, process_mode)
    else:
        if options.tel_id == 'all':
            process_mode = 'complete'
        elif options.tel_id == 'ST':
            process_mode = 'stereo'
        sequence_LST1 = single_process('LST1', process_mode)
        sequence_LST2 = single_process('LST2', process_mode)
        sequence_ST = stereo_process('ST', sequence_M1, sequence_M2)
##############################################################################
#
# single_process
#
##############################################################################

def single_process(telescope, process_mode):

    """
    Runs the single process for a single telescope
    
    Parameters
    ----------
    telescope : str
        Options: 'LST1', 'LST2' or 'ST'
    process_mode : str
        Options: 'single', 'stereo' or 'complete'

    Returns
    -------
    sequence_list : 
    """
    tag = gettag()

    from osa.nightsummary import extract
    from osa.jobs import job
    from osa.veto import veto
    import dot
    from osa.nightsummary.nightsummary import readnightsummary
    from osa.reports.report import rule
    from osa.autocloser.closer import is_day_closed
    from shutil import copy                                                                                                                                                                                         
    from osa.configs.config import cfg
    from version import get_version


    sequence_list = []
    options.tel_id = telescope
    options.lstchain_version = 'v' + get_version()
    options.prod_id = options.lstchain_version + '_' + cfg.get('LST1', 'VERSION')
    options.directory = cliopts.set_default_directory_if_needed()
    options.log_directory = os.path.join(options.directory,'log') 
    os.makedirs(options.log_directory, exist_ok=True)
    print("DIR: ", options.directory)
    simulate_save = options.simulate
    is_report_needed = True

    if process_mode == 'single':
        if is_day_closed():
            output(tag, "Day {0} for {1} already closed".\
             format(options.date, options.tel_id))
            return sequence_list
    else:
        if process_mode == 'stereo':
            """ Only simulation for single array required """
            options.nightsum = True
            options.simulate = True
            is_report_needed = False

    """ Building the sequences """
    night = readnightsummary()  # night corresponds to f.read()
    print(night)
                                                                                                                                                                                                                    
    configfile = cfg.get('LSTOSA','CONFIGFILE')                                                                                                                                                                     
                                                                                                                                                                                                                    
    # Copy used lstchain config file to log directory                                                                                                                                                                    
    copy(configfile, options.log_directory)  

    subrun_list = extract.extractsubruns(night)
    run_list = extract.extractruns(subrun_list)

    # Modifies run_list by adding the seq and parent info into runs
    sequence_list = extract.extractsequences(run_list)

    # Workflow and Submission
#    dot.writeworkflow(sequence_list)

    # Adds the scripts
    job.preparejobs(sequence_list, subrun_list)
    
#    queue_list = job.getqueuejoblist(sequence_list)
#    veto_list = veto.getvetolist(sequence_list)
#    closed_list = veto.getclosedlist(sequence_list)
#    updatelstchainstatus(sequence_list)
#    updatesequencedb(sequence_list)
    # actually, submitjobs does not need the queue_list nor veto_list
#    job_list = job.submitjobs(sequence_list, queue_list, veto_list)

    job_list = job.submitjobs(sequence_list)

#    combine_muon(job_list)
#    # Report
#    if is_report_needed:
#        insert_if_new_activity_db(sequence_list)
#        updatesequencedb(sequence_list)
#        rule()
#        reportsequences(sequence_list)
#
#    # Cleaning
#    options.directory = None
#    options.simulate = simulate_save

    return sequence_list
##############################################################################
#
# stereo_process
#
##############################################################################
# def stereo_process(telescope, s1_list, s2_list):
#    tag = gettag()
#    import extract
#    import dot
#    import job
#    import veto
#    from report import start, rule
#
#    options.tel_id = telescope
#    options.directory = cliopts.set_default_directory_if_needed()
#
#    # Building the sequences
#    sequence_list = extract.extractsequencesstereo(s1_list, s2_list)
#    # Workflow and Submission
#    dot.writeworkflow(sequence_list)
#    # Adds the scripts    
#    job.preparestereojobs(sequence_list)
#    #job.preparedailyjobs(dailysrc_list)
#    queue_list = job.getqueuejoblist(sequence_list)
#    veto_list = veto.getvetolist(sequence_list)
#    closed_list = veto.getclosedlist(sequence_list)
#    updatelstchainstatus(sequence_list)
#    # actually, submitjobs does not need the queue_list nor veto_list
#    job_list = job.submitjobs(sequence_list, queue_list, veto_list)
#    # Finalizing report    
#    insert_if_new_activity_db(sequence_list)    
#    updatesequencedb(sequence_list)
#    rule()
#    reportsequences(sequence_list)
#    # Cleaning
#    options.directory = None
#    return sequence_list

##############################################################################
#
# updatelstchainstatus
#
##############################################################################

def updatelstchainstatus(seq_list):
    tag = gettag()
    from decimal import Decimal
    for s in seq_list:
        if s.type == 'CALI':
            s.scalibstatus = int(Decimal( getlstchainforsequence(s, 'Scalib')  * 100 ) / s.subruns)
        elif s.type == 'DATA':
            s.dl1status = int(Decimal( getlstchainforsequence(s, 'R0-DL1')  * 100 ) / s.subruns)
            s.dl2status = int(Decimal( getlstchainforsequence(s, 'DL1-DL2') * 100 ) / s.subruns)
        elif s.type == 'STEREO':
            s.dl2status = int(Decimal( getlstchainforsequence(s, 'DL1-DL2')  * 100 ) )
            s.dl3status = int(Decimal( getlstchainforsequence(s, 'DL2-DL3')  * 100 ) )

##############################################################################
#
# getlstchainforsequence
#
##############################################################################

def getlstchainforsequence(s, program):
    tag = gettag()
    from os.path import join
    from glob import glob
    from config import cfg
    prefix = cfg.get('LSTOSA', program + 'PREFIX')
    pattern = cfg.get('LSTOSA', program + 'PATTERN')
    suffix = cfg.get('LSTOSA', program + 'SUFFIX')

    files = glob(join(options.directory, "{0}*{1}*{2}*{3}".format(prefix, s.run, pattern, suffix)))
    numberoffiles = len(files)
    verbose(tag, "Found {0} {1}ed for sequence name {2}".format(numberoffiles, program, s.jobname))
    return numberoffiles
##############################################################################
#
# reportsequences
#
##############################################################################
def reportsequences(seqlist):
    tag = gettag()
    import config
    matrix = []
    header = ['Tel', 'Seq', 'Parent', 'Type', 'Run', 'Subruns', 'Source', 'Wobble', 'Action', 'Tries', 'JobID', 'State', 'Host', 'CPU_time', 'Walltime', 'Exit']
    if options.tel_id == 'M1' or options.tel_id == 'M2':
        header.append('_Y_%')
        header.append('_D_%')
        header.append('_I_%')
    elif options.tel_id == 'ST':
        header.append('_S_%')
        header.append('_Q_%')
        header.append('ODI%')
    matrix.append(header)
    for s in seqlist:
        row_list = [s.telescope, s.seq, s.parent, s.type, s.run, s.subruns, s.source, s.wobble, s.action, s.tries, s.jobid, s.state, s.jobhost, s.cputime, s.walltime, s.exit]
        if s.type == 'CALI':
            row_list.append(None)
            row_list.append(None)
            row_list.append(None)
        elif s.type == 'DATA':
            row_list.append(s.sorcererstatus)
            row_list.append(s.merppstatus)
            row_list.append(s.starstatus)
        elif s.type == 'STEREO':
            row_list.append(s.superstarstatus)
            row_list.append(s.melibeastatus)
            row_list.append(s.odiestatus)
        matrix.append(row_list)
    padding = int(config.cfg.get('OUTPUT', 'PADDING')) # space chars inserted between columnns
    prettyoutputmatrix(matrix, padding)
##############################################################################
#
# insert_if_new_activity_db
#
##############################################################################
def insert_if_new_activity_db(sequence_list):
    tag = gettag()
    import config
    from datetime import datetime
    from mysql import update_db, insert_db, select_db

    server = config.cfg.get('MYSQL', 'SERVER')
    user = config.cfg.get('MYSQL', 'USER')
    database = config.cfg.get('MYSQL', 'DATABASE')
    table = config.cfg.get('MYSQL', 'SUMMARYTABLE')

    if len(sequence_list) != 0:
        """ Declare the beginning of OSA activity """
        start = datetime.now()
        selections = ['ID']
        conditions = {'NIGHT': options.date, 'TELESCOPE': options.tel_id,\
         'ACTIVITY': 'LSTOSA'}
        matrix = select_db(server, user, database, table, selections, conditions)
        id = None
        if matrix:
            id = matrix[0][0]
        if id and int(id) > 0:
            """ Activity already started """
        else:
            """ Insert it into the database """
            assignments = conditions
            assignments['IS_FINISHED'] = 0
            assignments['START'] = start
            assignments['END'] = None 
            conditions = {}
            insert_db(server, user, database, table, assignments, conditions)
##############################################################################
#
# updatesequencedb
#
##############################################################################
def updatesequencedb(seqlist):
    tag = gettag()
    import config
    from mysql import update_db, insert_db, select_db

    server = config.cfg.get('MYSQL', 'SERVER')
    user = config.cfg.get('MYSQL', 'USER')
    database = config.cfg.get('MYSQL', 'DATABASE')
    table = config.cfg.get('MYSQL', 'SEQUENCETABLE')
    for s in seqlist:
        """ Fine tuning """
        hostname = None
        id_processor = None
        if s.jobhost != None:
            hostname, id_processor = s.jobhost.split('/')
        """ Select ID if exists """
        selections = ['ID']
        conditions = {'TELESCOPE': s.telescope, 'NIGHT': s.night, 'ID_NIGHTLY':s.seq}
        matrix = select_db(server, user, database, table, selections, conditions)
        id = None
        if matrix:
            id = matrix[0][0]
        verbose(tag, "To this sequence corresponds an entry in the {0} with ID {1}".format(table, id))
        assignments = {\
         'TELESCOPE': s.telescope,\
         'NIGHT': s.night,\
         'ID_NIGHTLY': s.seq,\
         'TYPE': s.type,\
         'RUN': s.run,\
         'SUBRUNS': s.subruns,\
         'SOURCEWOBBLE': s.sourcewobble,\
         'ACTION': s.action,\
         'TRIES': s.tries,\
         'JOBID': s.jobid,\
         'STATE': s.state,\
         'HOSTNAME': hostname,\
         'ID_PROCESSOR': id_processor,\
         'CPU_TIME': s.cputime,\
         'WALL_TIME': s.walltime,\
         'EXIT_STATUS': s.exit,\
         }

        if s.type == 'CALI':
            assignments.update({'PROGRESS_SCALIB': s.scalibstatus})
        elif s.type == 'DATA':
            assignments.update({\
             'PROGRESS_SORCERER': s.sorcererstatus,\
             'PROGRESS_SSIGNAL': s.ssignalstatus,\
             'PROGRESS_MERPP': s.merppstatus,\
             'PROGRESS_STAR': s.starstatus,\
             'PROGRESS_STARHISTOGRAM': s.starhistogramstatus,\
             })
        elif s.type == 'STEREO':
            assignments.update({\
             'PROGRESS_SUPERSTAR': s.superstarstatus,\
             'PROGRESS_SUPERSTARHISTOGRAM': s.superstarhistogramstatus,\
             'PROGRESS_MELIBEA': s.melibeastatus,\
             'PROGRESS_MELIBEAHISTOGRAM': s.melibeahistogramstatus,\
             })
        
        # TODO: Add this (Mireia) 
        #'PROGRESS_ODIE': s.odiestatus,\

        if s.parent != None:
            assignments['ID_NIGHTLY_PARENTS'] = '{0},'.format(s.parent)
        if not id:
            conditions = {}
            insert_db(server, user, database, table, assignments, conditions)
        else:
            conditions = {'ID':id}
            update_db(server, user, database, table, assignments, conditions)
##############################################################################
#
# combinemuon
#
##############################################################################
def combine_muon(job_list):
    tag = gettag()
    import os.path
    import subprocess
    import config
    from utils import magicdate_to_number
    muon_script = get_muon_script() 
    update_muon_script(muon_script)
    commandargs = ['qsub']
    if job_list != 0:
        commandargs.append('-W')
        depend_list = 'depend=afterany'
        for j in job_list:
            if j != None and j>0:
                depend_list += ":{0}".format(j)
        commandargs.append(depend_list)
    else:
        warning(tag, "Empty job list")
        return 0;

    commandargs.append(muon_script)
    if not options.simulate:
        try:
            outputstring = subprocess.check_output(commandargs)
        #except OSError as (ValueError, NameError):
        except OSError as  NameError:
            warning(tag, "Could not execute script {0}, {1}".format(stringify(commandargs), NameError))
            return ValueError
        else:
            job_id = outputstring.rstrip()
            verbose(tag, "Muon script {0} submitted with job_id {1}".format(muon_script, job_id))
    else:
        verbose(tag, "SIMULATE {0}".format(stringify(commandargs)))
##############################################################################
#
# updatemuoncontent                                                            
#                                
##############################################################################
def update_muon_script(muon_script):
    tag = gettag()
    import iofile
    from os.path import join
    from glob import glob
    import config
    from utils import magicdate_to_number
    bindir = config.cfg.get('LSTOSA', 'BINDIR')
    lstchaindir = config.cfg.get('LSTOSA', 'LSTCHAINDIR')
    command = join(bindir, config.cfg.get('LSTOSA', 'COMBINEMUON'))
    # Beware we want to change this in the future
    filename = get_muon_file()
    commandargs = [command, filename]
    jobname = "{0}_{1}_{2}".format(config.cfg.get('LSTOSA', 'MUONPREFIX'), magicdate_to_number(options.date), options.tel_id)
    content = "#!/bin/bash\n"
    # PBS-like assignments, beware that being comented with # is still parsed by the qsub routine and assigned
    content += "#PBS -S /bin/bash\n"
    content += "#PBS -N {0}\n".format(jobname)
    content += "#PBS -d {0}\n".format(options.directory)
    content += "#PBS -e {0}_$PBS_JOBID.err\n".format(jobname)
    content += "#PBS -o {0}_$PBS_JOBID.out\n".format(jobname)
    # Preparing the environment
    content += "LSTCHAIN={0}\n".format(LSTDIR)
    content += "{0}\n".format(stringify(commandargs))
    if not options.simulate:
        is_updated = iofile.writetofile(muon_script, content)
        return is_updated
##############################################################################
#
# get_muon_script
#
##############################################################################
def get_muon_script():
    tag = gettag()
    from utils import build_magicbasename
    from os.path import join
    from config import cfg
    prefix = cfg.get('LSTOSA', 'MUONPREFIX')
    suffix = cfg.get('LSTOSA', 'SCRIPTSUFFIX')
    basename = build_magicbasename(prefix, suffix)
    file = join(options.directory, basename)
    return file
##############################################################################
#
# get_muonfile
#
##############################################################################
def get_muon_file():
    tag = gettag()
    from os.path import join
    from config import cfg
    prefix = cfg.get('LSTOSA', 'MUONPREFIX')
    suffix = cfg.get('LSTOSA', 'TEXTSUFFIX')
    basename = build_magicbasename(prefix, suffix)
    file = join(options.directory, basename)
    return file
##############################################################################
#
# prettyoutputmatrix
#
##############################################################################
def prettyoutputmatrix(m, paddingspace):
    tag = gettag()
    maxfieldlength = []
    for i in range(len(m)):
        row = m[i]
        for j in range(len(row)):
            col = row[j]
            l = len(str(col))
            # verbose(tag, "Row {0}, Col {1}, Val {2} Len {3}".format(i, j, col, l))
            if m.index(row) == 0:
                maxfieldlength.append(l)
            elif l > maxfieldlength[j]:
                # Insert or update the first length
                maxfieldlength[j] = l
    for row in m:
        stringrow = ''
        for j in range(len(row)):
            col = row[j]
            lpadding = (maxfieldlength[j] - len(str(col))) * ' '
            rpadding = paddingspace * ' '
            if isinstance(col, (int, long)):
                # We got an integer, right aligned
                stringrow += "{0}{1}{2}".format(lpadding, col, rpadding)
            else:
                # Should be a string, left aligned
                stringrow += "{0}{1}{2}".format(col, lpadding, rpadding)
        output(tag, stringrow)
##############################################################################
#
# MAIN
#
##############################################################################
""" Sequencer called as a script does the full job """
if __name__ == '__main__':
    tag = gettag()
    import sys
    from osa.utils import options, cliopts
    # Set the options through parsing of the command line interface
    cliopts.sequencercliparsing(sys.argv[0])
    # Run the routine
#   sequencer()
    main()
