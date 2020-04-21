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
# MAGIC OSA. Hence, a big portion of the credits goes to its authors.
##############################################################################

import os
from shutil import copy

from osa.autocloser.closer import is_day_closed
from osa.jobs import job
from osa.nightsummary import extract
from osa.nightsummary.nightsummary import readnightsummary
from osa.reports.report import start
from osa.utils.standardhandle import output, verbose, gettag


__all__ = ["sequencer", "single_process"]


def sequencer():
    """Runs the sequencer
    This is the main script to be called in crontab
    """
    tag = gettag()

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
        sequence_lst1 = single_process('LST1', process_mode)
        sequence_lst2 = single_process('LST2', process_mode)
        # sequence_st = stereo_process('ST', sequence_lst1, sequence_lst2)


def single_process(telescope, process_mode):
    """Runs the single process for a single telescope
    
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

    from osa.configs import config
    # FIXME: find a better way of handling lstchain version
    from lstchain.version import get_version

    sequence_list = []

    # Define global variables and create night directory
    options.tel_id = telescope
    options.lstchain_version = 'v' + get_version()
    options.prod_id = options.lstchain_version + '_' + config.cfg.get('LST1', 'VERSION')
    options.directory = cliopts.set_default_directory_if_needed()
    options.log_directory = os.path.join(options.directory, 'log')
    os.makedirs(options.log_directory, exist_ok=True)
    output(tag, f"Analysis directory: {options.directory}")

    simulate_save = options.simulate
    is_report_needed = True

    if process_mode == 'single':
        if is_day_closed():
            output(tag, f"Day {options.date} for {options.tel_id} already closed")
            return sequence_list
    else:
        if process_mode == 'stereo':
            """ Only simulation for single array required """
            options.nightsum = True
            options.simulate = True
            is_report_needed = False

    """ Building the sequences """
    night = readnightsummary()  # night corresponds to f.read()
    output(tag, f"NightSummary:\n{night}")
                                                                                                                                                                                                                    
    configfile = config.cfg.get('LSTOSA', 'CONFIGFILE')
    copy(configfile, options.log_directory)  

    subrun_list = extract.extractsubruns(night)
    run_list = extract.extractruns(subrun_list)

    # Modifies run_list by adding the seq and parent info into runs
    sequence_list = extract.extractsequences(run_list)

    # Workflow and Submission
    # dot.writeworkflow(sequence_list)

    # Adds the scripts
    job.preparejobs(sequence_list)
    
#    queue_list = job.getqueuejoblist(sequence_list)
#    veto_list = veto.getvetolist(sequence_list)
#    closed_list = veto.getclosedlist(sequence_list)
#    updatelstchainstatus(sequence_list)
#    updatesequencedb(sequence_list)
    # actually, submitjobs does not need the queue_list nor veto_list
#    job_list = job.submitjobs(sequence_list, queue_list, veto_list)

    if not options.simulate:
        output(tag, "Launching the jobs")
        job_list = job.submitjobs(sequence_list)

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


def updatelstchainstatus(seq_list):
    tag = gettag()
    from decimal import Decimal
    for s in seq_list:
        if s.type == 'CALI':
            # FIXME: Change scalib to LST proper name
            s.scalibstatus = int(Decimal(getlstchainforsequence(s, 'Scalib') * 100) / s.subruns)
        elif s.type == 'DATA':
            s.dl1status = int(Decimal(getlstchainforsequence(s, 'R0-DL1') * 100) / s.subruns)
            s.dl2status = int(Decimal(getlstchainforsequence(s, 'DL1-DL2') * 100) / s.subruns)
        elif s.type == 'STEREO':
            s.dl2status = int(Decimal(getlstchainforsequence(s, 'DL1-DL2') * 100))
            s.dl3status = int(Decimal(getlstchainforsequence(s, 'DL2-DL3') * 100))


def getlstchainforsequence(s, program):
    tag = gettag()
    from os.path import join
    from glob import glob
    prefix = config.cfg.get('LSTOSA', program + 'PREFIX')
    pattern = config.cfg.get('LSTOSA', program + 'PATTERN')
    suffix = config.cfg.get('LSTOSA', program + 'SUFFIX')

    files = glob(join(options.directory, f"{prefix}*{s.run}*{pattern}*{suffix}"))
    numberoffiles = len(files)
    verbose(tag, f"Found {numberoffiles} {program}ed for sequence name {s.jobname}")
    return numberoffiles


def reportsequences(seqlist):
    tag = gettag()
    from osa.configs import config
    matrix = []
    header = [
        'Tel', 'Seq', 'Parent', 'Type', 'Run', 'Subruns',
        'Source', 'Wobble', 'Action', 'Tries', 'JobID',
        'State', 'Host', 'CPU_time', 'Walltime', 'Exit'
    ]
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        # FIXME: adap this to LST-related info
        header.append('_Y_%')
        header.append('_D_%')
        header.append('_I_%')
    elif options.tel_id == 'ST':
        header.append('_S_%')
        header.append('_Q_%')
        header.append('ODI%')
    matrix.append(header)
    for s in seqlist:
        row_list = [
            s.telescope, s.seq, s.parent, s.type, s.run, s.subruns,
            s.source, s.wobble, s.action, s.tries, s.jobid, s.state,
            s.jobhost, s.cputime, s.walltime, s.exit
        ]
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
    padding = int(config.cfg.get('OUTPUT', 'PADDING'))
    prettyoutputmatrix(matrix, padding)


def insert_if_new_activity_db(sequence_list):
    tag = gettag()
    from osa.configs import config
    from datetime import datetime
    from mysql import insert_db, select_db

    server = config.cfg.get('MYSQL', 'SERVER')
    user = config.cfg.get('MYSQL', 'USER')
    database = config.cfg.get('MYSQL', 'DATABASE')
    table = config.cfg.get('MYSQL', 'SUMMARYTABLE')

    if len(sequence_list) != 0:
        """ Declare the beginning of OSA activity """
        start = datetime.now()
        selections = ['ID']
        conditions = {
            'NIGHT': options.date,
            'TELESCOPE': options.tel_id,
            'ACTIVITY': 'LSTOSA'
        }
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


def updatesequencedb(seqlist):
    tag = gettag()
    from osa.configs import config
    from mysql import update_db, insert_db, select_db

    server = config.cfg.get('MYSQL', 'SERVER')
    user = config.cfg.get('MYSQL', 'USER')
    database = config.cfg.get('MYSQL', 'DATABASE')
    table = config.cfg.get('MYSQL', 'SEQUENCETABLE')
    for s in seqlist:
        """ Fine tuning """
        hostname = None
        id_processor = None
        if s.jobhost is not None:
            hostname, id_processor = s.jobhost.split('/')
        """ Select ID if exists """
        selections = ['ID']
        conditions = {
            'TELESCOPE': s.telescope,
            'NIGHT': s.night,
            'ID_NIGHTLY': s.seq
        }
        matrix = select_db(server, user, database, table, selections, conditions)
        id = None
        if matrix:
            id = matrix[0][0]
        verbose(tag, f"To this sequence corresponds an entry in the {table} with ID {id}")
        assignments = {
            'TELESCOPE': s.telescope,
            'NIGHT': s.night,
            'ID_NIGHTLY': s.seq,
            'TYPE': s.type,
            'RUN': s.run,
            'SUBRUNS': s.subruns,
            'SOURCEWOBBLE': s.sourcewobble,
            'ACTION': s.action,
            'TRIES': s.tries,
            'JOBID': s.jobid,
            'STATE': s.state,
            'HOSTNAME': hostname,
            'ID_PROCESSOR': id_processor,
            'CPU_TIME': s.cputime,
            'WALL_TIME': s.walltime,
            'EXIT_STATUS': s.exit,
        }

        if s.type == 'CALI':
            assignments.update({'PROGRESS_SCALIB': s.scalibstatus})
        elif s.type == 'DATA':
            # FIXME: translate to LST related stuff
            assignments.update({
                'PROGRESS_SORCERER': s.sorcererstatus,
                'PROGRESS_SSIGNAL': s.ssignalstatus,
                'PROGRESS_MERPP': s.merppstatus,
                'PROGRESS_STAR': s.starstatus,
                'PROGRESS_STARHISTOGRAM': s.starhistogramstatus,
            })
        elif s.type == 'STEREO':
            assignments.update({
                'PROGRESS_SUPERSTAR': s.superstarstatus,
                'PROGRESS_SUPERSTARHISTOGRAM': s.superstarhistogramstatus,
                'PROGRESS_MELIBEA': s.melibeastatus,
                'PROGRESS_MELIBEAHISTOGRAM': s.melibeahistogramstatus,
            })

        if s.parent is not None:
            assignments['ID_NIGHTLY_PARENTS'] = '{0},'.format(s.parent)
        if not id:
            conditions = {}
            insert_db(server, user, database, table, assignments, conditions)
        else:
            conditions = {'ID': id}
            update_db(server, user, database, table, assignments, conditions)


def prettyoutputmatrix(m, paddingspace):
    tag = gettag()
    maxfieldlength = []
    for i in range(len(m)):
        row = m[i]
        for j in range(len(row)):
            col = row[j]
            # verbose(tag, "Row {0}, Col {1}, Val {2} Len {3}".format(i, j, col, l))
            if m.index(row) == 0:
                maxfieldlength.append(len(str(col)))
            elif len(str(col)) > maxfieldlength[j]:
                # Insert or update the first length
                maxfieldlength[j] = len(str(col))
    for row in m:
        stringrow = ''
        for j in range(len(row)):
            col = row[j]
            lpadding = (maxfieldlength[j] - len(str(col))) * ' '
            rpadding = paddingspace * ' '
            if isinstance(col, int):
                # We got an integer, right aligned
                stringrow += "{0}{1}{2}".format(lpadding, col, rpadding)
            else:
                # Should be a string, left aligned
                stringrow += "{0}{1}{2}".format(col, lpadding, rpadding)
        output(tag, stringrow)


if __name__ == '__main__':
    """ Sequencer called as a script does the full job """
    from osa.utils import options, cliopts
    tag = gettag()
    # Set the options through parsing of the command line interface
    cliopts.sequencercliparsing()
    # Run the routine
    sequencer()
