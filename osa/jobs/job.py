from osa.utils.standardhandle import verbose, warning, error, stringify, output, gettag
from osa.utils import options, cliopts
##############################################################################
#
# arealljobscorrectlyfinished
#
##############################################################################
def arealljobscorrectlyfinished(seqlist):
    tag = gettag()
    flag = True
    for s in seqlist:
        out, rc = historylevel(s.history, s.type)
        if out == 0:
            verbose(tag, "Job {0} correctly finished".format(s.seq))
            continue
        else:
            verbose(tag, "Job {0} not correctly/completely finished [{1}]".format(s.seq,out))
            flag = False
    return flag
##############################################################################
#
# historylevel
#
##############################################################################
def historylevel(historyfile, type):
    """
    Returns the level from which the analysis should begin and the rc of the
    last executable given a certain history file
    """
    tag = gettag()
    from iofile import readfromfile
    from os.path import exists
    from osa.configs.config import cfg
    level = 3
    exit_status = 0
    if type == 'PEDESTAL':
       level -= 2
    if type == 'CALIBRATION':
        level -= 1
    if exists(historyfile):
        for line in readfromfile(historyfile).splitlines():
            words = line.split()
            try:
                program = words[1]
                exit_status = int(words[10])
                print("DEBUG:",program, exit_status)
            except IndexError as e:
                error(tag, "Malformed history file {0}, e".format(historyfile), 3)
            except ValueError as e:
                error(tag, "Malformed history file {0}, e".format(historyfile), 3)
            else:
                if program == cfg.get('LSTOSA','R0-DL1'):
                    nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS','R0-DL1').split(",")]
                    if exit_status in nonfatalrcs:
                        level = 2
                    else:
                        level = 3
                elif program == cfg.get('LSTOSA','DL1-DL2'):
                    nonfatalrcs = [int(k) for k in cfg.get('NONFATALRCS','DL1-DL2').split(",")]
                    if exit_status in nonfatalrcs:
                        level = 2
                    else:
                        level = 3
                elif program == 'calibration':
                    if exit_status == 0:
                        level = 0
                    else:
                        level = 1
                elif program == 'drs4_pedestal':
                    if exit_status == 0:
                        level = 1
                    else:
                        level = 0

                else:
                    error(tag, 'Programme name not identified {0}'.format(program), 6)
                   
    print(level,exit_status) 
    return level, exit_status
##############################################################################
#
# preparejobs
#
##############################################################################
def preparejobs(sequence_list, subrun_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, "Creating sequence.txt and sequence.sh for sequence {0}".format(s.seq))
        createsequencetxt(s, sequence_list)
        createjobtemplate(s)
##############################################################################
#
# preparejobs
#
##############################################################################
def preparestereojobs(sequence_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, "Creating sequence.sh for sequence {0}".format(s.seq))
        createjobtemplate(s)
##############################################################################
#
# preparejobs
#
##############################################################################
def preparedailyjobs(sequence_list):
    tag = gettag()
    for s in sequence_list:
        verbose(tag, "Creating sequence.sh for source {0}".format(s.name))
        createjobtemplate(s)
##############################################################################
#
# setrunfromparent
#
##############################################################################
def setrunfromparent(sequence_list):
    tag = gettag()
    # This is a dictionary, seq -> parent's run number
    dictionary = {}
    for s1 in sequence_list:
        if s1.parent != None:
            for s2 in sequence_list:
                if s2.seq == s1.parent:
                    verbose(tag, "Assigning runfromparent({0}) = {1}".format(s1.parent, s2.run))
                    dictionary[s1.parent] = s2.run
                    break
    return dictionary
##############################################################################
#
# createsequencetxt
#
##############################################################################
def createsequencetxt(s, sequence_list):
    tag = gettag()
    from os.path import join
    import string
    import iofile
    from osa.utils.utils import lstdate_to_iso
    from osa.configs.config import cfg
    text_suffix = cfg.get('LSTOSA', 'TEXTSUFFIX')
    f = join(options.directory, "sequence_{0}{1}".\
     format(s.jobname, text_suffix))
    start = s.subrun_list[0].timestamp
    ped = ''
    cal = ''
    dat = ''
    if s.type == 'CALI':
        ped = formatrunsubrun(s.previousrun, 1)
        cal = formatrunsubrun(s.run, 1)
    elif s.type == 'DATA':
        ped = formatrunsubrun(s.parent_list[0].previousrun, 1)
        cal = formatrunsubrun(s.parent_list[0].run, 1)
        for sub in s.subrun_list:
            dat += formatrunsubrun(s.run, sub.subrun) + ' '

    content = "# Sequence number (identifier)\n"
#    content += "Sequence: {0}_{0}\n".format(s.run)    # Not clear why the same number twice
    content += "Sequence: {0}\n".format(s.run)
    content += "# Date of sunrise of the observation night\n"
    content += "Night: {0}\n".format(s.night)
    content += "# Start time of the sequence (first data run)\n"
    content += "Start: {0}\n".format(start)    # TODO, get the right date and time
    content += "# Source name of all runs of sequence\n"
    content += "Source: {0}\n".format(s.sourcewobble)
    content += "Telescope: {0}\n".format(options.tel_id.lstrip('M'))
    content += "\n"
    content += "PedRuns: {0}\n".format(ped)
    content += "CalRuns: {0}\n".format(cal)
    content += "DatRuns: {0}\n".format(dat)

    if not options.simulate:
        iofile.writetofile(f, content)
##############################################################################
#
# formatrunsubrun
#
##############################################################################
def formatrunsubrun(run, subrun):
    tag = gettag()
    # it needs 7 digits for the runs
    # it needs 3 digits (insert leading 0s needed)
    if not run:
        run = 7*'0'
    s = str(subrun).zfill(3)
    string = "{0}.{1}".format(run, s)
    return string
##############################################################################
#
# setsequencefilenames
#
##############################################################################
def setsequencefilenames(s):
    tag = gettag()
    import os.path
    from osa.configs.config import cfg
    script_suffix = cfg.get('LSTOSA', 'SCRIPTSUFFIX')
    history_suffix = cfg.get('LSTOSA', 'HISTORYSUFFIX')
    veto_suffix = cfg.get('LSTOSA', 'VETOSUFFIX')
    basename = "sequence_{0}".format(s.jobname)

    s.script = os.path.join(options.directory, basename + script_suffix)
#    print("DEBUG",options.directory)
    s.veto = os.path.join(options.directory, basename + veto_suffix )
    s.history = os.path.join(options.directory, basename + history_suffix)
    # Calibfiles cannot be set here, since they require the runfromparent
##############################################################################
#
# setsequencecalibfilenames
#
##############################################################################
def setsequencecalibfilenames(sequence_list):
    tag = gettag()
    from osa.configs.config import cfg
    from osa.utils.utils import lstdate_to_dir
    scalib_suffix = cfg.get('LSTOSA', 'SCALIBSUFFIX')
    pedestal_suffix = cfg.get('LSTOSA', 'PEDESTALSUFFIX')
    drive_suffix = cfg.get('LSTOSA', 'DRIVESUFFIX')
    for s in sequence_list:
        if len(s.parent_list) == 0:
            #calfile = '666calib.root'

            cal_run_string = str(s.run).zfill(4)
            calfile = "calibration.Run{0}.0000{1}".\
                 format(cal_run_string, scalib_suffix)
            #pedfile = '666ped.root'
            ped_run_string = str(s.previousrun).zfill(4) 
            pedfile = "drs4_pedestal.Run{0}.0000{1}".\
                 format(ped_run_string, pedestal_suffix)
            drivefile = '666drive.txt'
        else:
            run_string = str(s.parent_list[0].run).zfill(4)
            ped_run_string = str(s.parent_list[0].previousrun).zfill(4)
     #       print("DEBUG",s.subrun_list[0].time)
     #       print("DEBUG2",s.subrun_list[0].date)
            date_string = str(s.subrun_list[0].date).zfill(8)
            time_string = str(s.subrun_list[0].time).zfill(8)
            nightdir = lstdate_to_dir(options.date)
     #       print(date_string,time_string)
            yy,mm,dd = date_in_yymmdd(nightdir)            
            if options.mode == 'P':
                calfile = "calibration.Run{0}.0000{1}".\
                 format(run_string, scalib_suffix)
                pedfile = "drs4_pedestal.Run{0}.0000{1}".\
                 format(ped_run_string, pedestal_suffix)
                drivefile = "drive_log_{0}_{1}_{2}{3}".\
                 format(yy, mm, dd, drive_suffix)
            elif ( options.mode == 'S' or options.mode == 'T' ):
                calfile = "ssginal{0}_{1}{2}".\
             format(run_string, s.telescope, ssignal_suffix)
        s.calibration = calfile
        s.pedestal = pedfile
        s.drive    = drivefile
##############################################################################
#
# guesscorrectinputcard
#
##############################################################################
def guesscorrectinputcard(s):
    '''
    Returns guessed input card for:
     datasequencer
     calibrationsequence
     stereosequence

    If it fails, return the default one
    '''

    import os
    from os.path import join
    import iofile
    from osa.configs import config

    bindir = config.cfg.get('LSTOSA', 'PYTHONDIR')

    # Stereo runs do not have kind_obs.
    try:
        #assert(s.kind_obs)
        assert(s.source)
        assert(s.hv_setting)
        assert(s.moonfilter)
    except:
        return(options.configfile)

    # Non standard input cards.
   
    inputCardStr = ""
    if ("MoonSh" in s.source or "GRB" in s.source or "AZ-" in s.source):
        inputCardStr = "%s_noPos" % inputCardStr
    if (s.hv_setting == "redHV"):
        inputCardStr = "%s_redHV" % inputCardStr
    if (s.moonfilter == "Filter"):
        inputCardStr = "%s_filters" % inputCardStr

    if (inputCardStr != ""):
        return(join(bindir,'cfg','osa%s.cfg' % inputCardStr))
    
    return(options.configfile)


##############################################################################
#
# createjobtemplate/
#
##############################################################################
def createjobtemplate(s):
    tag = gettag()
    #   This file contains instruction to be submitted to torque
    import os
    import iofile
    from osa.configs import config
    from osa.utils.utils import lstdate_to_dir
    bindir = config.cfg.get('LSTOSA', 'PYTHONDIR')
    calibdir = config.cfg.get('LST1', 'CALIBDIR')
    pedestaldir = config.cfg.get('LST1', 'PEDESTALDIR')
    drivedir = config.cfg.get('LST1', 'DRIVEDIR')
    nightdir = lstdate_to_dir(options.date)
    version  = config.cfg.get('LST1', 'VERSION')

    command = None
    if s.type == 'CALI':
        command = os.path.join(bindir, 'calibrationsequence.py')
    elif s.type == 'DATA':
        command = os.path.join(bindir, 'datasequence.py')
    elif s.type == 'STEREO':
        command = os.path.join(bindir, 'stereosequence.py')
    elif s.type == 'DAILY':
        command = os.path.join(bindir, 'daily.py')

    python = os.path.join(config.cfg.get('ENV', 'PYTHONBIN'), 'python')
    sbatchbin = config.cfg.get('ENV','SBATCHBIN')
     
   
#    pedestalfile = ped
#    drivefile = drive
    mode = 1
    # Beware we want to change this in the future
    commandargs = [sbatchbin, python, command]
    if options.verbose:
        commandargs.append('-v')
    if options.warning:
        commandargs.append('-w')
    if options.configfile:
        commandargs.append('-c')
        commandargs.append(guesscorrectinputcard(s))
    if options.compressed:
        commandargs.append('-z')
    #commandargs.append('--stderr=sequence_{0}_'.format(s.jobname) + "{0}.err'" + ".format(str(job_id))")
    #commandargs.append('--stdout=sequence_{0}_'.format(s.jobname) + "{0}.out'" + ".format(str(job_id))")
    commandargs.append('-d')
    commandargs.append(options.date)
    
     
    if s.type == 'CALI':
        commandargs.append(os.path.join(pedestaldir, nightdir, version, s.pedestal))
        commandargs.append(os.path.join(calibdir, nightdir, version, s.calibration))
        ped_run = str(s.previousrun).zfill(5)
        commandargs.append(ped_run)
   
    if s.type == 'DATA':
        commandargs.append(os.path.join(calibdir, nightdir, version, s.calibration))
        commandargs.append(os.path.join(pedestaldir, nightdir, version, s.pedestal))
        commandargs.append(os.path.join(calibdir, nightdir, version, 'time_'+ s.calibration))
        commandargs.append(os.path.join(drivedir, s.drive))
        pedfile = s.pedestal



    #commandargs.append(str(s.run).zfill(5))
 #   if s.type != 'STEREO':
      #  commandargs.append(options.tel_id)
    for sub in s.subrun_list:
        n_subruns = int(sub.subrun)

    
    content = "#!/bin/env python\n"
    # SLURM assignments
    content += "#SBATCH -p compute\n"
    if s.type == 'DATA':
       content += "#SBATCH --array=0-{0}\n".format(int(n_subruns)-1)
    content += "#SBATCH --cpus-per-task=1\n"
    content += "#SBATCH --mem-per-cpu=2G\n"
    content += "#SBATCH -t 0-24:00\n"
    # TODO: Change log to night directory
    content += "#SBATCH -o ./log/slurm.%A_%a.%N.out\n"
    content += "#SBATCH -e ./log/slurm.%A_$a.%N.err\n"
     #
    content +="import subprocess\n"
    content +="import os\n"
    content +="subruns=os.getenv('SLURM_ARRAY_TASK_ID')\n"
    content +="job_id=os.getenv('SLURM_JOB_ID')\n"
    dat = ''
    #for sub in s.subrun_list:
    #    dat += formatrunsubrun(s.run, sub.subrun) + ' '
    #    if s.type == 'DATA':
    #    	for i in range(int(sub.subrun)):
    #        		srun = str(i).zfill(4)
    #        		subruns.append(srun)
#   #             content += "subruns={0}\n".format(subruns)

    #dat += formatrunsubrun(s.run, sub.subrun) + ' '

        #else:
        #        #subruns.append(str(0).zfill(4))
    #content += "subruns={0}\n".format(subruns)
   # content += "for subrun in subruns:\n"

  #  content +="subprocess.call({0})\n".format(commandargs)
    content += "subprocess.call(["
    for i in commandargs: 
        content += "	'{0}',\n".format(i)
    content += "          '--stderr=sequence_{0}_".format(s.jobname) + "{0}.err'" + '.format(str(job_id))'+',\n'
    content += "          '--stdout=sequence_{0}_".format(s.jobname) + "{0}.out'" + '.format(str(job_id))'+',\n'
    if s.type == 'DATA':
       content += "	     '{0}".format(str(s.run).zfill(5))+".{0}'"+'.format(str(subruns).zfill(4))'+','
    else:
       content += "          '{0}'".format(str(s.run).zfill(5)) + ','
    content += "	'{0}'".format(options.tel_id)
    content +="		])"
    
    print("S.script",s.script)  
    if not options.simulate:
        iofile.writetofile(s.script, content)
##############################################################################
#
# submitjobs
#
##############################################################################
#def submitjobs(sequence_list, queue_list, veto_list):
def submitjobs(sequence_list):
    tag = gettag()
    import subprocess
    from os.path import join
    from osa.configs import config
    job_list = []
    command = 'sbatch'
    for s in sequence_list:
         commandargs = [command, s.script] # List of two elements
#        commandargs.append('-W')
#        commandargs.append('umask=0022')
#        """ Introduce the job dependencies """
#        if len(s.parent_list) != 0:
#            commandargs.append('-W')
#            depend_string = 'depend='
#            if s.type == 'DATA':
#                depend_string += 'afterok'
#            elif s.type == 'STEREO':
#                depend_string += 'afterany'
#            for pseq in s.parent_list:
#                if pseq.jobid > 0:
#                    depend_string += ":{0}".format(pseq.jobid)
#            commandargs.append(depend_string)
#        """ Skip vetoed """
#        if s.action == 'Veto':
#            verbose(tag, "job {0} has been vetoed".format(s.jobname))
#        elif s.action == 'Closed':
#            verbose(tag, "job {0} is already closed".format(s.jobname))
#        elif s.action == 'Check' and s.state != 'C':
#            verbose(tag, "job {0} checked to be dispatched but not completed yet".format(s.jobname))
#            if s.state == 'H' or s.state == 'R':
#                # Reset values
#                s.exit = None
#                if s.state == 'H':
#                    s.jobhost = None
#                    s.cputime = None
#                    s.walltime = None
#        elif s.action == 'Check' and s.state == 'C' and s.exit == 0:
#            verbose(tag, "job {0} checked to be successful".format(s.jobname))
#        else:
#            if options.simulate == True:
#                commandargs.insert(0, 'echo')
#                s.action = 'Simulate'
#                # This jobid is negative showing it belongs to a simulated environment (not real jobid)
#                s.jobid = -1 - s.seq
#            else:
#                s.action = 'Submit'
#                # Reset the values to avoid misleading info from previous jobs
#                s.jobhost = None
#                s.state = 'Q'
#                s.cputime = None
#                s.walltime = None
#                s.exit = None
#            try:
#                stdout = subprocess.check_output(commandargs)
#            except subprocess.CalledProcessError as Error:
#                error(tag, Error, 2)
#            except OSError (ValueError, NameError):
#                error(tag, "Command {0}, {1}".format(stringify(commandargs), NameError), ValueError)
#            else:
#                if options.simulate == False:
#                    try:
#                        s.jobid = int(stdout.split('.', 1)[0])
#                    except ValueError as e:
#                        warning(tag, "Wrong parsing of jobid {0} not being an integer, {1}".format(stdout.split('.', 1)[0], e))
#        job_list.append(s.jobid)
#        verbose(tag, "{0} {1}".format(s.action, stringify(commandargs)))
         print("Launching scripts {0} ".format(str(s.script)))
         try:
             verbose(tag,"Launching scripts {0} ".format(str(s.script)))
             stdout = subprocess.check_output(commandargs)
         except subprocess.CalledProcessError as Error:
                error(tag, Error, 2)
         except OSError (ValueError, NameError):
                error(tag, "Command {0}, {1}".format(stringify(commandargs), NameError), ValueError)  

         print(commandargs)
         job_list.append(s.script) 
        
    return job_list
 ##############################################################################
#
# getqueuejoblist
#
##############################################################################
def getqueuejoblist(sequence_list):
    tag = gettag()
# We have to work out the method to get if the sequence has been submitted or not
    import subprocess
    from os.path import join
    from osa.configs import config
    command = config.cfg.get('ENV', 'SBATCHBIN')
    commandargs = [command]
    queue_list = []
    print("DEBUG",commandargs)
    try:
        xmloutput = subprocess.check_output(commandargs)
    except subprocess.CalledProcessError as Error:
        error(tag, 'Command "{0}" failed, {1}'.format(stringify(commandargs), Error), 2)
    except OSError as ValueError: 
        error(tag, 'Command "{0}" failed, {1}'.format(stringify(commandargs), NameError), ValueError)
    else:
#        verbose(key, "qstat -x gives the folloging output\n{0}".format(xml).rstrip())
        if len(xmloutput) != 0:
            import xml.dom.minidom
            import xmlhandle
            document = xml.dom.minidom.parseString(xmloutput)
            queue_list = xmlhandle.xmlhandleData(document)
            setqueuevalues(queue_list, sequence_list)
    print("DEBUG",command)
    return queue_list
##############################################################################
#
# setqueuevalues
#
##############################################################################
def setqueuevalues(queue_list, sequence_list):
    tag = gettag()
    for s in sequence_list:
        s.tries = 0
        for q in queue_list:
            if s.jobname == q['name']:
                s.action = 'Check'
                s.jobid = q['jobid']
                s.state = q['state']
                if s.state == 'C' or s.state == 'R':
                    s.jobhost = q['jobhost']
                    if s.tries == 0:
                        s.cputime = q['cputime']
                        s.walltime = q['walltime']
                    else:
                        try:
                            s.cputime = sumtime(s.cputime, q['cputime'])
                        except AttributeError as ErrorName:
                            warning(tag, "{0}".format(ErrorName))
                        try:
                            s.walltime = sumtime(s.cputime, q['walltime'])
                        except AttributeError as ErrorName:
                            warning(tag, "{0}".format(ErrorName))
                    if s.state == 'C':
                        s.exit = q['exit']
                s.tries += 1
                verbose(tag, "Attributes of sequence {0}, {1}, {2}, {3}, {4}, {5}, {6} updated".format(s.seq, s.action, s.jobid, s.state, s.jobhost, s.cputime, s.exit))
##############################################################################
#
# setqueuevalues
#
##############################################################################
def sumtime(a, b):
    tag = gettag()
    # Beware of the strange format of timedelta http://docs.python.org/library/datetime.html?highlight=datetime#datetime.timedelta
    import datetime
    a_hh, a_mm, a_ss = a.split(':')
    b_hh, b_mm, b_ss = b.split(':')

    # Strange error: invalid literal for int() with base 10: '1 day, 0'
    if (' day, ' in a_hh):
        a_hh = int(a_hh.split(' day, ')[0])*24+int(a_hh.split(' day, ')[1])
    elif (' days, ' in a_hh):
        a_hh = int(a_hh.split(' days, ')[0])*24+int(a_hh.split(' days, ')[1])

    ta = datetime.timedelta(0, int(a_ss), 0, 0, int(a_mm), int(a_hh), 0 )
    tb = datetime.timedelta(0, int(b_ss), 0, 0, int(b_mm), int(b_hh), 0)
    tc = ta + tb
    c = str(tc)
    if len(c) == 7:
        c = '0' + c
    return c

#=============================================================
def date_in_yymmdd(datestring):
    # This is convert date string(yyyy_mm_dd) from the NightSummary in
    # (yy_mm_dd) format 
    # Depending on the time, +1 is added to date to consider the convention of
    # filenaming based on observation date
    from os.path import join
    #date = datestring.split('-')
    #da   = [ch for ch in date[0]]
    date = list(datestring)
    yy   =''.join(date[2:4])
    mm   =''.join(date[4:6])
    dd   =''.join(date[6:8])
    ## Change the day
    #time = timestring.split(':')
    #if (int(time[0]) >= 17 and int(time[0]) <= 23):
    #   dd = str(int(date[2]))
    #else:
    #   dd   = date[2]
    return yy,mm,dd
