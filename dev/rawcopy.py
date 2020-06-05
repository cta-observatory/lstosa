#!/usr/bin/env python2.7
##############################################################################
#
# rawcopy.py
#
# Copyright 2012 Alejandro Lorca <alejandro.lorca@fis.ucm.es>
#
##############################################################################
from standardhandle import output, verbose, warning, error, errornonfatal, \
    gettag
from dev.mail import send_email

__all__ = ["raw_copy", "is_activity_over", "Pool", "MountPool"]


def raw_copy():
    """ A program to loop over mounting and umounting a xfs disk location and
    proceed with a copy-or-compress of the raw files into a final destination.
    There is a locking of the activity and a the copy-or-compress takes places
    as another user. """
    tag = gettag()

    from report import start
    """ Starting the algorithm. """

    if not options.nocheck and is_activity_over():
        # Exit
        error(tag, "Activity over for {0} {1}".format(
            options.date, options.tel_id), 1)

    if is_activity_locked(lock):
        # Exit
        error(tag, "Activity locked for {0}".format(options.tel_id), 1)
    elif acquire_lock(lock):
        try:
            """ Initiating report. """
            start(tag)
            """ The main loop starts here until the process finishes """
            loop_until_activity_is_over()
        except Exception as e:
            # release the log and exit
            """ Rawcopy failed, releasing lock and raising the error """
            release_lock(lock)
            message = dict()
            message['Subject'] = 'Rawcopy crash'
            message['Content'] = \
                'Rawcopy crashed with the following error: %s' % (str(e))
            send_email(message)
            error(tag, "rawcopy loop crashed with error {}".format(e), 1)
        else:
            """ Unlock for a proper ending """
            release_lock(lock)
    else:
        """ Locking failed for some reason, stop """
        error(tag, "Activity for {0} could not be locked".format(
            options.tel_id), 1)
##############################################################################
#
# is_activity_over
#
##############################################################################
def is_activity_over():
    tag = gettag()

    """ Get the name and check for the existence of the flag file. """

    from os.path import join, exists, isfile
    from config import cfg
    from utils import magicdate_to_dir

    basename = cfg.get('COPY', 'activityoverbasename')
    flag_file = join(cfg.get(options.tel_id, 'activityoverdir'),\
     magicdate_to_dir(options.date), basename)
    if exists(flag_file) and isfile(flag_file):
        verbose(tag, "Flag file {0} indicates activity is over".\
         format(flag_file))
        return True
    else:
        return False
##############################################################################
#
# is_activity_locked
#
##############################################################################
def is_activity_locked(flag_file):
    tag = gettag()

    """ Get the name and check for the existence of the lock file. """

    from os.path import exists, isfile

    verbose(tag, "Lock file {0}".format(flag_file))
    if exists(flag_file) and isfile(flag_file):
        return should_remove_lock(flag_file)
        #return True
    else:
        return False
##############################################################################
#
# Should lock be removed
#
##############################################################################
def should_remove_lock(flag_file):
    tag = gettag()
    """ Get the path to the log file and check if we should remove the lock. """

    from os import environ

    try:
        log_file_path = environ['outfile']
    except:
        warning(tag, "Environment variable outfile not found, cannot check Rawcopy log")
        return(False)

    nlines=3
    with open(log_file_path) as log_file:
        content=log_file.readlines()
        nonempty = [line.strip() for line in content if line.strip()]
        number_locks = sum(['Activity locked for ' in line for line in nonempty[-nlines:]])
    
    if (number_locks is nlines):
        verbose(tag, "Lock should be released, removing it")
        release_lock(flag_file)
        return(True)

    return(False)
##############################################################################
#
# get_lockname
#
##############################################################################
def get_lockname():
    tag = gettag()

    """ Get the name of the lock file we are looking for """

    from os.path import join
    from config import cfg

    basename = options.tel_id + cfg.get('COPY', 'locksuffix')
    lock_file = join(cfg.get(options.tel_id, 'lockdir'), basename)
    return lock_file
##############################################################################
#
# acquire_lock
#
##############################################################################
def acquire_lock(lock_file):
    tag = gettag()

    """ Get the name and lock with a file to avoid multiple instances. """

    from os import getpid
    from socket import gethostname
    from utils import createlock

    pid = getpid()
    hostname = gethostname()
    content = "{0}:{1}".format(hostname, pid)
    if createlock(lock_file, content):
        verbose(tag, "Lock file created {0}".format(lock_file))
        return True 
    else:
        return False 
##############################################################################
#
# loop_until_activity_is_over 
#
##############################################################################
def loop_until_activity_is_over():
    tag = gettag()

    """ Routine to wait for more files (and mounting/umounting if needed) """

    from time import sleep
    from config import cfg

    """ General config parameters """
    wait_if_only_pending = int(cfg.get('COPY', 'offsetfileseconds'))
    wait_if_no_new = 2 * wait_if_only_pending

    while (not is_activity_over()) or options.nocheck:
        rc = 0
        while rc == 0:
            rc = handle_pools()

        if rc == 1:
            # More files are pending but they need still to be copied
            output(tag, "Waiting for {0}s...".format(wait_if_only_pending))
            try:
                sleep(wait_if_only_pending)
            except (KeyboardInterrupt, SystemExit):
                options.nocheck = False
                output(tag, "Program quitted by user")
                break
        elif rc == 2:
            # No more files are pending
            if is_daq_over() or (is_data() and is_nighttime_over(wait_if_only_pending)):
                set_activity_over()
            elif not is_data() and is_nighttime_over(wait_if_only_pending):
                output(tag, "Time to break the loop and go to sleep without data...")
                break
            else:
                # Wait and check if new files appeared
                output(tag, "Waiting for {0}s...".format(wait_if_no_new))
                try:
                    sleep(wait_if_no_new)
                except (KeyboardInterrupt, SystemExit):
                    options.nocheck = False
                    output(tag, "Program quitted by user")
                    break
        else:
            error(tag, "Exit status from copy={0} not foreseen."\
             .format(rc), rc)
##############################################################################
#
# handle_pools 
#
##############################################################################
def handle_pools():
    tag = gettag()

    """ Part starting the pool of files and organizing what to do """

    from datetime import datetime, timedelta
    from os.path import join
    from config import cfg

    """ Reading config for pools """
    offset = int(cfg.get('COPY', 'offsetfileseconds'))
    """ meta and raw fs are the same """
    meta_fs = cfg.get(options.tel_id, 'rawfilesystem')
    meta_suffix = cfg.get('COPY', 'metasuffix')
    report_suffix = cfg.get('COPY', 'reportsuffix')
    raw_suffix = cfg.get('COPY', 'rawsuffix')
    compress_suffix = cfg.get('COMPRESS', 'suffix')
    meta_path = join(cfg.get(options.tel_id, 'metasubdir'), options.date)
    report_path = join(cfg.get(options.tel_id, 'reportsubdir'), options.date)
    raw_path = join(cfg.get(options.tel_id, 'rawsubdir'), options.date)
    compress_path = join(cfg.get(options.tel_id, 'rawoutputdir'), options.date)

    """ Init pools: XFS for meta is the first 
        and Local for the rest to avoid remounts """
    output(tag, "Initiating meta pool")
    meta_pool = XFSPool(meta_fs, meta_path)
    output(tag, "Initiating report pool")
    report_pool = LocalPool(report_path)
    output(tag, "Initiating raw pool")
    raw_pool = LocalPool(raw_path)
    output(tag, "Initiating compress pool")
    compress_pool = LocalPool(compress_path)

    """ Discover files older than timestamp """
    timestamp = datetime.utcnow() - timedelta(seconds=offset)
    output(tag, "Discovering meta files in pool")
    meta_pool.discover(meta_suffix, timestamp, meta_processed)
    output(tag, "Discovering report files in pool")
    report_pool.discover(report_suffix, timestamp, report_processed)
    output(tag, "Discovering raw files in pool")
    raw_pool.discover(raw_suffix, timestamp, raw_processed)
    """ Time reset for the output """
    timestamp = datetime.utcnow()
    output(tag, "Discovering compress files in pool")
    compress_pool.discover(compress_suffix, timestamp, compress_processed)

    """ Match file objects between pool sets """
    output(tag, "Assigning files to one another in pools")
    interpool_match(meta_pool, report_pool, raw_pool, compress_pool)
    interpool_missing_summary(meta_pool, report_pool, raw_pool, compress_pool)

    """ Process new accepted files in order, meta, report, raw """
    interpool_process(meta_pool, report_pool, raw_pool, compress_pool)

    accepted_max = max(meta_pool.accepted, report_pool.accepted,\
     raw_pool.accepted)
    discarded_max = max(meta_pool.discarded, report_pool.discarded,\
     raw_pool.discarded)
    if len(accepted_max) != 0:
        rc = 0
    elif len(discarded_max) != 0:
        rc = 1
    else:
        rc = 2

    """ Destroy the pools after the iteration """
    output(tag, "Destroying meta pool")
    meta_pool.destroy()
    output(tag, "Destroying report pool")
    report_pool.destroy()
    output(tag, "Destroying raw pool")
    raw_pool.destroy()
    output(tag, "Destroying compress pool")
    compress_pool.destroy()

    return rc
##############################################################################
#
# is_data
#
##############################################################################
def is_data():
    tag = gettag()
    """ Check if there is data today, of any kind """
    total_objects = len(raw_processed) + len(compress_processed) + len(meta_processed)
    if total_objects == 0:
        return False
    else:
        return True
##############################################################################
#
# is_mounted
#
##############################################################################
def is_mounted(fsystem):
    tag = gettag()

    """ Check if the fyle system is mounted """

    from os.path import ismount
    return ismount(fsystem)
##############################################################################
#
# is_nighttime_over
#
##############################################################################
def is_nighttime_over(offset):
    tag = gettag()

    """ This functions allows us to know if the schedule for datataking is 
        over and we should exit the activity anyway. """

    from datetime import datetime, timedelta
    from utils import get_night_limit_timestamp
    now = datetime.utcnow()
    verbose(tag, "Now is {0}".format(now))
    night_limit_timestamp = get_night_limit_timestamp()

    """ Convert string into seconds and add offset """
    dt_obj = datetime.strptime(night_limit_timestamp, "%Y-%m-%d %H:%M:%S")
    dt_obj += timedelta(seconds=offset)

    if now <= dt_obj:
        verbose(tag, "We are still ahead of night time")
        return False
    else:
        verbose(tag, "We are behind night time")
        return True
##############################################################################
#
# mount_or_unmount (obvious)
#
##############################################################################
def mount_or_unmount(action, fsystem, wait_remount):
    tag = gettag()

    """ Routine to mount or umount a filesystem and return the time """

    import subprocess
    from datetime import datetime
    from time import sleep
    commandargs = [action, fsystem]
    t_start = None 
    try:
        rc = subprocess.check_call(commandargs)
    except subprocess.CalledProcessError as Error:
        # NOTE/ToDo: Crash in the disk, try to mount it. [mireia] 
        if Error.returncode == 1:
            error(tag, "{0}".format(Error), Error) 
        elif Error.returncode == 2:
            # Classical umounting failure in non-mounted fs
            error(tag, "{0}".format(Error), Error) 
        elif Error.returncode == 32:
            # Classical mounting failure without umounting first
            mount_or_unmount('umount', fsystem, wait_remount)
            sleep(wait_remount)
            t_start = mount_or_unmount('mount', fsystem, wait_remount)
        else:
            # Unlock activity and exit
            release_lock(lock)
            error(tag, "{0}".format(Error), Error.returncode)
    else:
        t_start = datetime.utcnow()
        output(tag, "{0} {1}ed at {2}".format(fsystem, action, t_start))
    return t_start
##############################################################################
#
# is_daq_over (returns False/True if we expect/do not expect more raw data)
#
##############################################################################
def is_daq_over():
    tag = gettag()

    """ Check whether the previous data taking activity is over. """
    
    from os.path import join, exists, isfile
    from config import cfg
    from utils import magicdate_to_dir
    # We could eventually use the database.
    currentdate_dir = magicdate_to_dir(options.date)
    flag_file = join(cfg.get('COPY', 'previousactivityoverdir'),\
     currentdate_dir,\
     cfg.get('COPY', 'previousactivityoverbasename'))
    if exists(flag_file) and isfile(flag_file):
        return True
    else:
        return False
##############################################################################
#
# set_activity_over
#
##############################################################################
def set_activity_over():
    tag = gettag()

    """ Close the activity for that night and telescope. """
    from os.path import join
    from config import cfg
    from utils import createlock, magicdate_to_dir

    """ Updating the summary table """
    update_summary_db()
    """ Creating the flag file """
    ao_basename = cfg.get('COPY', 'activityoverbasename')
    flag_file = join(cfg.get(options.tel_id, 'activityoverdir'),\
     magicdate_to_dir(options.date), ao_basename)
    output(tag, "Activity COPY/COMPRESS set as finished")
    return createlock(flag_file, '')
##############################################################################
#
# set_daq_start
#
##############################################################################
def set_summary_start(activity, start_string):
    tag = gettag()

    """ Set into the database the DAQ starting time according to xml meta """

    from dev.mysql import select_db, update_or_insert_and_select_id_db
    from config import cfg
    from utils import magicdate_to_iso
    server = cfg.get('MYSQL', 'server')
    user = cfg.get('MYSQL', 'user')
    db = cfg.get('MYSQL', 'database')
    table = cfg.get('MYSQL', 'summary')
    id = None
    start = None
    is_finished = False
    night = magicdate_to_iso(options.date)
    """ The whole script makes sense when the first data has been copied """
    where_conditions = {'NIGHT': night, 'TELESCOPE': options.tel_id,\
     'ACTIVITY': activity}
    selections = ['ID', 'START', 'IS_FINISHED']
    querymatrix = select_db(server, user, db, table, selections,\
     where_conditions)
    if len(querymatrix) == 1:
        id, start, is_finished = querymatrix[0]
        verbose(tag, "ID={0}, START={1} from select_db".format(id, start))
    elif len(querymatrix) > 1:
        errornonfatal(tag, "{0} entries in {1} for {2}, {3}, {4}".\
         format(len(querymatrix), table, activity, night, options.tel_id))
    else:
        # Nothing selected, no entry
        id = 0
        verbose(tag, "NO ID in {0} for {1}, {2}, {3}"\
         .format(table, activity, night, options.tel_id))

    """ Choose the start time if not found or NULL """
    if (not start) or (start == 'NULL'):
        start = start_string
        verbose(tag, "{0} starting time = {1}".format(activity, start))
        """ Updating or Insert into database """
        table = cfg.get('MYSQL', 'summary')
        where_conditions = {'NIGHT': night, 'TELESCOPE': options.tel_id,\
        'ACTIVITY': activity}
        assignments = {'START': start}
        if not is_finished:
            assignments['IS_FINISHED'] = 0
            assignments['END'] = None
        id = update_or_insert_and_select_id_db(server, user, db, table,\
         assignments, where_conditions)
    return id
##############################################################################
#
# calculate_md5sum
#
##############################################################################
def calculate_md5sum(file):
    tag = gettag()

    """ Similar to md5sum file | cut -d\  -f1 """
    import hashlib
    m = hashlib.md5()
    # Reading 32768 blocks of 128 bytes seems a good approach
    block_size = 128 * 32768 
    with open(file, 'r') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            m.update(data)
    return(m.hexdigest())
##############################################################################
#
# create_md5sum_inverse_dict
#
##############################################################################
def create_md5sum_inverse_dict(file):
    tag = gettag()
    from osa.utils.iofile import readfromfile
    content = readfromfile(file)
    md5sum_dict = {}
    if content:
        for line in content.splitlines():
            md5sum, path = line.split()
            md5sum_dict[path] = md5sum
    return md5sum_dict
##############################################################################
#
# update_md5sums
#
##############################################################################
def update_md5sums(hash, file, md5sums_file):
    tag = gettag()
    from osa.utils.iofile import writetofile, appendtofile, sedsi
    from os.path import exists
    """ Similar to md5sum file >> md5sums_file with update option """
    string = "{0}  {1}\n".format(hash, file)
    if not exists(md5sums_file):
        writetofile(md5sums_file, string)
    else:
        file_to_hash = create_md5sum_inverse_dict(md5sums_file)
        if file in file_to_hash.keys():
            """ Existing file """
            if file_to_hash[file] != hash:
                """ Other hash, update md5sums_file """
                sedsi(file_to_hash[file], hash, md5sums_file)
        else:
            appendtofile(md5sums_file, string)
##############################################################################
#
# update_summary_db
#
##############################################################################
def update_summary_db():
    tag = gettag()

    """ Populate the MySQL database with updated info"""

    from datetime import datetime
    from dev.mysql import update_ignore_db
    from utils import magicdate_to_iso
    from config import cfg
    server = cfg.get('MYSQL', 'server')
    user = cfg.get('MYSQL', 'user')
    database = cfg.get('MYSQL', 'database')
    table = cfg.get('MYSQL', 'summary')
    date = magicdate_to_iso(options.date)
    where_conditions =  {'TELESCOPE': options.tel_id, 'NIGHT': date}
    set_assignments = {'IS_FINISHED': 1}
    for activity in ['COPY', 'COMPRESS']:
        where_conditions['ACTIVITY'] = activity
        set_assignments['END'] = datetime.utcnow()
        update_ignore_db(server, user, database, table,\
         set_assignments, where_conditions)
##############################################################################
#
# insert_storage_db
#
##############################################################################
def insert_storage_db(daq_id, report_id, tel_id, night, hostname,\
     destination_path, m_time, size, md5sum, file_type):
    tag = gettag()

    """ Fill the MySQL table """
    from dev.mysql import insert_ignore_db
    from config import cfg
    from utils import magicdate_to_iso
    # This is always a disaster, convert into a proper date format
    date = magicdate_to_iso(night)
    # Constructing the mysql column assignments
    assignments = {"DAQ_ID": daq_id, "REPORT_ID": report_id,\
     "TELESCOPE": tel_id, "NIGHT": date, "HOSTNAME": hostname,\
     "FILE_PATH": destination_path, "M_TIME":m_time, "SIZE":size,\
     "MD5SUM": md5sum, "FILE_TYPE": file_type}
    conditions = {}
    insert_ignore_db(cfg.get('MYSQL', 'server'), cfg.get('MYSQL', 'user'),\
     cfg.get('MYSQL', 'database'), cfg.get('MYSQL', 'storage'),\
     assignments, conditions)
##############################################################################
#
# insert_copy_db (fill the mysql table)
#
##############################################################################
def insert_copy_db(daq_id, report_id, hostname, original_path, m_time, size,\
     md5sum, elapsed_transfer_time):
    tag = gettag()

    """ """
    from config import cfg
    from dev.mysql import insert_ignore_db
    # Constructing the mysql column assignments
    server = cfg.get('MYSQL', 'server')
    user = cfg.get('MYSQL', 'user')
    database = cfg.get('MYSQL', 'database')
    table = cfg.get('MYSQL', 'copy')
    assignments = {\
     "DAQ_ID": daq_id,\
     "REPORT_ID": report_id,\
     "HOSTNAME": hostname,\
     "ORIGINAL_FILE_PATH": original_path,\
     "ORIGINAL_M_TIME":m_time,\
     "ORIGINAL_SIZE":size,\
     "ORIGINAL_MD5SUM":md5sum,\
     "ELAPSED_TIME": elapsed_transfer_time,\
     }
    conditions = {}
    insert_ignore_db(server, user, database, table,\
     assignments, conditions)
##############################################################################
#
# release_lock
#
##############################################################################
def release_lock(file):
    tag = gettag()

    """ """
    from os import unlink
    try:
        unlink(file)
    except OSError as e:
        error(tag, "Could not remove lock file {0}, {1}".\
         format(file, e.strerror), e.errno)
    else:
        verbose(tag, "Lock successfully released")
##############################################################################
#
# def is_copy_finished 
#
##############################################################################
def is_copy_finished(tel_id, night):
    # File-based approach
    flag_file = get_copy_flag_file(tel_id, night)
    if os.path.exists(flag_file):
        return True
    else:
        return False
##############################################################################
#
# get_xml_entries 
#
##############################################################################
def get_xml_entries(xml):
    tag = gettag()

    import xml.dom.minidom as minidom
    assignments = {}
    try:
        doc = minidom.parse(xml)
    except:
        warning(tag, "Malformed xml file {0}".format(xml))
    else:
        node = doc.documentElement
        entries = doc.getElementsByTagName('DAQEntry')
        columns = {\
         'Telescope': 'TELESCOPE',\
         'Night': 'NIGHT',\
         'Run': 'RUN',\
         'SubRun': 'SUBRUN',\
         'SourceWobble': 'SOURCEWOBBLE',\
         'NumberEvents': 'EVENTS',\
         'Start': 'START',\
         'End': 'END',\
         'Hostname': 'HOSTNAME',\
         'File_Path': 'FILE_PATH',
         }
        for entry in entries:
            for key in columns.keys():
                try:
                    if key == 'Start' or key == 'End':
                        nodes = entry.getElementsByTagName(key)
                        for node in nodes:
                            entryObjDate = node.getElementsByTagName('date')[0]
                            entryObjTime = node.getElementsByTagName('time')[0]
                            entryObj = get_text(entryObjDate) + ' ' + \
                             get_text(entryObjTime)
                    else:
                        entryObj = get_text(entry.getElementsByTagName(key)[0])
                except IndexError:
                    entryObj = None
                else:
                    assignments[columns[key]] = entryObj
        for i in assignments.keys():
            verbose(tag, "{0} => {1}".format(i, assignments[i]))
    return assignments
##############################################################################
#
# get_text
#
##############################################################################
def get_text(obj):
    tag = gettag()

    nodes = obj.childNodes
    for node in nodes:
        return node.data
    return "{0}".format(obj)

##############################################################################
#
# class Pool and its child classes
#
##############################################################################
""" Pool class is meant as a File space where to look for specific files """
class Pool(object):
    def __init__(self, path):
        tag = gettag()
        from os.path import exists, isdir
        """ Initialization routine """
        self.path = path
        self.exists = False
        self.isdir = False

        self.discovered = set()
        self.processed = set()
        self.discarded = set()
        self.accepted = set()

        if exists(path):
            self.exists = True
            if isdir(path):
                self.isdir = True
##############################################################################
    def discover(self, suffix, limit_timestamp, processed):
        tag = gettag()

        """ This method extract the info about the File objects
            and fill up the list and set of accepted and discarded """
        from glob import glob
        from os.path import join
        from config import cfg

        raw_suffix = cfg.get('COPY', 'rawsuffix')
        compress_suffix = cfg.get('COMPRESS', 'suffix')        
        meta_suffix = cfg.get('COPY', 'metasuffix')
        report_suffix = cfg.get('COPY', 'reportsuffix')

        """ Populate the files from the filesystem """
        filenames = glob(join(self.path, '*{0}'.format(suffix)))
        for f in filenames:
            if suffix == meta_suffix:
                self.discovered.add(MetaFile(f))
            elif suffix == report_suffix:
                self.discovered.add(ReportFile(f))
            elif suffix == raw_suffix:
                self.discovered.add(RawFile(f))
            elif suffix == compress_suffix:
                self.discovered.add(CompressFile(f))
            else:
                error(tag, "Suffix {0} not admitted".format(suffix), 7)

        for fileobj in self.discovered:
            fileobj.update()
            verbose(tag, "fileobj.mtime={0}, limit_timestamp={1}".\
             format(fileobj.mtime, limit_timestamp))
            if fileobj.mtime and fileobj.mtime < limit_timestamp:
                is_processed = False
                for p in processed:
                    if fileobj.path == p.path:
                        self.processed.add(fileobj)
                        is_processed = True
                        break
                if not is_processed:
                    self.accepted.add(fileobj)
            else:
                self.discarded.add(fileobj)
        output(tag,\
         "-> discovered: {0}, processed: {1}, accepted: {2}, discarded: {3}".\
         format(len(self.discovered), len(self.processed),\
         len(self.accepted), len(self.discarded)))
##############################################################################
    def process(self, processed):
        tag = gettag()

        """ This method knows what to do with each File type """
        new_accepted = self.accepted - processed
        new_accepted_list = sorted(new_accepted, key=lambda file: file.mtime)
        for a in new_accepted_list:
            try:
                a.process()
            except (KeyboardInterrupt, SystemExit):
                options.nocheck = False
                break
            else:
                processed.add(a)
##############################################################################
    def destroy(self):
        tag = gettag()
        self.discovered.clear()
        self.processed.clear()
        self.accepted.clear()
        self.discarded.clear()
##############################################################################
#
# class MountPool
#
##############################################################################
""" MountPool class is meant as Pool which has to be eventually mounted """
class MountPool(Pool):
    def __init__(self, fsystem, path):
        tag = gettag()
        from config import cfg
        wait_remount = int(cfg.get('COPY', 'remountseconds'))

        """ Initialization routine """
        super(MountPool, self).__init__(path)
        from os.path import ismount
        from datetime import datetime
        self.fsystem = fsystem
        self.wait_remount = wait_remount
        self.mounted = False
        self.mounted_timestamp = None

        if ismount(fsystem):
            self.mounted = True
            self.mounted_timestamp = datetime.utcnow()
##############################################################################
    def mount(self):
        tag = gettag()
        t_start = mount_or_unmount('mount', self.fsystem, self.wait_remount)
        if t_start:
            self.mounted = True
        else:
            self.mounted = False
        self.mounted_timestamp = t_start
##############################################################################
    def unmount(self):
        tag = gettag()
        t_start = mount_or_unmount('umount', self.fsystem, self.wait_remount)
        if t_start:
            self.mounted = True
        else:
            self.mounted = False
        self.mounted_timestamp = None
##############################################################################
#
# class NFSPool
#
##############################################################################
""" NFSPool class is meant as MountPool which has to remain mounted """
class NFSPool(MountPool):
    def __init__(self, fsystem, path):
        tag = gettag()
        super(NFSPool, self).__init__(fsystem, path)
        self.type = 'nfs'
        if self.mounted == False:
            super(NFSPool, self).mount()
##############################################################################
#
# class XFSPool
#
##############################################################################
""" XFSPool class is meant as MountPool to be systematically remounted """
class XFSPool(MountPool):
    def __init__(self, fsystem, path):
        tag = gettag()
        from time import sleep
        super(XFSPool, self).__init__(fsystem, path)
        self.type = 'xfs'
        if self.mounted == True:
            super(XFSPool, self).unmount()
            sleep(self.wait_remount)
        super(XFSPool, self).mount()
##############################################################################
    def destroy(self):
        tag = gettag()        
        from time import sleep
        super(XFSPool, self).destroy()
        super(XFSPool, self).unmount()
        try:
            sleep(self.wait_remount)
        except (KeyboardInterrupt, SystemExit):
            options.nocheck = False
            output(tag, "Program quitted by user")
            raise 
##############################################################################
#
# class LocalPool
#
##############################################################################
""" LocalPool class is meant as Pool without mount/remount needed """
class LocalPool(Pool):
    def __init__(self, path):
        tag = gettag()
        super(LocalPool, self).__init__(path)
        from datetime import datetime
        self.mounted_timestamp = datetime.utcnow()
##############################################################################
#
# class File and child classes
#
##############################################################################
class File(object):
    def __init__(self, w):
        tag = gettag()
        from os.path import dirname, basename
        self.path = w
        self.dirname = dirname(w)
        self.basename = basename(w)
        self.basename_nosuffix, self.suffix = self.split_suffix()
        self.exists = False
        self.isfile = False
        self.size = None
        self.mtime = None
        self.md5sum = None
        self.update()
##############################################################################
    def split_suffix(self):
        tag = gettag()
        basename_nosuffix, sep, extension = self.basename.rpartition('.')
        suffix = sep + extension
        return basename_nosuffix, suffix
##############################################################################
    def update(self):
        tag = gettag()
        from os.path import exists, isfile, getsize, getmtime
        from datetime import datetime
        if exists(self.path):
            self.exists = True
            if isfile(self.path):
                self.isfile = True
                self.size = getsize(self.path)
                self.mtime = datetime.utcfromtimestamp(getmtime(self.path))
            else:
                warning("{0} is not a file".format(w))
##############################################################################
    def update_md5sum(self):
        tag = gettag()

        """ Similar to md5sum file | cut -d\  -f1 """
        import hashlib
        m = hashlib.md5()
        """ Reading 32768 blocks of 128 bytes seems a good approach """
        block_size = 128 * 32768
        with open(self.path, 'r') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                m.update(data)
        self.md5sum = m.hexdigest()
##############################################################################
    def show(self):
        tag = gettag()
        concept_list = ['path', 'dirname', 'basename', 'suffix', 'exists',\
         'isfile', 'size', 'mtime', 'md5sum']
        for c in concept_list:
            verbose(tag, "{0} = {1}".format(c, self.__dict__[c]))
##############################################################################
#
# class MagicFile
#
##############################################################################
class MagicFile(File):
    def __init__(self, w):
        super(MagicFile, self).__init__(w)
        
        """ Respective file properties """
        self.meta = None
        self.report = None
        self.raw = None
        self.compress = None
        """ MySQL file properties """
        self.mysql_table = None
        self.mysql_id = None
        """ Magic file parsing properties """
        self.date = None
        self.telescope = None
        self.run = None
        self.subrun = None
        self.sourcewobble = None
##############################################################################
    def show(self):
        tag = gettag()
        super(MagicFile, self).show()
        concept_list = ['mysql_table', 'mysql_id', 'date', 'telescope',\
         'run', 'subrun', 'sourcewobble']
        for c in concept_list:
            verbose(tag, "{0} = {1}".format(c, self.__dict__[c]))
##############################################################################
    def interact_with_db_generic(self, assignments, conditions):
        tag = gettag()
        from config import cfg
        from dev.mysql import update_or_insert_and_select_id_db
        server = cfg.get('MYSQL', 'server')
        user = cfg.get('MYSQL', 'user')
        database = cfg.get('MYSQL', 'database')
        
        self.mysql_id = update_or_insert_and_select_id_db(server, user,\
         database, self.mysql_table, assignments, conditions)    
##############################################################################
    def update(self):
        tag = gettag()
        super(MagicFile, self).update()
##############################################################################
#
# class MetaFile
#
##############################################################################
class MetaFile(MagicFile):
    def __init__(self, w):
        tag = gettag()
        from config import cfg
        super(MetaFile, self).__init__(w)
        self.type = cfg.get('COPY', 'metafiletype')
        self.meta = self
        self.mysql_table = cfg.get('MYSQL', 'daq')
        """ Meta properties """
        self.mysql_assignments = {\
         'TELESCOPE': None,\
         'NIGHT': None,\
         'RUN': None,\
         'SUBRUN': None,\
         'SOURCEWOBBLE': None,\
         'EVENTS': None,\
         'START': None,\
         'END': None,\
         'HOSTNAME': None,\
         'FILE_PATH': None,\
         }
        self.parse_magic_properties()
##############################################################################
    def parse_magic_properties(self):
        tag = gettag()
        field_list = self.basename_nosuffix.split('_')
        self.date = field_list[2]
        self.telescope = field_list[1]
        self.run = field_list[3]
        self.subrun = field_list[4]
        super(MetaFile, self).show()
##############################################################################
    def process(self, block):
        tag = gettag()
        self.extract_xml()
        self.interact_with_db()
##############################################################################
    def extract_xml(self):
        tag = gettag()
        if self.isfile == True:
            xmldict = get_xml_entries(self.path)
            for i in xmldict.keys():
                self.mysql_assignments[i] = xmldict[i]
##############################################################################
    def interact_with_db(self):
        tag = gettag()
        from copy import copy
        assignments = copy(self.mysql_assignments)
        conditions = {'FILE_PATH': assignments.pop('FILE_PATH')}
        super(MetaFile, self).\
         interact_with_db_generic(assignments, conditions)
##############################################################################
#
# class ReportFile
#
##############################################################################
class ReportFile(MagicFile):
    def __init__(self, w):
        from config import cfg
        super(ReportFile, self).__init__(w)
        self.type = cfg.get('COPY', 'reportfiletype')
        self.report = self
        self.mysql_table = cfg.get('MYSQL', 'report')
        """ Report properties """
        self.mysql_assignments = {\
         'DRIVE_REPORTS': 0,\
         'CAMERA_REPORTS': 0,\
         'AMC_REPORTS': 0,\
         'STARGUIDER_REPORTS': 0,\
         'TRIGGER_REPORTS': 0,\
         'PULSAR_REPORTS': 0,\
         'RECEIVER_REPORTS': 0,\
         'CALIBRATION_REPORTS': 0,\
         'COOLING_REPORTS': 0,\
         'LIDAR_REPORTS': 0,\
         'BM_REPORTS': 0,\
         'WEATHER_REPORTS': 0,\
         'CC_REPORTS': 0,\
         'RUN_REPORTS': 0,\
         }
        self.parse_magic_properties()
##############################################################################
    def parse_magic_properties(self):
        tag = gettag()
        #print(self.basename_nosuffix)
        #try:
        str_1, str_2 = self.basename_nosuffix.split('.', 1)
        #except:
            #print(self.basename_nosuffix)
            #raise
        #else:
        fields_1 = str_1.split('_')
        fields_2 = str_2.split('_')
        self.date = fields_1[0]
        self.telescope = fields_1[1]
        self.run = fields_1[2]
        self.subrun = fields_2[0]
        self.sourcewobble = fields_2[2]
        super(ReportFile, self).show()
##############################################################################
    def process(self, block):
        tag = gettag()
        self.extract_rep()
        self.interact_with_db(block)
##############################################################################
    def extract_rep(self):
        tag = gettag()
        from re import match
        from os.path import exists
        content = None

        if exists(self.path):
            try:
                with open(self.path, 'r') as i:
                    content = i.read()
            except (OSError, IOError) as (ValueError, NameError):
                """ Something wrong with the file! """
                errornonfatal(tag, "Could not read and process report {0}".\
                 format(self.path))
            else:
                for l in content.splitlines():
                    for k in self.mysql_assignments.keys():
                        rep_field = k.rstrip('S').replace('_', '-') 
                        if match(rep_field, l):
                            self.mysql_assignments[k] += 1
        else:
            errornonfatal(tag, "Report dissapeared {0}".format(self.path))
##############################################################################
    def interact_with_db(self, block):
        tag = gettag()
        from utils import magicdate_to_iso
        from socket import gethostname
        from copy import copy
        daq_id = None
        if block.meta:
           daq_id = block.meta.mysql_id
        night = magicdate_to_iso(options.date)
        hostname = gethostname()
        assignments = copy(self.mysql_assignments)
        assignments.update({\
         'DAQ_ID': daq_id,\
         'TELESCOPE': options.tel_id,\
         'NIGHT': night,\
         'HOSTNAME': hostname,\
         })
        conditions = {'FILE_PATH': self.path}

        super(ReportFile, self).\
         interact_with_db_generic(assignments, conditions)
##############################################################################
#
# class RawFile
#
##############################################################################
class RawFile(MagicFile):
    def __init__(self, w):
        tag = gettag()
        from config import cfg
        super(RawFile, self).__init__(w)
        self.type = cfg.get('COPY', 'rawfiletype')
        self.raw = self
        self.mysql_table = cfg.get('MYSQL', 'copy')
        self.mysql_assignments = {\
         'DAQ_ID': None,\
         'REPORT_ID': None,\
         'HOSTNAME': None,\
         'ORIGINAL_FILE_PATH': None,\
         'ORIGINAL_MD5SUM': None,\
         'ORIGINAL_M_TIME': None,\
         'ORIGINAL_SIZE': None,\
         'ELAPSED_TIME': None,\
         }
        self.parse_magic_properties()
##############################################################################
    def parse_magic_properties(self):
        tag = gettag()
        str_1, str_2 = self.basename_nosuffix.split('.', 1)
        fields_1 = str_1.split('_')
        fields_2 = str_2.split('_')
        self.date = fields_1[0]
        self.telescope = fields_1[1]
        self.run = fields_1[2]
        self.subrun = fields_2[0]
        self.sourcewobble = fields_2[2]
        super(RawFile, self).show()
##############################################################################
    def process(self, block):
        tag = gettag()
        """ Do not process zero size raw files """
        if self.size != 0:
            is_copied = self.copy_and_compress(block)
            if is_copied:
                self.interact_with_db(block)
##############################################################################
    def copy_and_compress(self, block):
        tag = gettag()
        from os.path import join, exists
        from config import cfg
        output_dir = block.compress.dirname
        md5sums_basename = cfg.get('COPY', 'md5sumsbasename')
        md5sums_file = join(output_dir, md5sums_basename)
        md5sums_compress_basename = cfg.get('COMPRESS', 'md5sumsbasename')
        md5sums_compress_file = join(output_dir, md5sums_compress_basename)

        """ Avoid copy/compress if it is OK """
        if block.compress.exists:
            warning(tag, "Compress file {0} exists".\
             format(block.compress.path))
            # To COMMENT
            #if '05062984.006' not in block.compress.path:
            #    return False
            if exists(md5sums_compress_file):
                f_to_h = create_md5sum_inverse_dict(md5sums_compress_file)
                if block.compress.basename in f_to_h.keys():
                    md5sum_in_file = f_to_h[block.compress.basename] # stringify?
                    block.compress.update_md5sum() # should be a string already
                    if block.compress.md5sum == md5sum_in_file:
                        """ They are equal, nothing to do """
                        return False
                else:
                    """ File is there but not in md5sums, probably corrupt """
                    pass
        """ The activity and the md5sums file update """
        output(tag, "Copy/Compress {0}".format(self.path))
        self.catmd5pigzmd5(block)
        update_md5sums(self.md5sum, self.basename, md5sums_file)
        update_md5sums(block.compress.md5sum, block.compress.basename,\
         md5sums_compress_file)
        return True
##############################################################################
    def catmd5pigzmd5(self, block):
        tag = gettag()
        """ This tool reads the file and while reading chunks computes the
            md5sum and writes in parallel """
        from datetime import datetime
        from config import cfg
        import subprocess
        command = cfg.get('COMPRESS', 'application')
        input_file = self.path
        output_dir = block.compress.dirname
        commandargs = [command, input_file, output_dir]

        """ Let set the start and do the job """
        start = datetime.utcnow()
        try:
            md5sum_list = subprocess.check_output(commandargs).splitlines()
        except subprocess.CalledProcessError as e:
            if e.returncode is 255:
                release_lock(lock)
                raise error(tag, "Failed, {0}".format(e.output))
            else:
                errornonfatal(tag, "Failed, {0}".format(e.output))
        except (KeyboardInterrupt, SystemExit):
            release_lock(lock)
            output(tag, "Exiting by user or system...")
            sys.exit(1)
        else:
            end = datetime.utcnow()
            self.mysql_assignments['ELAPSED_TIME'] = end - start
            self.md5sum = md5sum_list[0]
            block.compress.md5sum = md5sum_list[1]
##############################################################################
    def interact_with_db(self, block):
        tag = gettag()
        from utils import magicdate_to_iso
        from socket import gethostname
        from copy import copy
        daq_id = None
        report_id = None
        if block.meta:
            daq_id = block.meta.mysql_id
        if block.report:
            report_id = block.report.mysql_id
        night = magicdate_to_iso(options.date)
        hostname = gethostname()
        assignments = copy(self.mysql_assignments)
        assignments.update({\
         'DAQ_ID': daq_id,\
         'REPORT_ID': report_id,\
         'HOSTNAME': hostname,\
         'ORIGINAL_FILE_PATH': self.path,\
         'ORIGINAL_MD5SUM': self.md5sum,\
         'ORIGINAL_M_TIME': self.mtime,\
         'ORIGINAL_SIZE': self.size,\
         })
        conditions = {'ORIGINAL_FILE_PATH': assignments.pop('ORIGINAL_FILE_PATH')}
        super(RawFile, self).\
         interact_with_db_generic(assignments, conditions)
##############################################################################
#
# class CompressFile
#
##############################################################################
class CompressFile(MagicFile):
    def __init__(self, w):
        tag = gettag()
        from config import cfg
        super(CompressFile, self).__init__(w)
        self.type = cfg.get('COMPRESS', 'filetype')
        self.compress = self
        self.mysql_table = cfg.get('MYSQL', 'storage')
        self.mysql_assignments = {\
         'DAQ_ID': None,\
         'REPORT_ID': None,\
         'TELESCOPE': None,\
         'NIGHT': None,\
         'HOSTNAME': None,\
         'FILE_PATH': None,\
         'FILE_TYPE': None,\
         'SIZE': None,\
         'MD5SUM': None,\
         'M_TIME': None,\
         }
        self.parse_magic_properties()
##############################################################################
    def parse_magic_properties(self):
        tag = gettag()
        basename_norawext, sep, rawext = self.basename_nosuffix.rpartition('.')
        str_1, str_2 = basename_norawext.split('.', 1)
        fields_1 = str_1.split('_')
        fields_2 = str_2.split('_')
        self.date = fields_1[0]
        self.telescope = fields_1[1]
        self.run = fields_1[2]
        self.subrun = fields_2[0]
        self.sourcewobble = fields_2[2]
        super(CompressFile, self).show()
##############################################################################
    def process(self, block):
        tag = gettag()
        super(CompressFile, self).update()
        self.interact_with_db(block)
##############################################################################
    def interact_with_db(self, block):
        tag = gettag()
        from utils import magicdate_to_iso
        from socket import gethostname
        from copy import copy
        daq_id = None
        report_id = None
        if block.meta:
            daq_id = block.meta.mysql_id
        else:
            pass
        if block.report:
            report_id = block.report.mysql_id
        night = magicdate_to_iso(options.date)
        hostname = gethostname()
        assignments = copy(self.mysql_assignments)
        assignments.update({\
         'DAQ_ID': daq_id,\
         'REPORT_ID': report_id,\
         'TELESCOPE': options.tel_id,\
         'NIGHT': night,\
         'HOSTNAME': hostname,\
         'FILE_PATH': self.path,\
         'FILE_TYPE': self.type,\
         'SIZE': self.size,\
         'MD5SUM': self.md5sum,\
         'M_TIME': self.mtime,\
         })
        conditions = {'FILE_PATH': assignments.pop('FILE_PATH')}
        super(CompressFile, self).\
         interact_with_db_generic(assignments, conditions)
##############################################################################
#
# class FileBlock
#
##############################################################################
class FileBlock(object):
    def __init__(self, b):
        from config import cfg
        tag = gettag()
        self.meta = b[cfg.get('COPY', 'metafiletype')]
        self.report = b[cfg.get('COPY', 'reportfiletype')]
        self.raw = b[cfg.get('COPY', 'rawfiletype')]
        self.compress = b[cfg.get('COMPRESS', 'filetype')]
        """ A mtime attribute is needed to order blocks, usually raw.mtime """
        self.mtime = None
        if self.raw:
            self.mtime = self.raw.mtime
        elif self.meta:
            self.mtime = self.meta.mtime
        elif self.report:
            self.mtime = self.report.mtime

        verbose(tag, "Block initated with mtime {0}".format(self.mtime))
##############################################################################
    def process(self):
        tag = gettag()
        from datetime import datetime
        process_start = datetime.utcnow()

        if self.meta:
            self.meta.process(self)
            if len(meta_processed) == 0:
                set_summary_start('DAQ',self.meta.mysql_assignments['START'])
            meta_processed.add(self.meta)
        if self.report:
            self.report.process(self)
            report_processed.add(self.report)
        if self.raw:
            self.raw.process(self)
            if len(raw_processed) == 0:
                set_summary_start('COPY', process_start) 
            raw_processed.add(self.raw)
        if self.compress:
            self.compress.process(self)
            if len(compress_processed) == 0:
                set_summary_start('COMPRESS', process_start)
            compress_processed.add(self.compress)
##############################################################################
#
# interpool_match
#
##############################################################################
def interpool_match(meta_pool, report_pool, raw_pool, compress_pool):
    tag = gettag()
    """ This function tries to match the objects according to naming """
    from config import cfg
    meta_suffix = cfg.get('COPY', 'metasuffix')
    report_suffix = cfg.get('COPY', 'reportsuffix')
    raw_suffix = cfg.get('COPY', 'rawsuffix')
    compress_suffix = cfg.get('COMPRESS', 'suffix')

    meta_count = 0
    report_count = 0
    compress_count = 0

    for raw in raw_pool.discovered:
        raw_basename_nosuffix = raw.basename.rpartition(raw.suffix)[0]
        raw_splitall = raw_basename_nosuffix.replace('.','_').split('_')
        """ Meta match """
        if not raw.meta:
            meta_basename = 'DatabaseInput_{0}_{1}_{2}_{3}{4}'.\
             format(raw_splitall[1], raw_splitall[0], raw_splitall[2],\
             raw_splitall[3], meta_suffix)
            for meta in meta_pool.discovered:
                if meta.basename == meta_basename:
                    """ Apparently match! """
                    raw.meta = meta
                    meta.raw = raw
                    meta_count += 1
                    break
        """ Report match """
        if not raw.report:
            report_basename = '{0}{1}'.format(raw_basename_nosuffix,\
             report_suffix)
            for report in report_pool.discovered:
                if report.basename == report_basename:
                    """ Match! """
                    report.raw = raw
                    raw.report = report
                    report_count += 1
                    break
        """ Compress match """
        if not raw.compress:
            compress_basename = '{0}{1}'.format(raw.basename,\
             compress_suffix)
            for compress in compress_pool.discovered:
                if compress.basename == compress_basename:
                    """ Match! """
                    compress.raw = raw
                    raw.compress = compress
                    compress_count += 1
                    break

        if raw.meta and raw.report:
            raw.meta.report = raw.report
            raw.report.meta = raw.meta

    output(tag, "-> raw: {0}".format(len(raw_pool.discovered)))
    output(tag, "--> assigned meta: {0}/{1}".\
     format(meta_count, len(meta_pool.discovered)))
    output(tag, "--> assigned reports: {0}/{1}".\
     format(report_count, len(report_pool.discovered)))
    output(tag, "--> assigned compress: {0}/{1}".\
     format(compress_count, len(compress_pool.discovered)))
##############################################################################
#
# interpool_missing_summary
#
##############################################################################
def interpool_missing_summary(meta_pool, report_pool, raw_pool, compress_pool):
    tag = gettag()

    """ This functions provide a summary of the files which do not have the
        corresponding metadata or report """
    from os.path import join
    from config import cfg
    from utils import magicdate_to_iso
    from osa.utils.iofile import writetofile

    output_prefix = cfg.get('OUTPUT', 'orphanprefix')
    output_suffix = cfg.get('OUTPUT', 'textsuffix')
    night = magicdate_to_iso(options.date)
    output_file = join(cfg.get(options.tel_id, 'logdir'), "{0}_{1}_{2}{3}".\
     format(output_prefix, night, options.tel_id, output_suffix))

    raw_zero = []
    raw_meta_orphans = []
    raw_report_orphans = []
    content = ''

    raw_pool_sorted = sorted(raw_pool.accepted, key=lambda file: file.mtime)
    for r in raw_pool_sorted:
        if r.size == 0:
            raw_zero.append(r)
        if r.meta == None:
            raw_meta_orphans.append(r)
        if r.report == None:
            raw_report_orphans.append(r)

    if len(raw_zero) != 0:
        output(tag, "Raw files with zero size: {0}".format(len(raw_zero)))
        for o in raw_zero:
            warn_text = "{1}, DAQ{2} crash/reboot, Zero sized file {0}".\
             format(o.basename, o.mtime.replace(microsecond=0),\
             options.tel_id.lstrip('M'))
            warning(tag, warn_text)
            content += warn_text + "\n"
    if len(raw_meta_orphans) != 0:
        output(tag, "Orphan raw files without .xml metadata: {0}".\
         format(len(raw_meta_orphans)))
        for o in raw_meta_orphans:
            warn_text = "{1}, DAQ{2} crash/reboot, Missing xml for {0}".\
             format(o.basename, o.mtime.replace(microsecond=0),\
             options.tel_id.lstrip('M'))
            warning(tag, warn_text)
            content += warn_text + "\n"
    if len(raw_report_orphans) != 0:
        output(tag, "Orphan raw files without .rep reports: {0}".\
                 format(len(raw_report_orphans)))
        for o in raw_report_orphans:
            warn_text = "{1}, SA++ crash/reboot, Missing rep for {0}".\
             format(o.basename, o.mtime.replace(microsecond=0))
            warning(tag, warn_text)
            content += warn_text + "\n"

    if content != '':
        writetofile(output_file, content)                       
##############################################################################
#
# interpool_process
#
##############################################################################
def interpool_process(meta_pool, report_pool, raw_pool, compress_pool):
    tag = gettag()

    """ A block is a list of File objects: meta, report, raw and compress """
    accepted_matrix = build_block(meta_pool.accepted, report_pool.accepted,\
     raw_pool.accepted)

    for block in accepted_matrix:
        block.process()
##############################################################################
#
# build_bock
#
##############################################################################
def build_block(meta_set, report_set, raw_set):
    tag = gettag()
    from os.path import join
    from config import cfg

    meta_type =  cfg.get('COPY', 'metafiletype')
    report_type =  cfg.get('COPY', 'reportfiletype')
    raw_type = cfg.get('COPY', 'rawfiletype')
    compress_type = cfg.get('COMPRESS', 'filetype')
    fileset_dict = {meta_type :meta_set, report_type: report_set,\
     raw_type: raw_set}

    """ Returns a list of Fileblocks = [Meta, Report, Raw, Compress] """
    lexical_set = set() 
    for s in fileset_dict.keys():
        for o in fileset_dict[s]:
            if o.run and o.subrun:
                lexical_set.add(o.run + o.subrun)

    verbose(tag, "Lexical order gives {0} blocks: {1}".\
     format(len(lexical_set), lexical_set))

    fileblock_set = set()
    for l in sorted(lexical_set):
        block = {}
        for s in fileset_dict.keys():
            type = s
            for o in fileset_dict[s]:
                type = o.type
                if (o.run and o.subrun) and (o.run + o.subrun) == l:
                    block[o.type] = o
                    verbose(tag, "Added to block: {0} {1}".\
                     format(o.type, o.basename))
                    break
            else:
                block[type] = None
                verbose(tag, "Added to block: {0} {1}".format(type, None))

        if block[raw_type]:
            compress_path = join(cfg.get(options.tel_id, 'rawoutputdir'),\
             options.date,\
             block[raw_type].basename + cfg.get('COMPRESS', 'suffix'))
            block[compress_type] = CompressFile(compress_path)
        else:
            block[compress_type] = None
        fileblock_set.add(FileBlock(block))

    """ Finally we return a time ordered block_set """
    matrix = sorted(fileblock_set, key=lambda b: b.mtime)

    return matrix
##############################################################################
#
# MAIN
#
##############################################################################
if __name__ == '__main__':
    tag = gettag()
    import sys
    import options
    from cliopts import rawcopycliparsing
    """ Set the options through cli parsing """
    args = rawcopycliparsing(sys.argv[0])
    """ Set lock as global """
    lock = get_lockname() 
    """ The four main sets of processed files """
    raw_processed = set()
    compress_processed = set()
    meta_processed = set()
    report_processed = set()
    # Run the routine
    raw_copy()
