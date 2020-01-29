from standardhandle import output, verbose, warning, error, gettag
import options, cliopts

__all__ = ["history"]
##############################################################################
#
# start
#
##############################################################################
def start(parent_tag):
    tag = gettag()
    from datetime import datetime
    now = datetime.utcnow()
    simple_parent_tag = parent_tag.rsplit('(')[0]
    header("Starting {0} at {1} UTC for LST, Telescope: {2}, Night: {3}".\
        format(simple_parent_tag, now.strftime("%Y-%m-%d %H:%M:%S"),\
        options.tel_id, options.date))
##############################################################################
#
# header
#
##############################################################################
def header(message):
    tag = gettag()
    framesize = size()
    if len(message) < framesize - 2:
        prettyframe = int((framesize - 2 - len(message))/2) * '='
    else:
        prettyframe = ''
    output(tag, "{0} {1} {0}".format(prettyframe, message))
##############################################################################
#
# rule
#
##############################################################################
def rule():
    tag = gettag()
    prettyframe = size() * '-'
    output(tag, prettyframe)
##############################################################################
#
# size
#
##############################################################################
def size():
    tag = gettag()
    import config
    framesize = int(config.cfg.get('OUTPUT', 'REPORTWIDTH'))
    return framesize
##############################################################################
#
# finished_text
#
##############################################################################
def finished_text(ana_dict):
    tag = gettag()

    import config

    content = "analysis.finished.timestamp={0}\n".format(ana_dict['END'])
    content += "analysis.finished.night={0}\n".format(ana_dict['NIGHT'])
    content += "analysis.finished.telescope={0}\n".format(ana_dict['TELESCOPE'])
    if options.tel_id == 'M1' or options.tel_id == 'M2':
        content += "analysis.finished.data.size={0} GB\n".format(ana_dict['RAW_GB'])
        content += "analysis.finished.data.files={0}\n".format(ana_dict['FILES_RAW'])
        content += "analysis.finished.data.files.scalibed={0}\n".format(ana_dict['FILES_SCALIB'])
        content += "analysis.finished.data.files.calibrated={0}\n".format(ana_dict['FILES_SORCERER'])
        content += "analysis.finished.data.files.ssignaled={0}\n".format(ana_dict['FILES_SSIGNAL'])
        content += "analysis.finished.data.files.merpped={0}\n".format(ana_dict['FILES_MERPP'])
        content += "analysis.finished.data.files.starred={0}\n".format(ana_dict['FILES_STAR'])
        content += "analysis.finished.data.files.starhistogramed={0}\n".format(ana_dict['FILES_STARHISTOGRAM'])
    elif options.tel_id == 'ST':
        content += "analysis.finished.data.files.superstarred={0}\n".format(ana_dict['FILES_SUPERSTAR'])
        content += "analysis.finished.data.files.superstarhistogramed={0}\n".format(ana_dict['FILES_SUPERSTARHISTOGRAM'])
        content += "analysis.finished.data.files.melibeaed={0}\n".format(ana_dict['FILES_MELIBEA'])
        content += "analysis.finished.data.files.melibeahistogramed={0}\n".format(ana_dict['FILES_MELIBEAHISTOGRAM'])
        #content += "analysis.finished.data.files.odieed={0}\n".format(ana_dict['FILES_ODIE'])

    if options.reason != None:
        content += "analysis.finished.data.comment={}.\n".format(ana_dict['COMMENTS'])
            
    output(tag, content)
    return content
##############################################################################
#
# finished_assignments
#
##############################################################################
def finished_assignments(sequence_list):
    tag = gettag()
    from glob import glob
    from os.path import join, getsize, basename
    from datetime import datetime
    from fnmatch import fnmatchcase
    from config import cfg
    from raw import getrawdir
    concept_set = []
    anadir = options.directory
    disk_space_GB = 0
    rawnum = 0
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        concept_set = ['CALIB', 'DL1', 'DL2']
        rawdir = getrawdir()
        if sequence_list != None:
            for s in sequence_list:
                rawnum += s.subruns
        data_files = glob(join(rawdir, '*{0}*{1}*'\
         .format(cfg.get('LSTOSA', 'DL1PATTERN'), cfg.get('LSTOSA', 'RAWSUFFIX'))))
        disk_space = 0
        for d in data_files:
            disk_space += getsize(d)
        disk_space_GB_f = float(disk_space)/(1000*1000*1000)
        disk_space_GB = int(round(disk_space_GB_f, 0))
    elif options.tel_id == 'ST':
        concept_set = ['DL2']
    

    ana_files = glob(join(anadir, '*' + cfg.get('LSTOSA', 'ROOTSUFFIX')))
    scalib_file_no = 0
    sorcerer_file_no = 0
    merpped_file_no = 0
    starred_file_no = 0
    ssignal_file_no = 0
    starhistogram_file_no = 0
    superstar_file_no = 0 
    superstarhistogram_file_no = 0
    melibea_file_no = 0
    melibeahistogram_file_no = 0
    odie_file_no = 0
    file_no = {}
    ana_set = set(ana_files)
    # verbose(tag, "Let's try to identify the root files in {0}".format(ana_set))
    for concept in concept_set:
        pattern = "{0}*".format(cfg.get('LSTOSA', concept + 'PREFIX'))
        if cfg.get('LSTOSA', concept + 'PATTERN'):
            pattern += "{0}*".format(cfg.get('LSTOSA', concept + 'PATTERN'))

        verbose(tag, "Trying with {0} and searching {1}".format(concept, pattern))
        file_no[concept] = 0
        delete_set = set()
        for a in ana_set:
            ana_file = basename(a)
            pattern_found = fnmatchcase(ana_file, pattern)
            #verbose(tag, "Was pattern {0} found in {1}?: {2}".format(pattern, ana_file, pattern_found))
            if pattern_found == True:
                verbose(tag, "Was pattern {0} found in {1}?: {2}".format(pattern, ana_file, pattern_found))
                file_no[concept] += 1
                delete_set.add(a)
        ana_set -= delete_set

    comment = None
    if options.reason != None:
        if options.reason == 'other':
            comment = "No data tonight: see Runbook"
        elif options.reason == 'moon':
            comment = "No data taking tonight: Moon night"
        elif options.reason == 'weather':
            comment = "No data taking tonight due to bad weather"

    now_string = "{0}".format(datetime.utcnow())

    dictionary = {'NIGHT':options.date, 'TELESCOPE':options.tel_id,\
     'IS_CLOSED':1, 'SEQUENCES':len(sequence_list), 'COMMENTS':comment,\
     'FILES_RAW':rawnum, 'RAW_GB':disk_space_GB, 'END':now_string}

    for concept in concept_set:
        dictionary['FILES_' + concept] = file_no[concept]

    return dictionary
##############################################################################
#
# history
#
##############################################################################
def history(run, program, inputfile, inputcard, rc, historyfile):
    """Appends a history line to the history file.

    A history line reports the outcome of the execution of a Mars executable.

    Parameters
    ----------
    run : str
        Run/sequence analyzed.
    program : str
        Mars executable used.
    inputfile : str
        If needed, some input file used for the Mars executable (e.g. a scalib
        file for sorcerer).
    inputcard : str
        Input card used for the Mars executable.
    rc : str or int
        Return code of the Mars executable.
    historyfile : str
        The history file that keeps track of the analysis steps.
    """
    tag = gettag()
    from datetime import datetime
    import iofile
    now = datetime.utcnow()
    datestring = now.strftime("%a %b %d %X UTC %Y") # Similar but not equal to %c (no timezone)
    stringtowrite = "{0} {1} {2} {3} {4} {5}\n".format(run, program, datestring, inputfile, inputcard, rc)
    iofile.appendtofile(historyfile, stringtowrite)
