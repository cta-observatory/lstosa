from osa.utils import options
from osa.utils.standardhandle import output, verbose, gettag

__all__ = ["history", "start", "rule", "finished_assignments", "finished_text"]


def start(parent_tag):
    tag = gettag()
    from datetime import datetime
    now = datetime.utcnow()
    simple_parent_tag = parent_tag.rsplit('(')[0]
    header(
        f"Starting {simple_parent_tag} at {now.strftime('%Y-%m-%d %H:%M:%S')} "
        f"UTC for LST, Telescope: {options.tel_id}, Night: {options.date}"
    )


def header(message):
    tag = gettag()
    framesize = size()
    if len(message) < framesize - 2:
        prettyframe = int((framesize - 2 - len(message)) / 2) * '='
    else:
        prettyframe = ''
    output(tag, f"{prettyframe} {message} {prettyframe}")


def rule():
    tag = gettag()
    prettyframe = size() * '-'
    output(tag, prettyframe)


def size():
    tag = gettag()
    from osa.configs import config
    framesize = int(config.cfg.get('OUTPUT', 'REPORTWIDTH'))
    return framesize


def finished_text(ana_dict):
    tag = gettag()

    content = f"analysis.finished.timestamp={ana_dict['END']}\n"
    content += f"analysis.finished.night={ana_dict['NIGHT']}\n"
    content += f"analysis.finished.telescope={ana_dict['TELESCOPE']}\n"

    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        content += f"analysis.finished.data.size={ana_dict['RAW_GB']} GB\n"
        content += f"analysis.finished.data.files.r0={ana_dict['FILES_RAW']}\n"
        # FIXME: Add pedestal and calibration info
        # content += "analysis.finished.data.files.pedestal={0}\n".format(ana_dict['FILES_PED'])
        # content += "analysis.finished.data.files.calib={0}\n".format(ana_dict['FILES_CALIB'])
        # content += "analysis.finished.data.files.time_calib={0}\n".format(ana_dict['FILES_TIMECALIB'])
        content += f"analysis.finished.data.files.dl1={ana_dict['FILES_DL1']}\n"
        content += f"analysis.finished.data.files.dl2={ana_dict['FILES_DL2']}\n"
        content += f"analysis.finished.data.files.muons={ana_dict['FILES_MUON']}\n"
        content += f"analysis.finished.data.files.datacheck={ana_dict['FILES_DATACHECK']}\n"

    if options.reason is not None:
        content += f"analysis.finished.data.comment={ana_dict['COMMENTS']}.\n"

    output(tag, content)
    return content


def finished_assignments(sequence_list):
    tag = gettag()
    from glob import glob
    from os.path import join, getsize, basename
    from datetime import datetime
    from fnmatch import fnmatchcase
    from osa.configs.config import cfg
    from osa.rawcopy.raw import getrawdir
    concept_set = []
    anadir = options.directory
    disk_space_GB = 0
    rawnum = 0
    if options.tel_id == 'LST1' or options.tel_id == 'LST2':
        # FIXME: add all files 'PED', 'CALIB'?
        concept_set = ['DL1', 'DL2', 'MUON', 'DATACHECK']
        rawdir = getrawdir()
        if sequence_list is not None:
            for s in sequence_list:
                rawnum += s.subruns
        data_files = glob(
            join(rawdir, f'*{cfg.get("LSTOSA", "R0PREFIX")}*{cfg.get("LSTOSA", "R0SUFFIX")}*')
        )
        disk_space = 0
        for d in data_files:
            disk_space += getsize(d)
        disk_space_GB_f = float(disk_space) / (1000 * 1000 * 1000)
        disk_space_GB = int(round(disk_space_GB_f, 0))
    elif options.tel_id == 'ST':
        concept_set = ['DL2']

    ana_files = glob(join(anadir, '*' + cfg.get('LSTOSA', 'R0SUFFIX')))
    file_no = {}
    ana_set = set(ana_files)

    for concept in concept_set:
        pattern = f"{cfg.get('LSTOSA', concept + 'PREFIX')}*"
        verbose(tag, f"Trying with {concept} and searching {pattern}")
        file_no[concept] = 0
        delete_set = set()
        for a in ana_set:
            ana_file = basename(a)
            pattern_found = fnmatchcase(ana_file, pattern)
            # verbose(tag, "Was pattern {0} found in {1}?: {2}".format(pattern, ana_file, pattern_found))
            if pattern_found:
                verbose(tag, f"Was pattern {pattern} found in {ana_file}?: {pattern_found}")
                file_no[concept] += 1
                delete_set.add(a)
        ana_set -= delete_set

    comment = None
    if options.reason is not None:
        if options.reason == 'other':
            comment = "No data tonight: see Runbook"
        elif options.reason == 'moon':
            comment = "No data taking tonight: Moon night"
        elif options.reason == 'weather':
            comment = "No data taking tonight due to bad weather"

    now_string = f"{datetime.utcnow()}"

    dictionary = {
        'NIGHT': options.date,
        'TELESCOPE': options.tel_id,
        'IS_CLOSED': 1,
        'SEQUENCES': len(sequence_list),
        'COMMENTS': comment,
        'FILES_RAW': rawnum,
        'RAW_GB': disk_space_GB,
        'END': now_string
    }

    for concept in concept_set:
        dictionary['FILES_' + concept] = file_no[concept]

    return dictionary


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
    datestring = now.strftime("%a %b %d %X UTC %Y")  # Similar but not equal to %c (no timezone)
    stringtowrite = f"{run} {program} {datestring} {inputfile} {inputcard} {rc}\n"
    iofile.appendtofile(historyfile, stringtowrite)
