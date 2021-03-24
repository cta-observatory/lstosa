import logging
import os
import shutil
from filecmp import cmp
from glob import glob
from os.path import basename, exists, join

from osa.configs import options
from osa.configs.config import cfg
from osa.utils.utils import lstdate_to_dir
from pathlib import Path

log = logging.getLogger(__name__)


def register_files(type, run_str, inputdir, prefix, suffix, outputdir):
    """Copy files into final data directory destination and register them into the DB (to be implemented).

    Parameters
    ----------
    run_str:
    inputdir: analysis directory
    suffix: suffix of the data file
    outputdir: final data directory
    prefix: prefix of the data file
    type : type of run for DB purposes
    """
    file_list = Path(inputdir).rglob(f"{prefix}*{run_str}*{suffix}")
    log.debug(f"File list is {file_list}")
    # hostname = gethostname()
    # the default subrun index for most of the files
    # run = int(run_str.lstrip("0"))
    # subrun = 1
    for inputf in file_list:
        # next is the way of searching 3 digits after a dot in filenames
        # subrunsearch = re.search("(?<=\.)\d\d\d_", basename(inputf))
        # if subrunsearch:
        # and strip the zeroes after getting the first 3 character
        # subrun = subrunsearch.group(0)[0:3].lstrip("0")
        outputf = join(outputdir, basename(inputf))
        if exists(outputf) and cmp(inputf, outputf):
            # do nothing than acknowledging
            log.debug(f"Destination file {outputf} exists and it is identical to input")
        else:
            # there is no output file or it is different
            log.debug(f"Moving file {outputf}")
            shutil.move(inputf, outputf)
            if prefix == "dl1_LST-1" and suffix == ".h5":
                log.debug(f"Keeping DL1 symlink in {inputf}")
                os.symlink(outputf, inputf)
            if prefix == "muons_LST-1" and suffix == ".fits":
                log.debug(f"Keeping muons file symlink in {inputf}")
                os.symlink(outputf, inputf)
            # for the moment we are not interested in calculating the hash md5
            # md5sum = get_md5sum_and_copy(inputf, outputf)
            # log.debug("Resulting md5sum={0}".format(md5sum))
            # mtime = datetime.fromtimestamp(getmtime(outputf))
            # size = getsize(outputf)

            # # DB config parameters
            # s = cfg.get('MYSQL', 'SERVER')
            # u = cfg.get('MYSQL', 'USER')
            # d = cfg.get('MYSQL', 'DATABASE')
            # daqtable = cfg.get('MYSQL', 'DAQTABLE')
            # storagetable = cfg.get('MYSQL', 'STORAGETABLE')
            # sequencetable = cfg.get('MYSQL', 'SEQUENCETABLE')

            # Get the other values of parent files querying the database
            # selections = ['ID', 'DAQ_ID', 'REPORT_ID']
            # conditions = {'DAQ_ID': f"(SELECT ID FROM {daqtable} WHERE\
            #  RUN={run} AND SUBRUN={subrun} AND TELESCOPE='{options.tel_id}')"}

            # id_parent, daq_id, report_id = [None, None, None]
            # querymatrix = select_db(s, u, d, storagetable, selections, conditions)
            # It could be that they don't exists
            # if len(querymatrix) != 0:
            #     log.debug("Resulting query={0}".format(querymatrix[0]))
            #     id_parent, daq_id, report_id = querymatrix[0]
            #     log.debug(f"id_parent={id_parent}, daq_id={daq_id}, report_id={report_id}")

            # Get also de sequence id
            # selections = ['ID']
            # conditions = {'TELESCOPE': options.tel_id, 'RUN': run}
            # querymatrix = select_db(s, u, d, sequencetable, selections, conditions)
            # sequence_id = None
            # if len(querymatrix) != 0:
            #     sequence_id, = querymatrix[0]
            #     log.debug("sequence_id={0}".format(sequence_id))
            # Finally we update or insert the file
            # night = lstdate_to_iso(options.date)
            # assignments = {'ID_PARENT': id_parent, 'DAQ_ID': daq_id,
            #                'REPORT_ID': report_id, 'SEQUENCE_ID': sequence_id,
            #                'NIGHT': night, 'TELESCOPE': options.tel_id,
            #                'HOSTNAME': hostname, 'FILE_TYPE': type, 'SIZE': size,
            #                'M_TIME': mtime, 'MD5SUM': md5sum}
            # conditions = {'FILE_PATH': outputf}
            # update_or_insert_and_select_id_db(s, u, d, storagetable,
            #                                   assignments, conditions)


def register_run_concept_files(run_string, concept):
    """Prepare files to be moved to final destination directories

    Parameters
    ----------
    run_string
    concept
    """

    nightdir = lstdate_to_dir(options.date)
    if concept == "DL2":
        inputdir = options.directory
        outputdir = join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.dl2_prod_id
        )
    elif concept in ["DL1", "MUON"]:
        inputdir = options.directory
        outputdir = join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id
        )
    elif concept == "DL1AB":
        inputdir = join(options.directory, "dl1ab" + "_" + options.dl1_prod_id)
        outputdir = join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id, options.dl1_prod_id
        )
    elif concept == "DATACHECK":
        inputdir = join(options.directory, "datacheck" + "_" + options.dl1_prod_id)
        outputdir = join(
            cfg.get(options.tel_id, concept + "DIR"), nightdir, options.prod_id, options.dl1_prod_id
        )
    elif concept in ["PEDESTAL", "CALIB", "TIMECALIB"]:
        inputdir = options.directory
        outputdir = join(
                cfg.get(options.tel_id, concept + "DIR"), nightdir, options.calib_prod_id
        )
    type = cfg.get("LSTOSA", concept + "TYPE")
    prefix = cfg.get("LSTOSA", concept + "PREFIX")
    suffix = cfg.get("LSTOSA", concept + "SUFFIX")
    log.debug(f"Registering {type} file for {prefix}*{run_string}*{suffix}")
    register_files(type, run_string, inputdir, prefix, suffix, outputdir)
