from standardhandle import output, warning, verbose, error, gettag
import options, cliopts 
import config
##############################################################################
#
# register_files 
#
##############################################################################
def register_files(type, run_str, inputdir, prefix, pattern, suffix, outputdir):
    tag = gettag()
    from os.path import join, basename, exists, getmtime, getsize
    from filecmp import cmp
    from glob import glob
    from re import search
    from socket import gethostname
    from datetime import datetime
    from utils import get_md5sum_and_copy, magicdate_to_iso
    from mysql import select_db, update_or_insert_and_select_id_db
    from config import cfg

    file_list = glob(join(inputdir, "{0}*{1}*{2}*{3}".format(prefix, run_str, pattern, suffix)))
    verbose(tag, "File list is {0}".format(file_list))
    hostname = gethostname()
    # The default subrun index for most of the files
    run = int(run_str.lstrip('0'))
    subrun = 1
    for inputf in file_list:
        """ Next is the way of searching 3 digits after a dot in filenames """
        subrunsearch = search('(?<=\.)\d\d\d_', basename(inputf))
        if subrunsearch:
            """ And strip the zeroes after getting the first 3 character """
            subrun = subrunsearch.group(0)[0:3].lstrip('0')
        outputf = join(outputdir, basename(inputf))
        if exists(outputf) and cmp(inputf, outputf):
            # Do nothing than acknowledging
            verbose(tag, "Destination file {0} exists and it is identical to input".format(outputf))
        else:
            # There is no output file or it is different
            verbose(tag, "Copying file {0}".format(outputf))
            md5sum = get_md5sum_and_copy(inputf, outputf)
            verbose(tag, "Resulting md5sum={0}".format(md5sum))
            mtime = datetime.fromtimestamp(getmtime(outputf))
            size = getsize(outputf)

            # DB config parameters
            s = cfg.get('MYSQL', 'SERVER')
            u = cfg.get('MYSQL', 'USER')
            d = cfg.get('MYSQL', 'DATABASE')
            daqtable = cfg.get('MYSQL', 'DAQTABLE')
            storagetable = cfg.get('MYSQL', 'STORAGETABLE')
            sequencetable = cfg.get('MYSQL', 'SEQUENCETABLE')

            # Get the other values of parent files querying the database
            selections = ['ID', 'DAQ_ID', 'REPORT_ID']
            conditions = {'DAQ_ID': "(SELECT ID FROM {0} WHERE\
             RUN={1} AND SUBRUN={2} AND TELESCOPE='{3}')"\
             .format(daqtable, run, subrun, options.tel_id)}

            id_parent, daq_id, report_id = [None, None, None]
            querymatrix = select_db(s, u, d, storagetable, selections,\
                         conditions)
            # It could be that they don't exists
            if len(querymatrix) != 0:
                verbose(tag, "Resulting query={0}".format(querymatrix[0]))
                id_parent, daq_id, report_id = querymatrix[0]
                verbose(tag, "id_parent={0}, daq_id={1}, report_id={2}".\
                 format(id_parent, daq_id, report_id))

            # Get also de sequence id
            selections = ['ID']
            conditions = {'TELESCOPE': options.tel_id, 'RUN': run}
            querymatrix = select_db(s, u, d, sequencetable, selections,\
             conditions)
            sequence_id = None
            if len(querymatrix) != 0:
                sequence_id, = querymatrix[0]
                verbose(tag, "sequence_id={0}".format(sequence_id))
            # Finally we update or insert the file
            night = magicdate_to_iso(options.date)
            assignments = {'ID_PARENT': id_parent, 'DAQ_ID': daq_id,\
             'REPORT_ID': report_id, 'SEQUENCE_ID':sequence_id,\
             'NIGHT': night, 'TELESCOPE': options.tel_id,\
             'HOSTNAME': hostname, 'FILE_TYPE': type, 'SIZE': size,\
             'M_TIME': mtime, 'MD5SUM': md5sum}
            conditions = {'FILE_PATH': outputf}
            update_or_insert_and_select_id_db(s, u, d, storagetable,\
             assignments, conditions)
##############################################################################
#
# register_run_concept_files()
#
##############################################################################
def register_run_concept_files(run_string, concept):
    tag = gettag()
    from os.path import join
    from config import cfg
    from utils import magicdate_to_dir
    inputdir = options.directory
    middledir = magicdate_to_dir(options.date)
    outputdir = join(cfg.get(options.tel_id, concept + 'DIR'), middledir)
    type = cfg.get('OSA', concept + 'TYPE')
    prefix = cfg.get('OSA', concept + 'PREFIX')
    pattern = cfg.get('OSA', concept + 'PATTERN')
    suffix = cfg.get('OSA', concept + 'SUFFIX')
    verbose(tag, "Registering {0} file for {2}*{1}*{3}*{4}"\
     .format(type, run_string, prefix, pattern, suffix ))
    register_files(type, run_string, inputdir, prefix, pattern, suffix, outputdir)
