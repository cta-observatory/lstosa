from osa.utils.standardhandle import output, verbose, warning, error, stringify, gettag
from osa.utils import options, cliopts

__all__ = ["extractsubruns", "extractruns", "extractsequences",
"extractsequencesstereo", "generateworkflow", "dependsonpreviousseq",
"hastemperaturechanged"]
##############################################################################
#
# extractsubruns
#
##############################################################################
def extractsubruns(nightsummary):
    tag = gettag()
    from datetime import datetime
    from datamodel import SubrunObj, RunObj
    from osa.utils.utils import lstdate_to_iso
    subrun_list = []
    run_to_obj = {}
    if nightsummary:
        for i in nightsummary.splitlines():
            word = i.split()
            current_run_str = word[0]
            current_run = int(current_run_str.lstrip('0'))
            sr = SubrunObj()
            sr.subrun_str = word[1]
            sr.subrun = int(sr.subrun_str.lstrip('0'))
            sr.kind = str(word[12])
            sr.date = str(word[3])
            sr.time = str(word[4])
            sr.timestamp = datetime.strptime(str(word[3] + ' ' + word[4]), "%Y-%m-%d %H:%M:%S")
            try:
                sr.runobj = run_to_obj[current_run]
            except:
                """ Check if this is the first subrun. If not, print a warning """
                if (sr.subrun != 1):
                    warning(tag, "Missing {0}.001 subrun".format(current_run))

                """ Build run object """
                sr.runobj = RunObj()
                sr.runobj.run_str = current_run_str
                sr.runobj.run = current_run
                sr.runobj.type = str(word[2])
                sr.runobj.sourcewobble = str(word[5])
                sr.runobj.source, sep, sr.runobj.wobble = sr.runobj.sourcewobble.partition('-W')
                if sr.runobj.wobble == '':
                    sr.runobj.wobble = None
                sr.runobj.telescope = options.tel_id
                sr.runobj.night = lstdate_to_iso(options.date)

                """ Include extra information that might be used in the future """
                sr.runobj.number_events = word[6]
                sr.runobj.zd_deg = word[7]
                sr.runobj.source_ra = word[8]
                sr.runobj.source_dec = word[9]
                sr.runobj.l2_table = word[10]
                sr.runobj.test_run = word[11]
                sr.runobj.hv_setting = word[12]
                sr.runobj.moonfilter = word[13]

                run_to_obj[sr.runobj.run] = sr.runobj
            else:
               """ Check if we have a GRB, then update the source name """
               if "GRB" not in sr.runobj.source and "GRB" in str(word[5]):
                   sr.runobj.sourcewobble = str(word[5])
                   sr.runobj.source, sep, sr.runobj.wobble = sr.runobj.sourcewobble.partition('-W')
                   if sr.runobj.wobble == '':
                       sr.runobj.wobble = None
                   
                   subrun_list[-1] = sr
                   run_to_obj[sr.runobj.run] = sr.runobj
                   sr.runobj = sr.runobj
            
            sr.runobj.subrun_list.append(sr)
            sr.runobj.subruns = len(sr.runobj.subrun_list)
            subrun_list.append(sr)
        verbose(tag, "Subrun list extracted")
    return subrun_list
##############################################################################
#
# extractruns
#
##############################################################################
def extractruns(subrun_list):
    tag = gettag()

    """ After having extracted properly the subruns, the runobj is there """
    run_list = []
    for s in subrun_list:
        if s.runobj not in run_list:
            run_list.append(s.runobj)

    verbose(tag, "Run list extracted")
    return run_list
#############################################################################a
#
# extractsequences
#
##############################################################################
def extractsequences(run_list):
    # This function depends on the selected mode (P, S, T)
    # It searches for sequences composed out of
    # a) Pedestal->Calibration->Data turns into independent runs
    # b) Data[->Pedestal]->Data turns into dependent runs
    # c) Otherwise orphan runs which are dismissed
    tag = gettag()
    sequence_list = []  # This is the list of sequence objects to return
    head = []   # This is a set with maximum 3 tuples consisting of [run, type, require]
    store = []  # This is a set with runs which constitute every valid data sequence
    require = dict()   # This is a dictionary with runs as keys and required runs as values
    
    # Create a list of sources. For each, we should have at least a PED, CAL and some DATA
    # If not, then we use the previous PED and CAL. Try to sort this list so that the PED
    # and CAL are in the beginning. 
    sources = []
    run_list_sorted  = []
    pending = []

    for r in run_list:
        # Extract the basic info.
        currentsrc  = r.source
        currentrun  = r.run
        currenttype = r.type
        
        # Skip runs not belonging to this telescope ID. 
        #if (r.telescope!=options.tel_id): continue

        if currentsrc not in sources:
            #verbose(tag, "New source %s detected, waiting for PED and CAL" %currentsrc)
            hasped = False
            hascal = False
            sources.append(currentsrc)
        
        if currenttype == 'PEDESTAL':
            verbose(tag, "Detected a new PED run %s for %s" %(currentrun, currentsrc))
            hasped = True
            run_list_sorted.append(r)
        elif currenttype == 'CALIBRATION':
            verbose(tag, "Detected a new CAL run %s for %s" %(currentrun, currentsrc))
            hascal = True
            run_list_sorted.append(r)
        
        if (hasped == False or hascal == False):
            if currenttype == 'DATA':
                verbose(tag, "Detected a new DATA run %s for %s, but still no PED/CAL" %(currentrun, currentsrc))
                pending.append(r)
        else:
            if currenttype == 'DATA':
                # Normal case, we have the PED, the SUB, then append the DATA 
                verbose(tag, "Detected a new DATA run %s for %s" %(currentrun, currentsrc))
                run_list_sorted.append(r)
            elif (currenttype == 'CALIBRATION' and pending!=[]):
                # We just took the CAL, and we had the PED, so we can add the pending runs.
                verbose(tag, "PED/CAL are now available, adding the runs in the pending queue")
                for pr in pending:
                    run_list_sorted.append(pr)
                pending = []
            
    if (pending!=[]):
        # We reached the end, we can add the pending runs.
        verbose(tag, "Adding the pending runs")
        for pr in pending:
            run_list_sorted.append(pr)

    for i in run_list_sorted:
        currentrun = i.run
        currenttype = i.type
        
        if len(head) == 0:
            if currenttype == 'PEDESTAL':
                # Normal case 
                verbose(tag, "appending [{0}, {1}, {2}]".format(currentrun, currenttype, None))
                head.append([currentrun, currenttype, None])
        elif len(head) == 1:
            previousrun = head[0][0]
            previoustype =  head[0][1]
            previousreq = head[0][2]
            whichreq = None
            if currentrun == previousrun:
                # It shouldn't happen, same run number, just skip to next run
                continue
            if currenttype == 'PEDESTAL':
                if previoustype == 'DATA':
                    # replace the first head element, keeping its previous run or requirement run, depending on mode
                    if dependsonpreviousseq(previousrun, currentrun):
                        whichreq = previousrun
                    else:
                        whichreq = previousreq
                elif previoustype == 'PEDESTAL':
                    # One pedestal after another, keep replacing
                    whichreq = None
                verbose(tag, "replacing [{0}, {1}, {2}]".format(currentrun, currenttype, whichreq))
                head[0] = [currentrun, currenttype, whichreq]
            elif currenttype == 'CALIBRATION' and previoustype == 'PEDESTAL':
                # add it too
                verbose(tag, "appending [{0}, {1}, {2}]".format(currentrun, currenttype, None))
                head.append([currentrun, currenttype, None])
                require[currentrun] = previousrun
            elif currenttype == 'DATA':
                if previoustype == 'PEDESTAL':
                    #   it is the pedestal->data mistake from shifters; 
                    #   replace and store if they are not the first of observations
                    #   required run requirement inherited from pedestal run
                    if previousreq != None:
                        verbose(tag, "P->C, replacing [{0}, {1}, {2}]".format(currentrun, currenttype, previousreq))
                        head[0] = [currentrun, currenttype, previousreq]
                        store.append(currentrun)
                        require[currentrun] = previousreq
                elif previoustype == 'DATA':
                    #   it is the data->data case, replace and store
                    #   the whole policy has to be applied here:
                    #   if P=parallel, the dependence is previousreq
                    #   if S=sequential, the dependence is previousrun
                    #   if T=temperature-aware, the dependence has to be evaluated by a function
                    if dependsonpreviousseq(previousrun, currentrun):
                        whichreq = previousrun
                    else:
                        whichreq = previousreq
                    verbose(tag, "D->D, replacing [{0}, {1}, {2}]".format(currentrun, currenttype, whichreq))
                    head[0] = [currentrun, currenttype, whichreq]
                    store.append(currentrun)
                    require[currentrun] = whichreq
        elif len(head) == 2:
            previoustype =  head[1][1]
            if currenttype == 'DATA' and previoustype == 'CALIBRATION':
                #   it is the pedestal->calibration->data case, append, store, resize and replace
                previousrun = head[1][0]
                head.pop()
                verbose(tag, "P->C->D, appending [{0}, {1}, {2}]".format(currentrun, currenttype, previousrun))
                head[0] = [currentrun, currenttype, previousrun]
                store.append(currentrun)
                # This is different from currentrun since it marks parent sequence run
                require[currentrun] = previousrun  
            elif currenttype == 'PEDESTAL' and previoustype == 'CALIBRATION':
                # There was a problem with the previous calibration and shifters decide to give another try
                head.pop()
                verbose(tag, "P->C->P, deleting and replacing [{0}, {1}, {2}]".format(currentrun, currenttype, None))
                head[0] = [currentrun, currenttype, None]

    sequence_list = generateworkflow(run_list_sorted, store, require)
    # Ready to return the list of sequences
    verbose(tag, "Sequence list extracted")
    
    return sequence_list
##############################################################################
#
# extractsequencestereo
#
##############################################################################
def extractsequencesstereo(s1_list, s2_list):
    tag = gettag()
    from datamodel import SequenceStereo
    from job import setsequencefilenames
    ss_list = []
    for s1 in s1_list:
        ss = None
        if s1.type == 'DATA':
            for s2 in s2_list:
                if s2.type == 'DATA':
                    if s2.run == s1.run:
                        ss = SequenceStereo(s1, s2)
                        ss.seq = len(ss_list)
                        ss.jobname = "{0}_{1}".format(ss.telescope, str(ss.run).zfill(5))
                        setsequencefilenames(ss)
                        ss_list.append(ss)
                        break
    verbose(tag, "Appended {0} stereo sequences".format(len(ss_list)))
    return ss_list
##############################################################################
#
# generateworkflow
#
##############################################################################
def generateworkflow(run_list, store, require):
    tag = gettag()
    from datamodel import SequenceCalibration, SequenceData
    from ..jobs import job
    # we have a store set with correct data sequences to give seq numbers and parent dependencies
    sequence_list = []
    verbose(tag, "The storage contains {0} data sequences".format(len(store)))
    index = None
    seq = None
    parent = None
    for r in run_list:
        # The next seq value to assign (if this happens)
        seq = len(sequence_list)
        # verbose(tag, "trying to assing run {0}, type {1} to sequence {2}".format(r.run, r.type, seq))
        if r.type == 'DATA':
            try:
                store.index(r.run)
            except ValueError as e:
                # There is nothing really wrong with that, just a DATA run without sequence
                warning(tag, "There is no sequence for data run {0}".format(r.run))
            else:
                previousrun = require[r.run]
                for s in sequence_list:
                    if s.run == previousrun:
                        parent = s.seq
                        break

                verbose(tag, "Sequence {0} assigned to run {1} whose parent is {2} with run {3}".format(\
                        seq, r.run, parent, previousrun))
                s = SequenceData(r)
                s.seq = seq
                s.parent = parent
                for p in sequence_list:
                    if p.seq == parent:
                        s.parent_list.append(p)
                        break
                
                s.previousrun = previousrun
                s.jobname = "{0}_{1}".format(r.telescope, str(r.run).zfill(5))
                job.setsequencefilenames(s)
                if s not in sequence_list: sequence_list.append(s)
        elif r.type == 'CALIBRATION':
            # Calibration sequence are appended to the sequence list if they are parent from data sequences
            for k in iter(require):
                if r.run == require[k]:
                    previousrun = require[r.run]

                    # We found that this calibration is required
                    s = SequenceCalibration(r)
                    s.seq = seq
                    s.parent = None
                    s.previousrun = previousrun
                    s.jobname = "{0}_{1}".format(r.telescope, str(r.run).zfill(5))
                    job.setsequencefilenames(s)
                    verbose(tag, "Sequence {0} assigned to run {1} whose parent is {2} with run {3}".format(\
                            s.seq, r.run, s.parent, s.previousrun))
                    if s not in sequence_list: sequence_list.append(s)
                    break
        
    # Insert the calibration file names
    job.setsequencecalibfilenames(sequence_list)
    verbose(tag, "Workflow completed")
    
    return sequence_list
##############################################################################
#
# dependsonpreviousseq
#
##############################################################################
def dependsonpreviousseq(previous, current):
    tag = gettag()
    if options.mode == 'P':
        return False
    elif options.mode == 'S':
        return True
    elif options.mode == 'T':
        if hastemperaturechanged(previous, current):
            return True
        else:
            return False
    elif options.mode == None:
        # Not needed, let us assume easy parallel mode
        return False
    else:
        error(tag, "mode {0} not recognized".format(options.mode), 2)

##############################################################################
#
# hastemperaturechanged
#
##############################################################################
def hastemperaturechanged(previous, current):
    tag = gettag()
    # TODO: check if the temperature difference between runs is over threshold
    return False
