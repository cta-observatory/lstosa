class Period(object):
    def __init__(self):
        self.period = None

class Night(Period):
    def __init__(self):
        super(Night, self).__init__()
        self.night = None

class Telescope(Night):
    def __init__(self):
        super(Telescope, self).__init__()
        self.telescope = None

class Source(Telescope):
    def __init__(self):
        super(Source, self).__init__()    
        self.source = None

class Wobble(Source):
    def __init__(self):
        super(Wobble, self).__init__()
        self.sourcewobble = None
        self.wobble = None

class RunObj(Wobble):
    def __init__(self):
        super(RunObj, self).__init__()
        self.run_str = None
        self.run = None
        self.type = None
        self.subrun_list = []
        self.subruns = 0 

class SubrunObj(RunObj):
    def __init__(self):
        self.runobj = None 
        self.subrun_str = None
        self.subrun = None
        self.kind = None
        self.timestamp = None
        self.time = None
        self.date = None
        self.ucts_t0_dragon = None
        self.dragon_counter0 = None
        self.ucts_t0_tib = None
        self.tib_counter0 = None

class Sequence(RunObj):
    def __init__(self):
        super(Sequence, self).__init__()
        self.seq = None
        self.parent_list = []
        self.parent = None
        self.parentjobid = None
        self.previousrun = None
        self.script = None
        self.veto = None
        self.closed = None
        self.history = None
        self.queue = None
        self.jobname = None
        self.jobid = None
        self.action = None
        self.tries = None
        self.state = None
        self.jobhost = None
        self.cputime = None
        self.walltime = None
        self.exit = None

    def associate(self, r):
        for a in r.__dict__.keys():
            self.__dict__.update({a: r.__dict__[a]})

class SequenceCalibration(Sequence):
    def __init__(self, r):
        super(SequenceCalibration, self).__init__()
        super(SequenceCalibration, self).associate(r)        
        self.scalibstatus = None

class SequenceData(Sequence):
    def __init__(self, r):
        super(SequenceData, self).__init__()
        super(SequenceData, self).associate(r)
        self.calibration = None
        self.pedestal = None
        self.drive = None
        self.ssignalstatus = None
        self.sorcererstatus = None
        self.merppstatus = None
        self.starstatus = None
        self.starhistogramstatus = None

class SequenceStereo(Sequence):
    def __init__(self, v, w):
        super(SequenceStereo, self).__init__()
        attr_list = ['run', 'subrun_list' , 'subruns',\
         'wobble', 'sourcewobble', 'source', 'night']
        for a in attr_list:
            """ This copies the unique attrs of both sequences """
            self.__dict__.update({a: self.\
             set_unique(v.__dict__[a], w.__dict__[a])})
        self.type = 'STEREO'
        self.telescope = 'ST'
        self.subruns = v.subruns + w.subruns
        self.parent_list = [v, w]
        self.parent = "{0},{1}".format(v.seq, w.seq)
        self.parentjobid = "{0}:{1}".format(v.jobid, w.jobid)
        self.superstarstatus = None
        self.superstarhistogramstatus = None
        self.melibeastatus = None
        self.melibeahistogramstatus = None
        self.odiestatus = None
    def set_unique(self, v_attr, w_attr):
        if v_attr == w_attr:
            return v_attr
        else:
            return None
