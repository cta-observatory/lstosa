class Telescope(object):
    def __init__(self):
        super(Telescope, self).__init__()
        self.telescope = None


class Source(Telescope):
    def __init__(self):
        super(Source, self).__init__()
        self.source_name = None
        self.source_ra = None
        self.source_dec = None


class RunObj(Source):
    def __init__(self):
        super(RunObj, self).__init__()
        self.id = None
        self.type = None
        self.n_subruns = None


class SubrunObj(RunObj):
    def __init__(self):
        super(SubrunObj, self).__init__()
        self.runobj = None
        self.subrun = None
        self.date = None


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
        self.jobname = None
        self.jobid = None
        self.action = None
        self.tries = None
        self.state = None
        self.cputime = None
        self.exit = None

    def associate(self, r):
        for a in r.__dict__.keys():
            self.__dict__.update({a: r.__dict__[a]})


class SequenceCalibration(Sequence):
    def __init__(self, r):
        super(SequenceCalibration, self).__init__()
        super(SequenceCalibration, self).associate(r)
        self.calibstatus = None


class SequenceData(Sequence):
    def __init__(self, r):
        super(SequenceData, self).__init__()
        super(SequenceData, self).associate(r)
        self.calibration = None
        self.pedestal = None
        self.drive = None
        self.time_calibration = None
        self.systematic_correction = None
        self.dl1status = None
        self.dl1abstatus = None
        self.muonstatus = None
        self.datacheckstatus = None
        self.dl2status = None
        self.dl3status = None
