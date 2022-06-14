"""Definition of classes containing run and sequence information"""

from dataclasses import dataclass


@dataclass
class RunObj:
    run_str: str = None
    run: int = None
    type: str = None
    subruns: int = None
    source_name: str = None
    source_ra: float = None
    source_dec: float = None
    telescope: str = "LST1"
    night: str = None


class Sequence(RunObj):
    def __init__(self):
        super(Sequence, self).__init__()
        self.seq = None
        self.pedcal_run = None
        self.drs4_run = None
        self.parent = None
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
        self.calibstatus = None
        self.seq = 1
        self.parent = None
        self.drs4_run = None


class SequenceData(Sequence):
    def __init__(self, r):
        super(SequenceData, self).__init__()
        super(SequenceData, self).associate(r)
        self.parent = 1
        self.time_calibration_run = None
        self.drive = None
        self.drs4_file = None
        self.calibration_file = None
        self.time_calibration_file = None
        self.systematic_correction_file = None
        self.dl1status = None
        self.dl1abstatus = None
        self.muonstatus = None
        self.datacheckstatus = None
        self.dl2status = None
        self.dl3status = None
