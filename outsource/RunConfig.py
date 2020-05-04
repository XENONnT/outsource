import os
import re
import time
from .Config import config, base_dir, work_dir
from utilix import db



class RunConfigBase:
    """Base class that sets the defaults"""

    _update_run_db = False
    _force_rerun = False
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _executable = os.path.join(base_dir, 'workflow', 'run-pax.sh')
    _workdir = work_dir
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _pax_version = config.get('Outsource', 'pax_version')


class RunConfig(RunConfigBase):
    """Object that gets passed to outsource for each run/workflow.

    This base class has essentially the same info as a dictionary passed as input"""

    required_attributes = ['input_location', 'output_location']

    def __init__(self, **kwargs):
        
        # default job priority
        self._priority = 50
        
        for key, val in kwargs.items():
            setattr(self, "_" + key, val)

        throw_error = False
        for key in self.required_attributes:
            if not hasattr(self, "_" + key):
                print("{name} is required in the input dictionary".format(name=key))
                throw_error = True
        if throw_error:
            raise AttributeError()

    @property
    def workflow_id(self):
        return self._workflow_id

    @property
    def input_location(self):
        return self._input_location

    @property
    def output_location(self):
        return self._output_location

    @property
    def pax_version(self):
        return self._pax_version

    @property
    def update_run_db(self):
        return self._update_run_db

    @property
    def force_rerun(self):
        return self._force_rerun

    @property
    def x509_proxy(self):
        return self._x509_proxy

    @property
    def executable(self):
        return self._executable

    @property
    def workdir(self):
        return self._workdir

    @property
    def priority(self):
        return self._priority


class DBConfig(RunConfig):
    """Uses runDB to build _dbcfgs info"""
    # default detector is tpc
    _detector = 'tpc'

    def __init__(self, number, **kwargs):
        self._number = number
        self._run_doc = db.get_doc(self.number)

        # eventually get the input location, etc here using DB
        rawdir = os.path.join('/xenon/xenon1t/raw', str(self.number))
        output = os.path.join('/xenon/xenonnt_test/processed', str(self.number))

        super().__init__(input_location=rawdir, output_location=output, **kwargs)


    @property
    def workflow_id(self):
        if self.detector == 'tpc':
            idstring = "{:06d}".format(self.number)
        elif self.detector == 'muon_veto':
            idstring = self.number
        else:
            raise NotImplementedError
        return "xe1t_{detector}_{id}".format(detector=self.detector, id=idstring)

    @property
    def detector(self):
        return self._detector

    @property
    def number(self):
        return self._number

    @property
    def run_doc(self):
        return self._run_doc
