import os
import re
import time
from outsource.Config import Config
from outsource import db

config = Config()


class RunConfigBase:
    """Base class that sets the defaults"""

    _update_run_db = False
    _force_rerun = False
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _executable = os.path.join(config.base_dir(), 'workflow', 'run-pax.sh')
    _workdir = config.get('Outsource', 'work_dir')
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _pax_version = config.get('Outsource', 'pax_version')


class RunConfig(RunConfigBase):
    """Object that gets passed to outsource for each run/workflow.

    This base class has essentially the same info as a dictionary passed as input"""

    required_attributes = ['input_location', 'output_location']

    def __init__(self, **kwargs):
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
    def generated_dir(self):
        return os.path.join(self.workdir, 'generated', self.workflow_id)

    @property
    def workflow_path(self):
        return os.path.join(config.runs_dir(), self.workflow_id)


class DBConfig(RunConfig):
    """Uses runDB to build config info"""
    # default detector is tpc
    _detector = 'tpc'

    def __init__(self, number, **kwargs):
        self._number = number
        self._name = db.get_name(number)
        self._run_doc = db.get_doc(self.number)

        # eventually get the input location, etc here using DB
        rawdir = os.path.join('/xenon/xenon1t/raw', self.name)
        output = os.path.join('/xenon/nt_tests/processed', self.name)

        super().__init__(input_location=rawdir, output_location=output, **kwargs)


    @property
    def workflow_id(self):
        if self.detector == 'tpc':
            idstring = "{:06d}".format(self.number)
        elif self.detector == 'muon_veto':
            idstring = self.name
        else:
            raise NotImplementedError
        return "xe1t_{detector}_{id}".format(detector=self.detector, id=idstring)

    @property
    def detector(self):
        return self._detector

    @property
    def name(self):
        return self._name

    @property
    def number(self):
        return self._number

    @property
    def run_doc(self):
        return self._run_doc
