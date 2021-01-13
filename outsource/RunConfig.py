import os
import re
import time
from .Config import config, base_dir, work_dir
from utilix import db

# HARDCODE alert
# we could also import strax(en), but this makes outsource submission not dependent on strax
# maybe we could put this in database?
DEPENDS_ON = {'records': ['raw_records'],
              'peaklets': ['records']
              }


class RunConfigBase:
    """Base class that sets the defaults"""

    _ignore_db = False
    _ignore_rucio = False
    _force_rerun = False
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _executable = os.path.join(base_dir, 'workflow', 'run-pax.sh')
    _workdir = work_dir
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _chunks_per_job = 20

    @property
    def rundb_arg(self):
        return "--ignore-db" if self._ignore_db else ""

    @property
    def rucio_arg(self):
        return "--ignore-rucio" if self._ignore_rucio else ""

    @property
    def workflow_id(self):
        return self._workflow_id

    @property
    def input_location(self):
        return self._input_location

    @property
    def output_location(self):
        return self._output_location


class RunConfig(RunConfigBase):
    """Object that gets passed to outsource for each run/workflow.

    This base class has essentially the same info as a dictionary passed as input"""

    required_attributes = ['strax_context', 'straxen_version']

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
    def strax_context(self):
        return self._strax_context


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

    @property
    def chunks_per_job(self):
        return self._chunks_per_job


class DBConfig(RunConfig):
    """Uses runDB to build _dbcfgs info"""
    needs_processed=None

    def __init__(self, number, **kwargs):
        self._number = number
        self._run_doc = db.get_doc(self.number)
        super().__init__(**kwargs)
        # get the datatypes that need to be processed
        self.needs_processed = self.process_these()
        # determine which rse the input data is on
        self.rses = self.rse_data_find()


    @property
    def workflow_id(self):
        idstring = "{:06d}".format(self.number)
        return "xent_{id}".format(id=idstring)

    @property
    def number(self):
        return self._number

    @property
    def strax_context(self):
        return self._strax_context

    @property
    def straxen_version(self):
        return self._straxen_version

    @property
    def run_doc(self):
        return self._run_doc

    def process_these(self):
        """Returns the list of datatypes we need to process"""
        # do we need to process?
        requested_dtypes = config.get_list('Outsource', 'dtypes')
        # for this context and straxen version, see if we have that data yet
        ret = []
        for dtype in requested_dtypes:
            hash = db.get_hash(self.strax_context, dtype, self.straxen_version)
            rses = db.get_rses(self._number, dtype, hash)
            # if this data is not on any rse, reprocess it
            if len(rses) == 0:
                ret.append(dtype)
        return ret

    def rse_data_find(self):
        if self.needs_processed is None:
            self.needs_processed = self.process_these()
        rses = dict()
        for dtype in self.needs_processed:
            input_dtypes = DEPENDS_ON[dtype]
            _rses_tmp = []
            for input_dtype in input_dtypes:
                hash = db.get_hash(self.strax_context, input_dtype, self.straxen_version)
                _rses_tmp.extend(db.get_rses(self.number, input_dtype, hash))
            rses[dtype] = list(set(_rses_tmp))
        return rses

    def depends_on(self, dtype):
        return DEPENDS_ON[dtype]

    def nchunks(self, dtype):
        hash = db.get_hash(self.strax_context, dtype, self.straxen_version)
        for d in self.run_doc['data']:
            if d['type'] == dtype and hash in d.get('did', '_not_a_hash_'):
                if 'meta' in d:
                    if 'file_count' in d['meta']:
                        # one file is the metadata, so subtract
                        return d['meta']['file_count'] - 1
