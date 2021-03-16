import os
import re
import time
from .Config import config, base_dir, work_dir
from utilix import db
import straxen

# HARDCODE alert
# we could also import strax(en), but this makes outsource submission not dependent on strax
# maybe we could put this in database?
DEPENDS_ON = {'records': ['raw_records'],
              'peaklets': ['records'],
              'event_info_double': ['peaklets']
              }


def apply_global_version(context, cmt_version):
    context.set_config(dict(gain_model=('CMT_model', ("to_pe_model", cmt_version))))
    context.set_config(dict(s2_xy_correction_map=("CMT_model", ('s2_xy_map', cmt_version), True)))
    context.set_config(dict(elife_file=("elife_model", cmt_version, True)))
    context.set_config(dict(mlp_model=("CMT_model", ("mlp_model", cmt_version), True)))
    context.set_config(dict(gcn_model=("CMT_model", ("gcn_model", cmt_version), True)))
    context.set_config(dict(cnn_model=("CMT_model", ("cnn_model", cmt_version), True)))


def get_hashes(st):
    hashes = set([(d, st.key_for('0', d).lineage_hash)
                  for p in st._plugin_class_registry.values()
                  for d in p.provides])
    return {dtype: h for dtype, h in hashes}


class RunConfigBase:
    """Base class that sets the defaults"""

    _ignore_db = False
    _ignore_rucio = False
    _force_rerun = False
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _executable = os.path.join(base_dir, 'workflow', 'run-pax.sh')
    _workdir = work_dir
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _chunks_per_job = 25

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

    def __init__(self, **kwargs):
        # default job priority - workflows will be given priority
        # in the order they were submitted.
        self._priority = 2250000000 - int(time.time())
        
        for key, val in kwargs.items():
            setattr(self, "_" + key, val)

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

    def __init__(self, number, context_name, cmt_version, **kwargs):
        self._number = number
        self._run_doc = db.get_doc(self.number)
        self.cmt_global = cmt_version
        # setup context
        st = getattr(straxen.contexts, context_name)()
        st.storage = []
        st.context_config['forbid_creation_of'] = straxen.daqreader.DAQReader.provides
        apply_global_version(st, cmt_version)

        self.context_name = context_name
        self.context = st
        self.hashes = get_hashes(st)

        super().__init__(**kwargs)
        # get the datatypes that need to be processed
        self.needs_processed = self.process_these()
        # determine which rse the input data is on
        self.rses = self.rse_data_find()

    @property
    def workflow_id(self):
        return f"xent_{self.number:06d}_{self.cmt_global}"

    @property
    def number(self):
        return self._number

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
            hash = self.hashes[dtype]
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
                hash = self.hashes[input_dtype]
                _rses_tmp.extend(db.get_rses(self.number, input_dtype, hash))
            rses[dtype] = list(set(_rses_tmp))
        return rses

    def depends_on(self, dtype):
        return DEPENDS_ON[dtype]

    def nchunks(self, dtype):
        hash = self.hashes[dtype]
        for d in self.run_doc['data']:
            if d['type'] == dtype and hash in d.get('did', '_not_a_hash_'):
                if 'meta' in d:
                    if 'file_count' in d['meta']:
                        # one file is the metadata, so subtract
                        return d['meta']['file_count'] - 1


