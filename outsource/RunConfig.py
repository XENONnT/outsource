import os
import re
import time
from .Config import config, base_dir, work_dir
from utilix import DB, xent_collection
import straxen
import cutax
import admix

# HARDCODE alert
# we could also import strax(en), but this makes outsource submission not dependent on strax
# maybe we could put this in database?
DEPENDS_ON = {'records': ['raw_records'],
              'peaklets': ['raw_records'],
              'peak_basics_he': ['raw_records_he'],
              'event_info_double': ['peaklets'],
              'hitlets_nv': ['raw_records_nv'],
              'event_positions_nv': ['hitlets_nv'],
              'events_mv': ['raw_records_mv']
              }

DETECTOR_DTYPES = {'tpc': ['records', 'peaklets', 'event_info_double', 'peak_basics_he'],
                   'neutron_veto': ['hitlets_nv', 'event_positions_nv'],
                   'muon_veto': ['events_mv']
                   }

# these are datetypes to look for in runDB
ACTUALLY_STORED = {'event_info_double': ['event_info', 'distinct_channels', 'event_pattern_fit'],
                   'peak_basics_he': ['peak_basics_he'],
                   'event_positions_nv': ['event_positions_nv'],
                   'peaklets': ['peaklets', 'lone_hits'],
                   'hitlets_nv': ['hitlets_nv'],
                   'events_mv': ['events_mv']
                   }


db = DB()
coll = xent_collection()


def get_hashes(st):
    return {key: val['hash'] for key, val in st.provided_dtypes().items()}


class RunConfigBase:
    """Base class that sets the defaults"""

    _update_db = False
    _upload_to_rucio = False
    _force_rerun = False
    _standalone_download = False 
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _workdir = work_dir
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _chunks_per_job = 25
    _staging_site = 'staging'

    #@property
    def rundb_arg(self):
        return "--update-db" if self._update_db else ""

    #@property
    def rucio_arg(self):
        return "--upload-to-rucio" if self._upload_to_rucio else ""
    
    @property
    def update_db(self):
        return self._update_db
    
    @property
    def upload_to_rucio(self):
        return self._upload_to_rucio

    @property
    def force_rerun(self):
        return self._force_rerun
    
    @property
    def standalone_download(self):
        return self._standalone_download

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
    def staging_site(self):
        return self._staging_site


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
    needs_processed = None

    def __init__(self, number, context_name, **kwargs):
        self._number = number
        self.run_data = db.get_data(number)
        # setup context
        st = getattr(cutax.contexts, context_name)()
        st.storage = []
        st.context_config['forbid_creation_of'] = straxen.daqreader.DAQReader.provides

        self.context_name = context_name
        self.context = st
        self.hashes = get_hashes(st)

        # get the detectors that were on during this run
        self.detectors = coll.find_one({'number': number}, {'detectors': 1, '_id': 0})['detectors']
        assert isinstance(self.detectors, list), \
            f"Detectors needs to be a list, not a {type(self.detectors)}"

        super().__init__(**kwargs)
        # get the datatypes that need to be processed
        self.needs_processed = self.process_these()
        # determine which rse the input data is on
        self.rses = self.rse_data_find()
        self.raw_data_exists = self._raw_data_exists()

    @property
    def workflow_id(self):
        return f"xent_{self.number:06d}"

    @property
    def number(self):
        return self._number

    def process_these(self):
        """Returns the list of datatypes we need to process"""
        # do we need to process?
        requested_dtypes = config.get_list('Outsource', 'dtypes')
        # for this context and straxen version, see if we have that data yet

        # get all possible dtypes we can process for this run
        possible_dtypes = []
        for detector in self.detectors:
            possible_dtypes.extend(DETECTOR_DTYPES[detector])

        # modify requested_dtypes to only consider the possible ones
        requested_dtypes = [dtype for dtype in requested_dtypes if dtype in possible_dtypes]

        ret = []
        for category in requested_dtypes:
            dtypes_already_processed = []
            for dtype in ACTUALLY_STORED[category]:
                hash = self.hashes[dtype]
                rses = db.get_rses(self._number, dtype, hash)
                # if this data is not on any rse, reprocess it, or we are asking for a rerun
                dtypes_already_processed.append(len(rses) > 0)
            if not all(dtypes_already_processed) or self._force_rerun:
                ret.append(category)

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
        # get the dtype this one depends on
        dtype = self.depends_on(dtype)[0]
        hash = self.hashes[dtype]
        did = f"xnt_{self._number:06d}:{dtype}-{hash}"
        files = admix.rucio.list_files(did)
        # subtract 1 for metadata
        return len(files) - 1


    def _raw_data_exists(self, raw_type='raw_records'):
        """Returns a boolean for whether or not raw data exists in rucio and is accessible"""
        # it's faster to just go through runDB

        for data in self.run_data:
            if (data['type'] == raw_type and
                data['host'] == 'rucio-catalogue' and
                data['status'] == 'transferred' and
                data['location'] != 'LNGS_USERDISK' and
                'TAPE' not in data['location']):
                return True
        return False

