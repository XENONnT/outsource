import os
import re
import time
from .Config import config, base_dir, work_dir
from utilix import DB, xent_collection
import straxen
import cutax
import strax
import admix
import numpy as np

# HARDCODE alert
# we could also import strax(en), but this makes outsource submission not dependent on strax
# maybe we could put this in database?
DEPENDS_ON = {'records': ['raw_records'],
              'peaklets': ['raw_records'],
              'peak_basics': ['peaklets'],
              'peak_basics_he': ['raw_records_he'],
              'event_info_double': ['peaklets'],
              'event_shadow': ['peaklets'],
              'hitlets_nv': ['raw_records_nv'],
              'events_nv': ['hitlets_nv'],
              'ref_mon_nv':['hitlets_nv'],
              'events_mv': ['raw_records_mv'],
              'afterpulses': ['raw_records'],
              'led_calibration': ['raw_records']
              }

# Corresponding to keys in ACTUALLY_STORED
DETECTOR_DTYPES = {'tpc': ['records', 'peaklets', 'peak_basics', 'event_info_double',
                           'event_shadow', 'peak_basics_he', 'afterpulses', 'led_calibration'],
                   'neutron_veto': ['hitlets_nv', 'events_nv', 'ref_mon_nv'],
                   'muon_veto': ['events_mv']
                   }

# these are datetypes to look for in runDB
ACTUALLY_STORED = {'event_info_double': ['peak_basics', 
                                         'event_info', 'distinct_channels', 'event_pattern_fit', 
                                         'event_area_per_channel', 'event_n_channel',
                                         'event_top_bottom_params', 'event_ms_naive',
                                         'event_ambience', 'event_shadow', 'peak_s1_positions_cnn'],
                   'event_shadow': ['event_shadow', 'event_ambience'],
                   'peak_basics_he': ['peak_basics_he'],
                   'events_nv': ['ref_mon_nv', 'events_nv'],
                   'ref_mon_nv': ['ref_mon_nv']
                   'peak_basics': ['merged_s2s', 'peak_basics', 'peaklet_classification'],
                   'peaklets': ['peaklets', 'lone_hits'],
                   'hitlets_nv': ['hitlets_nv'],
                   'events_mv': ['events_mv'],
                   'afterpulses': ['afterpulses'],
                   'led_calibration': ['led_calibration']
                   }

# these modes have particular datatypes we care about
LED_MODES = {'tpc_pmtap': ['afterpulses'],
             'tpc_commissioning_pmtap': ['afterpulses'],
             'tpc_pmtgain': ['led_calibration']
             }

LED_DTYPES = []
for mode, dtypes in LED_MODES.items():
    for d in dtypes:
        if d not in LED_DTYPES:
            LED_DTYPES.append(d)


db = DB()
coll = xent_collection()


def get_hashes(st):
    return {key: val['hash'] for key, val in st.provided_dtypes().items()}


class RunConfigBase:
    """Base class that sets the defaults"""
    _force_rerun = False
    _standalone_download = False 
    _x509_proxy = os.path.join(os.environ['HOME'], 'user_cert')
    _workdir = work_dir
    _workflow_id = re.sub('\..*', '', str(time.time()))
    _chunks_per_job = config.getint('Outsource', 'chunks_per_job')

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

    def __init__(self, number, st, **kwargs):
        self._number = number
        self.run_data = db.get_data(number)

        self.context = st
        self.hashes = get_hashes(st)

        # get the detectors and start time of this run
        cursor = coll.find_one({'number': number}, {'detectors': 1, 'start': 1, '_id': 0,
                                                    'mode': 1})
        self.detectors = cursor['detectors']
        self.start = cursor['start']
        self.mode = cursor['mode']
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
        # do we need to process? read from xenon_config
        requested_dtypes = config.get_list('Outsource', 'dtypes')

        # if we are using LED data, only process those dtyopes
        # for this context and straxen version, see if we have that data yet
        if self.mode in LED_MODES:
            requested_dtypes = [dtype for dtype in requested_dtypes if dtype in LED_MODES[self.mode]]
        # if we are not, don't process those dtypes
        else:
            requested_dtypes = list(set(requested_dtypes) - set(LED_DTYPES))

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

        ret.sort(key=lambda x: len(get_dependencies(self.context, x)))

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


def get_dependencies(st, target):
    ret = []
    def _get_dependencies(target):
        plugin = st._plugin_class_registry[target]()
        dependencies = list(strax.to_str_tuple(plugin.depends_on))
        ret.extend(dependencies)
        if len(dependencies):
            for dep in dependencies:
                _get_dependencies(dep)

    _get_dependencies(target)
    ret = np.unique(ret).tolist()
    return ret

