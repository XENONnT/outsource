import re
import os
import time
import getpass
from . import db

dir_raw = '/xenon/xenon1t/raw'

class Config:
    
    _run_id = None
    
    def __init__(self):
        self._run_id = str(time.time())
        self._run_id = re.sub('\..*', '', self._run_id)

    
    def get_run_id(self):
        return self._run_id
    
    def get_base_dir(self):
        return os.path.dirname(__file__)
    
    def get_work_dir(self):
        return os.path.join('/scratch', getpass.getuser(), 'workflows')
    
    def get_runs_dir(self):
        return os.path.join('/scratch', getpass.getuser(), 'workflows/runs')
    
    def get_generated_dir(self):
        return os.path.join(self.get_work_dir(), 'generated')
    
    def get_pax_version(self):
        return 'v6.9.0'
    
    def get_pegasus_path(self):
        return '/cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/rhel6/4.9.0dev'

    def raw_dir(self):
        return os.path.join(dir_raw, '160315_1824')

    def workflow_title(self):
        return os.path.join(self.get_runs_dir(), self.run_id)

    @property
    def run_id(self):
        return self._run_id


class ConfigDB(Config):
    """Object that uses run identifier and RunDB API to make config"""

    def __init__(self, run_id, detector='tpc'):
        self._run_id = db.get_name(run_id, detector)
