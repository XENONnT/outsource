import re
import os
import time
import getpass
#from outsource.rundb import DB

import configparser
import threading

dir_raw = '/xenon/xenon1t/raw'


class EnvInterpolation(configparser.BasicInterpolation):
    '''Interpolation which expands environment variables in values.'''

    def before_get(self, parser, section, option, value, defaults):
        return os.path.expandvars(value)


class Config():
    
    # singleton
    instance = None
    
    def __init__(self):
        if not Config.instance:
            Config.instance = Config.__Config()
            
    
    def __getattr__(self, name):
        return getattr(self.instance, name)
    
    
    class __Config(configparser.ConfigParser):
        
        _run_id = None
        
        def __init__(self):
            
            config_file_path = os.path.join(os.environ['HOME'], '.xenonnt.conf')
            print('Loading configuration from %s' %(config_file_path)) 
            
            configparser.ConfigParser.__init__(self, interpolation=EnvInterpolation())
            try:
                self.readfp(open(config_file_path), 'r')
            except FileNotFoundError as e:
                raise RuntimeError('Unable to open %s. Please see the README for an example configuration' %(config_file_path)) from e
    
            self._run_id = str(time.time())
            self._run_id = re.sub('\..*', '', self._run_id)
    
        
        def get_run_id(self):
            return self._run_id
        
        def get_base_dir(self):
            return os.path.dirname(__file__)
        
        def get_work_dir(self):
            return self.get('Outsource', 'work_dir')
        
        def get_runs_dir(self):
            return os.path.join(self.get_work_dir(), 'runs')
        
        def get_generated_dir(self):
            return os.path.join(self.get_work_dir(), 'generated', self._run_id)
        
        def get_pax_version(self):
            return 'v' + self.get('Outsource', 'pax_version')
        
        def get_pegasus_path(self):
            return self.get('Outsource', 'pegasus_path')
    
        def raw_dir(self):
            return os.path.join(dir_raw, '160315_1824')
    
        def workflow_title(self):
            return os.path.join(self.get_runs_dir(), self.run_id)
   
        def set_run_id(self, run_id):
            self._run_id = run_id

        @property
        def run_id(self):
            return self._run_id


class ConfigDB(Config):
    """Object that uses run identifier and RunDB API to make config"""

    def __init__(self, run_id, detector='tpc'):
        #self._run_id = DB.get_name(run_id, detector)
        pass

