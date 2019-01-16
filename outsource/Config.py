import re
import os
import time
import configparser

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
        
        def __init__(self):
            
            config_file_path = os.path.join(os.environ['HOME'], '.xenonnt.conf')
            print('Loading configuration from %s' %(config_file_path)) 
            
            configparser.ConfigParser.__init__(self, interpolation=EnvInterpolation())
            try:
                self.readfp(open(config_file_path), 'r')
            except FileNotFoundError as e:
                raise RuntimeError('Unable to open %s. Please see the README for an example configuration' %(config_file_path)) from e

        def base_dir(self):
            return os.path.dirname(__file__)
        
        def work_dir(self):
            return self.get('Outsource', 'work_dir')
        
        def runs_dir(self):
            return os.path.join(self.work_dir(), 'runs')

        def pegasus_path(self):
            return self.get('Outsource', 'pegasus_path')
