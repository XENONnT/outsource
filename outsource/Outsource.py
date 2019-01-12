#!/usr/bin/env python

import json
import logging
import os
import re
import socket
import subprocess
import sys

from outsource.Shell import Shell
from outsource.Config import Config
from outsource.rundb import DB

config = Config()
logger = logging.getLogger("my_logger")

# Pegasus environment
sys.path.insert(0, os.path.join(config.pegasus_path(), 'lib64/python2.6/site-packages'))
os.environ['PATH'] = os.path.join(config.pegasus_path(), 'bin') + ':' + os.environ['PATH']
from Pegasus.DAX3 import *


class Outsource:
    # Data availability to site selection map
    _rse_to_req_expr = {
        'UC_OSG_USERDISK': 'GLIDEIN_Country == "US"',
        'NIKHEF_USERDISK': 'GLIDEIN_ResourceName == "NIKHEF-ELPROD"',
        'CCIN2P3_USERDISK': 'GLIDEIN_ResourceName == "CCIN2P3"',
        'WEIZMANN_USERDISK': 'GLIDEIN_ResourceName == "WEIZMANN-LCG2"',
        'CNAF_USERDISK': 'GLIDEIN_ResourceName == "INFN-T1"',
        'CNAF_TAPE_USERDISK': '',
        'SURFSARA_USERDISK': '',
    }

    #def __init__(self, config):
    #    """Take a Config object as input to make the workflow"""
    #
    #    # TODO config stuff (see above also)
    #    self.config = config
    
    # information from the run database
    _run_info = None
    
    
    def __init__(self, detector, name, force_rerun = False, update_run_db = False):
        '''
        Creates a new Outsource object. Specifying a detector and name is required.
        '''
        if not detector:
            raise RuntimeError('Detector is a required parameter')
        if not name:
            raise RuntimeError('Name is a required parameter')
        self._detector = detector
        self._name = name
        self._force_rerun = force_rerun
        self._force_update_run_db = update_run_db
        self.config = config

        # logger
        console = logging.StreamHandler()
        # default log level - make logger/console match
        logger.setLevel(logging.INFO)
        console.setLevel(logging.INFO)
        # debug - where to get this from?
        if True:
            logger.setLevel(logging.DEBUG)
            console.setLevel(logging.DEBUG)
        # formatter
        formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
        console.setFormatter(formatter)
        logger.addHandler(console)

        # environment for subprocesses
        os.environ['X509_USER_PROXY'] = os.path.join(os.environ['HOME'], 'user_cert')

        # update run id, this is currently derived from detector and name
        self.config.set_run_id(detector + '__' + name)
        self.config.set_run_name(name)
        
        db = DB()
        self._run_info = db.get_run(name, detector)
        

    def submit_workflow(self):
        '''
        Main interface to submitting a new workflow
        '''

        # does workflow already exist?
        if os.path.exists(self.config.workflow_title()):
            logger.error("Workflow already exists at {path}. Exiting.".format(path=self.config.workflow_title()))
            return

        # work dirs
        try:
            os.makedirs(self.config.generated_dir(), 0o755)
        except OSError:
            pass
        try:
            os.makedirs(self.config.runs_dir(), 0o755)
        except OSError:
            pass
        
        # ensure we have a proxy with enough time left
        self._validate_x509_proxy()

        self._generate_dax()
        self._plan_and_submit()
    
    
    def _generate_dax(self):
        '''
        Use the Pegasus DAX API to build an abstract graph of the workflow
        '''
        
        # figure our where input data exists
        rucio_dataset, rses, stash_raw_path = self._data_find_locations()
        
        # determine the job requirements based on the data locations
        requirements = 'OSGVO_OS_STRING == "RHEL 7" && HAS_CVMFS_xenon_opensciencegrid_org'
        requirements = requirements + ' && (' + self._determine_target_sites(rses, stash_raw_path) + ')'
        
        # Create a abstract dag
        dax = ADAG('xenonnt')
        
        # event callouts
        dax.invoke('start',  self.config.base_dir() + '/workflow/events/wf-start')
        dax.invoke('at_end',  self.config.base_dir() + '/workflow/events/wf-end')
        
        # Add executables to the DAX-level replica catalog
        wrapper = Executable(name='run-pax.sh', arch='x86_64', installed=False)
        wrapper.addPFN(PFN('file://' + self.config.base_dir() + '/workflow/run-pax.sh', 'local'))
        wrapper.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements))
        wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 1))
        dax.addExecutable(wrapper)
        
        pax_version = self.config.pax_version()

        # determine_rse - a helper for the job to determine where to pull data from
        determine_rse = File('determine_rse.py')
        determine_rse.addPFN(PFN('file://' + os.path.join(config.base_dir(), 'workflow/determine_rse.py'), 'local'))
        dax.addFile(determine_rse)

        # json file for the run
        self._write_run_info_json(os.path.join(config.generated_dir(), 'run_info.json'))
        json_infile = File('run_info.json')
        json_infile.addPFN(PFN('file://' + os.path.join(config.generated_dir(), 'run_info.json'), 'local'))
        dax.addFile(json_infile)
        
        # add jobs, one for each input file
        for zip_file, zip_props in self._data_find_zips(rucio_dataset, stash_raw_path).items():

            logger.debug("Adding job for zip file: " + zip_file)
    
            filepath, file_extenstion = os.path.splitext(zip_file)
            if file_extenstion != ".zip":
                raise RuntimeError('Non-zip in the input file list')

            file_rucio_dataset = None
            if zip_props['rucio_available']:
                file_rucio_dataset = rucio_dataset
                
            stash_gridftp_url = None
            if zip_props['stash_available']:
                stash_gridftp_url = 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm' + config.raw_dir() + '/' + zip_file 
        
            # output files
            job_output = File(zip_file + '.OUTPUT')
        
            # Add job
            job = Job(name='run-pax.sh')
            # Note that any changes to this argument list, also means run-pax.sh has to be updated
            job.addArguments(self.config.run_id(), 
                             zip_file,
                             str(file_rucio_dataset),
                             str(stash_gridftp_url),
                             socket.gethostname(),
                             pax_version,
                             "n/a",
                             str(1),
                             'False')
            job.uses(determine_rse, link=Link.INPUT)
            job.uses(json_infile, link=Link.INPUT)
            #job.uses(job_output, link=Link.OUTPUT)
            dax.addJob(job)
        
        # Write the DAX to stdout
        f = open(os.path.join(self.config.generated_dir(), 'dax.xml'), 'w')
        dax.writeXML(f)
        f.close()
       
        
    def _plan_and_submit(self):
        '''
        Call out to plan-env-helper.sh to start the workflow
        '''
        
        cmd = ' '.join([os.path.join(self.config.base_dir(), 'workflow/plan-env-helper.sh'),
                        self.config.base_dir(),
                        self.config.generated_dir(),
                        self.config.runs_dir(),
                        self.config.run_id()])
        shell = Shell(cmd, log_cmd = False, log_outerr = True)
        shell.run()
       
    
    def _validate_x509_proxy(self):
        '''
        ensure $HOME/user_cert exists and has enough time left
        '''
        logger.info('Verifying that the ~/user_cert proxy has enough lifetime')  
        min_valid_hours = 48
        shell = Shell('grid-proxy-info -timeleft -file ~/user_cert')
        shell.run()
        valid_hours = int(shell.get_outerr()) / 60 / 60
        if valid_hours < min_valid_hours:
            raise RuntimeError('User proxy is only valid for %d hours. Minimum required is %d hours.' \
                               %(valid_hours, min_valid_hours))


    def _write_run_info_json(self, json_file):
        '''
        take a run info structure, write to json file
        '''

        with open(json_file, "w") as f:
            # fix run_info so that all '|' become '.' in json
            fixed_run_info = self._fix_keys(self._run_info)
            json.dump(fixed_run_info, f)
        if os.stat(json_file).st_uid == os.getuid():
            os.chmod(json_file, 0o777)
        return json_file


    def _fix_keys(self,  dictionary):
        for key, value in dictionary.items():
            if type(value) in [type(dict())]:
                dictionary[key] = self._fix_keys(value)
            if '|' in key:
                dictionary[key.replace('|', '.')] = dictionary.pop(key)
        return dictionary
  
 
    def _data_find_locations(self):
        '''
        Check Rucio and other locations to determine where input files exist
        '''

        rucio_dataset = None
        rses = []
        stash_raw_path = None
        
        for d in self._run_info['data']:
            if d['host'] == 'rucio-catalogue' and d['status'] == 'transferred':
                    rucio_dataset = d['location']
        
        if rucio_dataset:
            logger.info('Querying Rucio for RSEs for the data set ' + rucio_dataset)  
            out = subprocess.Popen(["rucio", "list-rules", rucio_dataset], stdout=subprocess.PIPE).stdout.read()
            out = out.decode("utf-8").split("\n")
            
            for line in out:
                line = re.sub(' +', ' ', line).split(" ")
                if len(line) > 4 and line[3][:2] == "OK":
                    rses.append(line[4])
            if len(rses) < 1:
                logger.warning("Problem finding Rucio RSEs")
                
        # also check local dir (which is available via GridFTP)
        if os.path.exists(config.raw_dir()):
            # also make sure the dir contains some zip files?
            stash_raw_path = config.raw_dir()
            
        return rucio_dataset, rses, stash_raw_path
    
    
    def _data_find_zips(self, rucio_dataset, stash_raw_path):
        '''
        Look up which zip files are in the dataset - return a dict where the keys are the
        zipfiles, and the values a dict of locations
        '''
        zip_files = {}

        if rucio_dataset:
            logger.info('Querying Rucio for files in  the data set ' + rucio_dataset)  
            out = subprocess.Popen(["rucio", "list-file-replicas", rucio_dataset], stdout=subprocess.PIPE).stdout.read()
            out = str(out).split("\\n")
            files = set([l.split(" ")[3] for l in out if '---' not in l and 'x1t' in l])
            for f in sorted([f for f in files if f.startswith('XENON1T')]):
                zip_files[f] = {'rucio_available': True}
        
        if stash_raw_path:
            logger.info('Checking ' + stash_raw_path + ' for local files')
            files = os.listdir(stash_raw_path)
            for f in sorted([f for f in files if f.startswith('XENON1T')]):
                if f not in zip_files:
                    zip_files[f] = {}
                zip_files[f]['stash_available'] = True
                
        # make sure all the properties are defined for all the files - we want a neat dict to 
        # smooth access later on
        for fname, props in zip_files.items():
            if 'rucio_available' not in props:
                props['rucio_available'] = False
            if 'stash_available' not in props:
                props['stash_available'] = False
        
        return zip_files


    def _determine_target_sites(self, rses, stash_raw_path):
        '''
        Given a list of RSEs, limit the runs for sites for those locations
        '''
        
        # want a temporary copy so we can modify it
        my_rses = rses.copy()
        
        # stash enables US processing
        if stash_raw_path and 'UC_OSG_USERDISK' not in my_rses:
            my_rses.append('UC_OSG_USERDISK')
        
        exprs = []
        for rse in my_rses:
            if rse in self._rse_to_req_expr:
                if self._rse_to_req_expr[rse] is not '':
                    exprs.append(self._rse_to_req_expr[rse])
            else:
                raise RuntimeError('We do not know how to handle the RSE: ' + rse)

        final_expr = ' || '.join(exprs)
        logger.info('Site expression from RSEs list: ' + final_expr)
        return final_expr


if __name__ == '__main__':
    outsource = Outsource()
    outsource.submit_workflow()
