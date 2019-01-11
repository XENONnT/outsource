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
sys.path.insert(0, os.path.join(config.get_pegasus_path(), 'lib64/python2.6/site-packages'))
os.environ['PATH'] = os.path.join(config.get_pegasus_path(), 'bin') + ':' + os.environ['PATH']
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
            os.makedirs(self.config.get_generated_dir(), 0o755)
        except OSError:
            pass
        try:
            os.makedirs(self.config.get_runs_dir(), 0o755)
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
        
        # figrue our where input data exists
        rucio_dataset, rses = self._data_find()
        
        # determine the job requirements based on the data locations
        requirements = 'OSGVO_OS_STRING == "RHEL 7" && HAS_CVMFS_xenon_opensciencegrid_org'
        requirements = requirements + ' && (' + self._determine_sites_from_rses(rses) + ')'
        
        # Create a abstract dag
        dax = ADAG('xenonnt')
        
        # event callouts
        dax.invoke('start',  self.config.get_base_dir() + '/workflow/events/wf-start')
        dax.invoke('at_end',  self.config.get_base_dir() + '/workflow/events/wf-end')
        
        # Add executables to the DAX-level replica catalog
        wrapper = Executable(name='run-pax.sh', arch='x86_64', installed=False)
        wrapper.addPFN(PFN('file://' + self.config.get_base_dir() + '/workflow/run-pax.sh', 'local'))
        wrapper.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements))
        wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 1))
        dax.addExecutable(wrapper)
        
        # for now, bypass the data find step and use: /xenon/xenon1t/raw/160315_1824
        
        base_url = 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm'

        pax_version = self.config.get_pax_version()
        rawdir = self.config.raw_dir()

        dir_raw = '/xenon/xenon1t/raw'
        
        #rawdir = os.path.join(dir_raw, run_id)

        # determine_rse - a helper for the job to determine where to pull data from
        determine_rse = File('determine_rse.py')
        determine_rse.addPFN(PFN('file://' + os.path.join(config.get_base_dir(), 'workflow/determine_rse.py'), 'local'))
        dax.addFile(determine_rse)

        # json file for the run
        self._write_run_info_json(os.path.join(config.get_generated_dir(), 'run_info.json'))
        json_infile = File('run_info.json')
        json_infile.addPFN(PFN('file://' + os.path.join(config.get_generated_dir(), 'run_info.json'), 'local'))
        dax.addFile(json_infile)
        
        # add jobs, one for each input file
        zip_counter = 0
        #for dir_name, subdir_list, file_list in os.walk(rawdir):
        #for infile in file_list:

        for infile in self._rucio_get_zips(rucio_dataset):

            logger.debug("Adding job for zip file: " + infile)
    
            filepath, file_extenstion = os.path.splitext(infile)
            if file_extenstion != ".zip":
                continue
    
            zip_counter += 1
            base_name = filepath.split("/")[-1]
            zip_name = base_name + file_extenstion
            #outfile = zip_name + ".root"

            infile_handle = re.sub('raw$', infile, rucio_dataset)
    
            # Add input file to the DAX-level replica catalog
            #infile_local = os.path.abspath(os.path.join(dir_name, infile))
            #zip_infile = File(zip_name)
            #zip_infile.addPFN(PFN(base_url + infile_local, 'UChicago'))
            #dax.addFile(zip_infile)
        
            # output files
            job_output = File(zip_name + '.OUTPUT')
        
            # Add job
            job = Job(name='run-pax.sh')
            # TODO update arguments and the executable
            job.addArguments(self.config.get_run_id(), 
                             infile_handle,
                             socket.gethostname(),
                             pax_version,
                             "n/a",
                             "n/a",
                             str(1),
                             'False',
                             json_infile,
                             'True')
            job.uses(determine_rse, link=Link.INPUT)
            job.uses(json_infile, link=Link.INPUT)
            #job.uses(zip_infile, link=Link.INPUT)
            job.uses(job_output, link=Link.OUTPUT)
            dax.addJob(job)
        
        # Write the DAX to stdout
        f = open(os.path.join(self.config.get_generated_dir(), 'dax.xml'), 'w')
        dax.writeXML(f)
        f.close()
       
        
    def _plan_and_submit(self):
        '''
        Call out to plan-env-helper.sh to start the workflow
        '''
        
        cmd = ' '.join([os.path.join(self.config.get_base_dir(), 'workflow/plan-env-helper.sh'),
                        self.config.get_base_dir(),
                        self.config.get_generated_dir(),
                        self.config.get_runs_dir(),
                        self.config.get_run_id()])
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
  
 
    def _data_find(self):
        '''
        call out to rucio to figure out where the data is located
        '''

        for d in self._run_info['data']:
            if d['host'] == 'rucio-catalogue' and d['status'] == 'transferred':
                    rucio_location = d['location']
        
        logger.info('Querying Rucio for RSEs for the data set ' + rucio_location)  
        out = subprocess.Popen(["rucio", "list-rules", rucio_location], stdout=subprocess.PIPE).stdout.read()
        out = out.decode("utf-8").split("\n")
        rses = []
        for line in out:
            line = re.sub(' +', ' ', line).split(" ")
            if len(line) > 4 and line[3][:2] == "OK":
                rses.append(line[4])
        if len(rses) < 1:
            raise RuntimeError("Problem finding rucio rses")
        return rucio_location, rses


    def _determine_sites_from_rses(self, rses):
        '''
        Given a list of RSEs, limit the runs for sites for those locations
        '''
        
        exprs = []
        for rse in rses:
            if rse in self._rse_to_req_expr:
                if self._rse_to_req_expr[rse] is not '':
                    exprs.append(self._rse_to_req_expr[rse])
            else:
                raise RuntimeError('We do not know how to handle the RSE: ' + rse)

        final_expr = ' || '.join(exprs)
        logger.info('Site expression from RSEs list: ' + final_expr)
        return final_expr

    def _rucio_get_zips(self, dataset):
        '''
        Look up which zip files are in the dataset
        '''

        logger.info('Querying Rucio for files in  the data set ' + dataset)  
        out = subprocess.Popen(["rucio", "list-file-replicas", dataset], stdout=subprocess.PIPE).stdout.read()
        out = str(out).split("\\n")
        files = set([l.split(" ")[3] for l in out if '---' not in l and 'x1t' in l])
        zip_files = sorted([f for f in files if f.startswith('XENON1T')])
        return zip_files


if __name__ == '__main__':
    outsource = Outsource()
    outsource.submit_workflow()
