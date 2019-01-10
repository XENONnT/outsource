#!/usr/bin/env python

import sys
import os
import socket
import re

from Shell import Shell
from outsource.Config import Config
config = Config()
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
        
        # TODO: look up run here


    def submit_workflow(self):
        '''
        Main interface to submitting a new workflow
        '''

        # does workflow already exist?
        if os.path.exists(self.config.workflow_title()):
            print("Workflow already exists at {path}. Exiting.".format(path=self.config.workflow_title()))
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
        
        # determine the job requirements based on the data locations
        requirements = 'OSGVO_OS_STRING == "RHEL 7" && HAS_CVMFS_xenon_opensciencegrid_org'
        requirements = requirements + ' && (' + self._determine_sites_from_rses(None) + ')'
        
        # Create a abstract dag
        dax = ADAG('xenonnt')
        
        # event callouts
        dax.invoke('start',  self.config.get_base_dir() + '/events/wf-start')
        dax.invoke('at_end',  self.config.get_base_dir() + '/events/wf-end')
        
        # Add executables to the DAX-level replica catalog
        wrapper = Executable(name='run-pax.sh', arch='x86_64', installed=False)
        #wrapper.addPFN(PFN('file://' + self.config.get_base_dir() + '/run-pax.sh', 'local'))
        wrapper.addPFN(PFN('file://' + config.get_base_dir() + '/run-pax.sh', 'local'))
        wrapper.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements))
        wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 1))
        dax.addExecutable(wrapper)
        
        # for now, bypass the data find step and use: /xenon/xenon1t/raw/160315_1824
        
        base_url = 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm'

        pax_version = self.config.get_pax_version()
        rawdir = self.config.raw_dir() #os.path.join(dir_raw, run_id)
        # run_id = '160315_1824'
        run_id = self.config.run_id

        dir_raw = '/xenon/xenon1t/raw'
        
        pax_version = 'v6.9.0'
        run_id = '160315_1824'
        
        rawdir = os.path.join(dir_raw, run_id)

        # determine_rse - a helper for the job to determine where to pull data from
        determine_rse = File('determine_rse.py')
        determine_rse.addPFN(PFN('file://' + os.path.join(config.get_base_dir(), 'task-data/determine-rse.py'), 'local'))
        dax.addFile(determine_rse)

        # json file for the run - TODO: verify existance
        json_infile = File('pax_info.json')
        json_infile.addPFN(PFN(base_url + os.path.join(rawdir, 'pax_info.json'), 'UChicago'))
        dax.addFile(json_infile)
        
        # add jobs, one for each input file
        zip_counter = 0
        for dir_name, subdir_list, file_list in os.walk(rawdir):
        
            for infile in file_list:
        
                # TODO: testing - only do 5 zip files for now
                if zip_counter == 5:
                    continue
        
                filepath, file_extenstion = os.path.splitext(infile)
                if file_extenstion != ".zip":
                    continue
        
                zip_counter += 1
                base_name = filepath.split("/")[-1]
                zip_name = base_name + file_extenstion
                #outfile = zip_name + ".root"
                infile_local = os.path.abspath(os.path.join(dir_name, infile))
        
                # Add input file to the DAX-level replica catalog
                zip_infile = File(zip_name)
                zip_infile.addPFN(PFN(base_url + infile_local, 'UChicago'))
                dax.addFile(zip_infile)
            
                # output files
                job_output = File(zip_name + '.OUTPUT')
            
                # Add job
                job = Job(name='run-pax.sh')
                # TODO update arguments and the executable
                job.addArguments(run_id, 
                                 "n/a",
                                 socket.gethostname(),
                                 pax_version,
                                 "n/a",
                                 "n/a",
                                 str(1),
                                 'False',
                                 json_infile,
                                 'False',
                                 'n/a')
                job.uses(determine_rse, link=Link.INPUT)
                job.uses(json_infile, link=Link.INPUT)
                job.uses(zip_infile, link=Link.INPUT)
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
        
        cmd = ' '.join([os.path.join(self.config.get_base_dir(), 'plan-env-helper.sh'),
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
        
        min_valid_hours = 48
        shell = Shell('grid-proxy-info -timeleft -file ~/user_cert')
        shell.run()
        valid_hours = int(shell.get_outerr()) / 60 / 60
        if valid_hours < min_valid_hours:
            raise RuntimeError('User proxy is only valid for %d hours. Minimum required is %d hours.' \
                               %(valid_hours, min_valid_hours))
         

    def _determine_sites_from_rses(self, rses):
        '''
        Given a list of RSEs, limit the runs for sites for those locations
        '''
        
        exprs = []
        rses = ['CNAF_USERDISK', 'SURFSARA_USERDISK', 'UC_OSG_USERDISK']

        for rse in rses:
            if rse in self._rse_to_req_expr:
                if self._rse_to_req_expr[rse] is not '':
                    exprs.append(self._rse_to_req_expr[rse])
            else:
                raise RuntimeError('We do not know how to handle the RSE: ' + rse)

        final_expr = ' || '.join(exprs)
        print('Site expression from RSEs list: ' + final_expr)
        return final_expr


if __name__ == '__main__':
    outsource = Outsource()
    outsource.submit_workflow()
