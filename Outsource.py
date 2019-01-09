#!/usr/bin/env python

import sys
import os
import socket
import getpass

from Shell import Shell
from outsource.Config import Config
config = Config()
# Pegasus environment
sys.path.insert(0, os.path.join(config.get_pegasus_path(), 'lib64/python2.6/site-packages'))
os.environ['PATH'] = os.path.join(config.get_pegasus_path(), 'bin') + ':' + os.environ['PATH']
from Pegasus.DAX3 import *


class Outsource:

    def __init__(self, config):
        """Take a Config object as input to make the workflow"""

        # TODO config stuff (see above also)
        self.config = config

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
        
        # Create a abstract dag
        dax = ADAG('xenonnt')
        
        # event callouts
        dax.invoke('start',  self.config.get_base_dir() + '/events/wf-start')
        dax.invoke('at_end',  self.config.get_base_dir() + '/events/wf-end')
        
        # Add executables to the DAX-level replica catalog
        wrapper = Executable(name='run-pax.sh', arch='x86_64', installed=False)
        wrapper.addPFN(PFN('file://' + self.config.get_base_dir() + '/run-pax.sh', 'local'))
        wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 1))
        dax.addExecutable(wrapper)
        
        # for now, bypass the data find step and use: /xenon/xenon1t/raw/160315_1824
        
        base_url = 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm'

        pax_version = self.config.get_pax_version()
        rawdir = self.config.raw_dir() #os.path.join(dir_raw, run_id)
        # run_id = '160315_1824'
        run_id = self.config.run_id

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
         

if __name__ == '__main__':
    outsource = Outsource()
    outsource.submit_workflow()
