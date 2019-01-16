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

config = Config()
logger = logging.getLogger("outsource")

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

    def __init__(self, dbcfgs):
        '''
        Creates a new Outsource object. Specifying a list of DBConfig objects required.
        '''
       
        if not dbcfgs:
            raise RuntimeError('At least one DBConfig is required')
        if not isinstance(dbcfgs, list):
            raise RuntimeError('Outsource expects a list of DBConfigs to run')
        # TODO there's likely going to be some confusion between the two configs here
        self._dbcfgs = dbcfgs

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
        if (logger.hasHandlers()):
            logger.handlers.clear()
        logger.addHandler(console)

        # environment for subprocesses
        os.environ['X509_USER_PROXY'] = self._dbcfgs[0].x509_proxy
        
        # Determine a unique id for the workflow. If only one dbconfig is provided, use
        # the workflow id of that object. If more than one is provided, make one up.
        if len(self._dbcfgs) == 1:
            self._wf_id = self._dbcfgs[0].workflow_id
        else:
            self._wf_id = 'multiples-' + self._dbcfgs[0].workflow_id


    def submit_workflow(self):
        '''
        Main interface to submitting a new workflow
        '''

        # does workflow already exist?
        if os.path.exists(self._workflow_dir()):
            logger.error("Workflow already exists at {path}. Exiting.".format(path=self._workflow_dir()))
            return

        # work dirs
        try:
            os.makedirs(self._generated_dir(), 0o755)
        except OSError:
            pass
        try:
            os.makedirs(config.runs_dir(), 0o755)
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
        dax.invoke('start',  config.base_dir() + '/workflow/events/wf-start')
        dax.invoke('at_end',  config.base_dir() + '/workflow/events/wf-end')
        
        # Add executables to the DAX-level replica catalog
        wrapper = Executable(name='run-pax.sh', arch='x86_64', installed=False)
        wrapper.addPFN(PFN('file://' + config.base_dir() + '/workflow/run-pax.sh', 'local'))
        wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 1))
        dax.addExecutable(wrapper)

        merge = Executable(name='merge.sh', arch='x86_64', installed=False)
        merge.addPFN(PFN('file://' + config.base_dir() + '/workflow/merge.sh', 'local'))
        dax.addExecutable(merge)
        
        upload = Executable(name='upload.sh', arch='x86_64', installed=False)
        upload.addPFN(PFN('file://' + config.base_dir() + '/workflow/upload.sh', 'local'))
        dax.addExecutable(upload)

        # determine_rse - a helper for the job to determine where to pull data from
        determine_rse = File('determine_rse.py')
        determine_rse.addPFN(PFN('file://' + os.path.join(config.base_dir(), 'workflow/determine_rse.py'), 'local'))
        dax.addFile(determine_rse)

        # paxify is what processes the data. Gets called by the executable run-pax.sh
        paxify = File('paxify.py')
        paxify.addPFN(PFN('file://' + os.path.join(config.base_dir(), 'workflow/paxify.py'), 'local'))
        dax.addFile(paxify)

        for dbcfg in self._dbcfgs:
            
            logger.info('Adding run ' + dbcfg.name + ' to the workflow')
        
            # figure our where input data exists
            rucio_dataset, rses, stash_raw_path = self._data_find_locations(dbcfg)
            
            # determine the job requirements based on the data locations
            requirements = 'OSGVO_OS_STRING == "RHEL 7" && HAS_CVMFS_xenon_opensciencegrid_org'
            requirements = requirements + ' && (' + self._determine_target_sites(rses, stash_raw_path) + ')'
            if self._exclude_sites():
                requirements = requirements + ' && (' + self._exclude_sites()  + ')'
            # map some jobs to US
            requirements_us = 'OSGVO_OS_STRING == "RHEL 7" && HAS_CVMFS_xenon_opensciencegrid_org && GLIDEIN_Country == "US"'
                        
            pax_version = dbcfg.pax_version
    
            # json file for the run
            json_file = os.path.join(self._generated_dir(), dbcfg.name + '.json')
            write_json_file(dbcfg.run_doc, json_file)
            json_infile = File(dbcfg.name + '.json')
            json_infile.addPFN(PFN('file://' + os.path.join(self._generated_dir(), dbcfg.name + '.json'), 'local'))
            dax.addFile(json_infile)
            
            # Set up the merge job first - we can then add to that job inside the zip file loop
            merged_root = File(dbcfg.name + '.root')
            merge_job = Job('merge.sh')
            merge_job.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements_us))
            merge_job.addProfile(Profile(Namespace.CONDOR, 'priority', str(dbcfg.priority * 5)))
            merge_job.uses(merged_root, link=Link.OUTPUT)
            merge_job.addArguments(merged_root)
            dax.addJob(merge_job)
            
            # add jobs, one for each input file
            for zip_file, zip_props in self._data_find_zips(rucio_dataset, stash_raw_path).items():
    
                logger.debug(" ... adding job for zip file: " + zip_file)
        
                filepath, file_extenstion = os.path.splitext(zip_file)
                if file_extenstion != ".zip":
                    raise RuntimeError('Non-zip in the input file list')
    
                file_rucio_dataset = None
                if zip_props['rucio_available']:
                    file_rucio_dataset = rucio_dataset
                    
                stash_gridftp_url = None
                if zip_props['stash_available']:
                    stash_gridftp_url = 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm' + \
                                        dbcfg.input_location + '/' + zip_file
            
                # output files
                job_output = File(zip_file.replace('.zip', '.root'))
            
                # Add job
                job = Job(name='run-pax.sh')
                job.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements))
                job.addProfile(Profile(Namespace.CONDOR, 'priority', str(dbcfg.priority)))
                # Note that any changes to this argument list, also means run-pax.sh has to be updated
                job.addArguments(dbcfg.name,
                                 zip_file,
                                 str(file_rucio_dataset),
                                 str(stash_gridftp_url),
                                 job_output,
                                 pax_version,
                                 'False')
                job.uses(determine_rse, link=Link.INPUT)
                job.uses(json_infile, link=Link.INPUT)
                job.uses(paxify, link=Link.INPUT)
                job.uses(job_output, link=Link.OUTPUT, transfer=False)
                dax.addJob(job)
                
                # update merge job
                merge_job.uses(job_output, link=Link.INPUT)
                merge_job.addArguments(job_output)
                dax.depends(parent=job, child=merge_job)
                
            # upload job  - runs on the submit host
            upload_job = Job("upload.sh")
            upload_job.addProfile(Profile(Namespace.HINTS, 'execution.site', 'local'))
            upload_job.uses(merged_root, link=Link.INPUT)
            upload_job.addArguments(dbcfg.name, 
                                    merged_root,
                                    config.base_dir())
            dax.addJob(upload_job)
            dax.depends(parent=merge_job, child=upload_job)
        
        # Write the DAX to stdout
        f = open(os.path.join(self._generated_dir(), 'dax.xml'), 'w')
        dax.writeXML(f)
        f.close()
       
        
    def _plan_and_submit(self):
        '''
        Call out to plan-env-helper.sh to start the workflow
        '''
        
        cmd = ' '.join([os.path.join(config.base_dir(), 'workflow/plan-env-helper.sh'),
                        config.pegasus_path(),
                        config.base_dir(),
                        config.work_dir(),
                        self._generated_dir(),
                        config.runs_dir(),
                        self._wf_id])
        shell = Shell(cmd, log_cmd = False, log_outerr = True)
        shell.run()
 

    def _generated_dir(self):
        return os.path.join(config.work_dir(), 'generated', self._wf_id)


    def _workflow_dir(self):
        return os.path.join(config.runs_dir(), self._wf_id)
      
    
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


    def _data_find_locations(self, dbcfg):
        '''
        Check Rucio and other locations to determine where input files exist
        '''

        rucio_dataset = None
        rses = []
        stash_raw_path = None
        
        for d in dbcfg.run_doc['data']:
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
        if os.path.exists(dbcfg.input_location):
            # also make sure the dir contains some zip files?
            stash_raw_path = dbcfg.input_location
            
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


    def _exclude_sites(self):
        '''
        Exclude sites from the user _dbcfgs file
        '''
    
        if not config.has_option('Outsource', 'exclude_sites'):
            return ''

        sites = [x.strip() for x in config.get('Outsource', 'exclude_sites').split(',')]
        if len(sites) == 0:
            return ''

        exprs = []
        for site in sites:
            exprs.append('GLIDEIN_Site =!= "%s"' %(site))
        return ' && '.join(exprs)


# This should be temporary hopefully, but just to get things working now
def write_json_file(doc, output):
    # take a run doc, write to json file
    with open(output, "w") as f:
        # fix doc so that all '|' become '.' in json
        fixed_doc = fix_keys(doc)
        json.dump(fixed_doc, f)

    if os.stat(output).st_uid == os.getuid():
        os.chmod(output, 0o777)


def fix_keys(dictionary):
    # Need this due to mongoDB syntax issues, I think
    for key, value in dictionary.items():
        if type(value) in [type(dict())]:
            dictionary[key] = fix_keys(value)
        if '|' in key:
            dictionary[key.replace('|', '.')] = dictionary.pop(key)
    return dictionary


if __name__ == '__main__':
    outsource = Outsource()
    outsource.submit_workflow()
