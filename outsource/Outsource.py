#!/usr/bin/env python

import json
import logging
import os
import re
import socket
import subprocess
import sys
import numpy as np

from pprint import pprint

from outsource.Shell import Shell
from outsource.Config import config, pegasus_path, base_dir, work_dir, runs_dir

logger = logging.getLogger("outsource")

# Pegasus environment
sys.path.insert(0, os.path.join(pegasus_path, 'lib64/python2.7/site-packages'))
os.environ['PATH'] = os.path.join(pegasus_path, 'bin') + ':' + os.environ['PATH']
from Pegasus.DAX3 import *


class Outsource:

    # Data availability to site selection map
    _rse_site_map = {
        'UC_OSG_USERDISK':    {'expr': 'GLIDEIN_Country == "US"'},
        'UC_DALI_USERDISK':   {},
        'CCIN2P3_USERDISK':   {'desired_sites': 'CCIN2P3',  'expr': 'GLIDEIN_Site == "CCIN2P3"'},
        'CNAF_TAPE_USERDISK': {},
        'CNAF_USERDISK':      {'desired_sites': 'CNAF',     'expr': 'GLIDEIN_Site == "CNAF"'},
        'LNGS_USERDISK':      {},
        'NIKHEF2_USERDISK':   {'desired_sites': 'NIKHEF',   'expr': 'GLIDEIN_Site == "NIKHEF"'},
        'NIKHEF_USERDISK':    {'desired_sites': 'NIKHEF',   'expr': 'GLIDEIN_Site == "NIKHEF"'},
        'SURFSARA_USERDISK':  {'desired_sites': 'SURFsara', 'expr': 'GLIDEIN_Site == "SURFsara"'},
        'WEIZMANN_USERDISK':  {'desired_sites': 'Weizmann', 'expr': 'GLIDEIN_Site == "Weizmann"'},
    }

    def __init__(self, dbcfgs, debug=False):
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
        if debug:
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
            logger.error("Workflow already exists at {path} . Exiting.".format(path=self._workflow_dir()))
            return

        # work dirs
        try:
            os.makedirs(self._generated_dir(), 0o755)
        except OSError:
            pass
        try:
            os.makedirs(runs_dir, 0o755)
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
        notification_email = ''
        if config.has_option('Outsource', 'notification_email'):
            notification_email = config.get('Outsource', 'notification_email')
        dax.invoke('start', base_dir + '/workflow/events/wf-start ' + notification_email)
        dax.invoke('at_end', base_dir + '/workflow/events/wf-end ' + notification_email)
        
        # Add executables to the DAX-level replica catalog
        
        pre_flight = Executable(name='pre-flight', arch='x86_64', installed=False)
        pre_flight.addPFN(PFN('file://' + base_dir + '/workflow/pre-flight-wrapper.sh', 'local'))
        dax.addExecutable(pre_flight)

        wrapper = Executable(name='strax-wrapper', arch='x86_64', installed=False)
        wrapper.addPFN(PFN('file://' + base_dir + '/workflow/strax-wrapper.sh', 'local'))
        #wrapper.addProfile(Profile(Namespace.PEGASUS, 'clusters.size', 10))
        dax.addExecutable(wrapper)

        combine = Executable(name='combine-wrapper.sh', arch='x86_64', installed=False)
        combine.addPFN(PFN('file://' + base_dir + '/workflow/combine-wrapper.sh', 'local'))
        dax.addExecutable(combine)
        
        upload_wrapper = Executable(name='upload.sh', arch='x86_64', installed=False)
        upload_wrapper.addPFN(PFN('file://' + base_dir + '/workflow/upload-wrapper.sh', 'local'))
        dax.addExecutable(upload_wrapper)


        # straxify is what processes the data. Gets called by the executable run-pax.sh
        straxify = File('runstrax.py')
        straxify.addPFN(PFN('file://' + os.path.join(base_dir, 'workflow/runstrax.py'), 'local'))
        dax.addFile(straxify)

        # performs the combine
        combinepy = File('combine.py')
        combinepy.addPFN(PFN('file://' + os.path.join(base_dir, 'workflow/combine.py'), 'local'))
        dax.addFile(combinepy)

        uploadpy = File('upload.py')
        uploadpy.addPFN(PFN('file://' + os.path.join(base_dir, 'workflow/upload.py'), 'local'))
        dax.addFile(uploadpy)

        xenon_config = File('.xenon_config')
        xenon_config.addPFN(PFN('file://' + config.config_path, 'local'))
        dax.addFile(xenon_config)

        token = File('.dbtoken')
        token.addPFN(PFN('file://' + os.path.join(os.environ['HOME'], '.dbtoken'), 'local'))
        dax.addFile(token)

        for dbcfg in self._dbcfgs:
            
            logger.info('Adding run ' + str(dbcfg.number) + ' to the workflow')
        
            # figure our where input data exists
            rucio_dataset, rses = self._data_find_locations(dbcfg)
            if len(rses) == 0:
                raise RuntimeError('Unable to find a data location for ' + str(dbcfg.number))
            
            # determine the job requirements based on the data locations
            sites_expression, desired_sites = self._determine_target_sites(rses)

            requirements_base = 'HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org'
            # hs06_test_run limits the run to a set of compute nodes at UChicago with a known HS06 factor
            if config.has_option('Outsource', 'hs06_test_run') and \
               config.getboolean('Outsource', 'hs06_test_run') == True:
                requirements_base = requirements_base + ' && GLIDEIN_ResourceName == "MWT2" && regexp("uct2-c4[1-7]", Machine)'
            # general compute jobs
            requirements = requirements_base + ' && (' + sites_expression + ')'
            if self._exclude_sites():
                requirements = requirements + ' && (' + self._exclude_sites()  + ')'
            # map some jobs to US to limit data transfers - for example the combine jobs
            requirements_us = requirements_base + ' && GLIDEIN_Country == "US"'
            if self._exclude_sites():
                requirements_us = requirements_us + ' && (' + self._exclude_sites()  + ')'

            
            # pre flight - runs on the submit host!
            pre_flight_job = self._job('pre-flight', run_on_submit_node=True)
            pre_flight_job.addArguments(base_dir, str(dbcfg.number))
            dax.addJob(pre_flight_job)
            
            # Set up the combine job first - we can then add to that job inside the chunk file loop
            combine_job = self._job('combine-wrapper.sh', disk=50000)
            combine_job.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements_us))
            combine_job.addProfile(Profile(Namespace.CONDOR, 'priority', str(dbcfg.priority * 5)))
            combine_job.uses(combinepy, link=Link.INPUT)
            combine_job.uses(xenon_config, link=Link.INPUT)
            combined_output = '%06d-records_combined.tar.gz' % (dbcfg.number)
            combine_job.addArguments(str(dbcfg.number),
                                     'records',
                                     combined_output,
                                     dbcfg.strax_context
                                    )
            combine_job.uses(combined_output, link=Link.OUTPUT)
            dax.addJob(combine_job)
            
            # add jobs, one for each input file
            # TODO is there a DB query we can do instead?
            chunk_list = self._data_find_chunks(rucio_dataset)

            njobs = int(np.ceil(len(chunk_list) / dbcfg.chunks_per_job))

            for job_i in range(njobs):
                chunks = chunk_list[dbcfg.chunks_per_job*job_i:dbcfg.chunks_per_job*(job_i + 1)]
                chunk_str = " ".join([str(c) for c in chunks])

                logger.debug(" ... adding job for chunk files: " + chunk_str)

                # output files
                job_output_tar = File('%06d-output-%04d.tar.gz' % (dbcfg.number, job_i))
                # do we already have a local copy?
                job_output_tar_local_path = os.path.join(work_dir, 'outputs', self._wf_id, self._wf_id, 
                                                         job_output_tar.name)
                if os.path.isfile(job_output_tar_local_path):
                    logger.info(" ... local copy found at: " + job_output_tar_local_path)
                    job_output_tar.addPFN(PFN('file://' + job_output_tar_local_path, 'local'))
                    dax.addFile(job_output_tar)
            
                # Add job
                job = self._job(name='strax-wrapper')
                if desired_sites and len(desired_sites) > 0:
                    # give a hint to glideinWMS for the sites we want (mostly useful for XENONVO in Europe)
                    job.addProfile(Profile(Namespace.CONDOR, '+XENON_DESIRED_Sites', '"' + desired_sites + '"'))
                job.addProfile(Profile(Namespace.CONDOR, 'requirements', requirements))
                job.addProfile(Profile(Namespace.CONDOR, 'priority', str(dbcfg.priority)))
                # Note that any changes to this argument list, also means strax-wrapper.sh has to be updated
                job.addArguments(str(dbcfg.number),
                                 dbcfg.strax_context,
                                 'raw_records',
                                 'records',
                                 job_output_tar,
                                 chunk_str)
                job.uses(straxify, link=Link.INPUT)
                job.uses(xenon_config, link=Link.INPUT)
                job.uses(job_output_tar, link=Link.OUTPUT, transfer=True)
                job.uses(token, link=Link.INPUT)
                dax.addJob(job)

                # all strax jobs depend on the pre-flight one
                dax.depends(parent=pre_flight_job, child=job)

                # update combine job
                combine_job.uses(job_output_tar, link=Link.INPUT, transfer=True)
                dax.depends(parent=job, child=combine_job)
                
            # upload job  - runs on the submit host
            upload_job = self._job("upload.sh", run_on_submit_node=True)
            upload_job.uses(combined_output, link=Link.INPUT)
            upload_job.uses(uploadpy, link=Link.INPUT)
            upload_job.uses(xenon_config, link=Link.INPUT)
            upload_job.addArguments(str(dbcfg.number),
                                    'records',
                                    'UC_OSG_USERDISK',
                                    dbcfg.strax_context
                                    )
            dax.addJob(upload_job)
            dax.depends(parent=combine_job, child=upload_job)
        
        # Write the DAX to stdout
        f = open(os.path.join(self._generated_dir(), 'dax.xml'), 'w')
        dax.writeXML(f)
        f.close()
       
        
    def _plan_and_submit(self):
        '''
        Call out to plan-env-helper.sh to start the workflow
        '''
        
        cmd = ' '.join([os.path.join(base_dir, 'workflow/plan-env-helper.sh'),
                        pegasus_path,
                        base_dir,
                        work_dir,
                        self._generated_dir(),
                        runs_dir,
                        self._wf_id])
        shell = Shell(cmd, log_cmd = False, log_outerr = True)
        shell.run()
 

    def _generated_dir(self):
        return os.path.join(work_dir, 'generated', self._wf_id)


    def _workflow_dir(self):
        return os.path.join(runs_dir, self._wf_id)
      
    
    def _validate_x509_proxy(self):
        '''
        ensure $HOME/user_cert exists and has enough time left
        '''
        logger.info('Verifying that the ~/user_cert proxy has enough lifetime')  
        min_valid_hours = 20
        shell = Shell('grid-proxy-info -timeleft -file ~/user_cert')
        shell.run()
        valid_hours = int(shell.get_outerr()) / 60 / 60
        if valid_hours < min_valid_hours:
            raise RuntimeError('User proxy is only valid for %d hours. Minimum required is %d hours.' \
                               %(valid_hours, min_valid_hours))


    def _job(self, name, run_on_submit_node=False, cores=1, memory=1700, disk=10000):
        '''
        Wrapper for a Pegasus job, also sets resource requirement profiles. Memory and
        disk units are in MBs.
        '''
        job = Job(name)
        job.addProfile(Profile(Namespace.CONDOR, 'request_cpus', str(cores)))
        job.addProfile(Profile(Namespace.CONDOR, 'request_disk', str(disk)))

        # increase memory if the first attempt fails
        memory = 'ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, %d, %d)' \
                 %(memory, memory * 3)
        job.addProfile(Profile(Namespace.CONDOR, 'request_memory', memory))

        if run_on_submit_node: 
            job.addProfile(Profile(Namespace.HINTS, 'execution.site', 'local'))

        return job


    def _data_find_locations(self, dbcfg):
        '''
        Check Rucio and other locations to determine where input files exist
        '''

        rucio_dataset = None
        rses = []
    
        for d in dbcfg.run_doc['data']:
            if d['host'] == 'rucio-catalogue' and d['status'] == 'transferred' and d['type'] == 'raw_records':
                    rucio_dataset = d['did']
        
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
        
        if len(rses) > 0:
            logger.info('Found replicas at: ' + ', '.join(rses))
        return rucio_dataset, rses
    
    
    def _data_find_chunks(self, rucio_dataset):
        '''
        Look up which chunk files are in the dataset - return a dict where the keys are the
        chunks, and the values a dict of locations
        '''
        chunks_files = []

        logger.info('Querying Rucio for files in the data set ' + rucio_dataset)
        # TODO use rucio python API or admix
        out = subprocess.Popen(["rucio", "list-file-replicas", rucio_dataset], stdout=subprocess.PIPE).stdout.read()
        out = str(out).split("\\n")
        files = set([l.split(" ")[3] for l in out if '---' not in l and 'xnt' in l])
        for i, f in enumerate(sorted([f for f in files if 'json' not in f])):
            chunks_files.append(i)
        
        return chunks_files


    def _determine_target_sites(self, rses):
        '''
        Given a list of RSEs, limit the runs for sites for those locations
        '''
        
        # want a temporary copy so we can modify it
        my_rses = rses.copy()
        
        exprs = []
        sites = []
        for rse in my_rses:
            if rse in self._rse_site_map:
                if 'expr' in self._rse_site_map[rse]:
                    exprs.append(self._rse_site_map[rse]['expr'])
                if 'desired_sites' in self._rse_site_map[rse]:
                    sites.append(self._rse_site_map[rse]['desired_sites'])
            else:
                raise RuntimeError('We do not know how to handle the RSE: ' + rse)

        final_expr = ' || '.join(exprs)
        desired_sites = ','.join(sites)
        logger.info('Site expression from RSEs list: ' + final_expr)
        logger.info('XENON_DESIRED_Sites from RSEs list (mostly used for European sites): ' + desired_sites)
        return final_expr, desired_sites


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
