#!/usr/bin/env python3

import json
import getpass
import logging
import os
import re
import sys
import numpy as np

from pprint import pprint

from utilix import db
from outsource.Config import config, pegasus_path, base_dir, work_dir, runs_dir
from outsource.Shell import Shell

from admix.interfaces.rucio_summoner import RucioSummoner

# Pegasus environment
sys.path.insert(0, os.path.join(pegasus_path, 'lib64/python3.6/site-packages'))
os.environ['PATH'] = os.path.join(pegasus_path, 'bin') + ':' + os.environ['PATH']
from Pegasus.api import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

rc = RucioSummoner()

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
        formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
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

        wf = self._generate_workflow()

        self._plan_and_submit(wf)
    
    def _generate_workflow(self):
        '''
        Use the Pegasus API to build an abstract graph of the workflow
        '''
        
        # Create a abstract dag
        wf = Workflow('xenonnt')
        tc = TransformationCatalog()
        rc = ReplicaCatalog()
        sc = self._generate_sc()
        
        # event callouts
        notification_email = ''
        if config.has_option('Outsource', 'notification_email'):
            notification_email = config.get('Outsource', 'notification_email')
        wf.add_shell_hook(EventType.START, pegasus_path + '/share/pegasus/notification/email -t ' + notification_email)
        wf.add_shell_hook(EventType.END, pegasus_path + '/share/pegasus/notification/email -t ' + notification_email)

        # add executables to the wf-level transformation catalog
        for fname in [
                'combine-wrapper.sh',
                'pre-flight-wrapper.sh',
                'strax-wrapper.sh',
                ]:
            t = Transformation(fname,
                               site='local',
                               pfn='file://' + base_dir + '/workflow/' + fname,
                               is_stageable=True)
            tc.add_transformations(t)

        # scripts some exectuables might need
        straxify = File('runstrax.py')
        rc.add_replica('local', 'runstrax.py', 'file://' + base_dir + '/workflow/runstrax.py')
        
        combinepy = File('combine.py')
        rc.add_replica('local', 'combine.py', 'file://' + base_dir + '/workflow/combine.py')
        
        uploadpy = File('upload.py')
        rc.add_replica('local', 'upload.py', 'file://' + base_dir + '/workflow/upload.py')

        # add common data files to the replica catalog
        xenon_config = File('.xenon_config')
        rc.add_replica('local', '.xenon_config', 'file://' + config.config_path)

        token = File('.dbtoken')
        rc.add_replica('local', '.dbtoken', 'file://' + os.path.join(os.environ['HOME'], '.dbtoken'))

        # remove compute jobs after 3 hours
        # periodic_remove = "((JobStatus == 2) & ((CurrentTime - EnteredCurrentStatus) > (60 * 60 * 3)))"

        requirements_base = 'HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org'
        requirements_us = requirements_base + ' && GLIDEIN_Country == "US"'
        requirements_for_combine = requirements_base + '&& GLIDEIN_ResourceName == "MWT2" && ' \
                                                       '!regexp("campuscluster.illinois.edu", Machine)'

        for dbcfg in self._dbcfgs:

            # check if this run needs to be processed
            if len(dbcfg.needs_processed) > 0:
                logger.info('Adding run ' + str(dbcfg.number) + ' to the workflow')
            else:
                logger.info(f"Run {dbcfg.number} is already processed with context {dbcfg.strax_context} and "
                             f"straxen version {dbcfg.straxen_version}")
                continue

            combine_jobs = []

            # get dtypes to process
            for dtype_i, dtype in enumerate(dbcfg.needs_processed):
                logger.info(f"|-----> adding {dtype}")
                rses = dbcfg.rses[dtype]
                if len(rses) == 0:
                    if dtype == 'raw_records':
                        raise RuntimeError(f'Unable to find a raw records location for {dbcfg.number}')
                    else:
                        logger.debug(f'No data found for {dbcfg.number} {dtype}... '
                                     f'hopefully those will be created by the workflow')

            
                # determine the job requirements based on the data locations
                sites_expression, desired_sites = self._determine_target_sites(rses)

                # hs06_test_run limits the run to a set of compute nodes at UChicago with a known HS06 factor
                if config.has_option('Outsource', 'hs06_test_run') and \
                   config.getboolean('Outsource', 'hs06_test_run') == True:
                    requirements_base = requirements_base + ' && GLIDEIN_ResourceName == "MWT2" && regexp("uct2-c4[1-7]", Machine)'

                # general compute jobs
                requirements = requirements_base + (' && (' + sites_expression + ')') * (len(sites_expression) > 0)
                if self._exclude_sites():
                    requirements = requirements + ' && (' + self._exclude_sites()  + ')'
                    requirements_us = requirements_us + ' && (' + self._exclude_sites()  + ')'

                # pre flight - runs on the submit host!
                pre_flight_job = self._job('pre-flight-wrapper.sh', run_on_submit_node=True)
                pre_flight_job.add_args(base_dir, str(dbcfg.number))
                wf.add_jobs(pre_flight_job)

                # Set up the combine job first - we can then add to that job inside the chunk file loop
                combine_job = self._job('combine-wrapper.sh', disk=50000)
                combine_job.add_profiles(Namespace.CONDOR, 'requirements', requirements_for_combine)
                combine_job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))
                combine_job.add_inputs(combinepy, uploadpy, xenon_config)
                combine_job.add_args(str(dbcfg.number),
                                     dtype,
                                     dbcfg.strax_context,
                                     config.get('Outsource', '%s_rse' % dtype,
                                                fallback='UC_OSG_USERDISK'),
                                     dbcfg.rucio_arg,
                                     dbcfg.rundb_arg
                                    )

                wf.add_jobs(combine_job)
                combine_jobs.append(combine_job)

                # add jobs, one for each input file
                n_chunks = dbcfg.nchunks(dtype)
                # hopefully temporary
                if n_chunks is None:
                    scope = 'xnt_%06d' % dbcfg.number
                    dataset = "raw_records-%s" % (db.get_hash(dbcfg.strax_context, 'raw_records', dbcfg.straxen_version))
                    did =  "%s:%s" % (scope, dataset)
                    chunk_list = self._data_find_chunks(did)
                    n_chunks = len(chunk_list)

                chunk_list = np.arange(n_chunks)
                njobs = int(np.ceil(n_chunks / dbcfg.chunks_per_job))

                # Loop over the chunks
                for job_i in range(njobs):
                    chunks = chunk_list[dbcfg.chunks_per_job*job_i:dbcfg.chunks_per_job*(job_i + 1)]
                    chunk_str = " ".join([str(c) for c in chunks])

                    logger.debug(" ... adding job for chunk files: " + chunk_str)

                    # output files
                    job_output_tar = File('%06d-output-%s-%04d.tar.gz' % (dbcfg.number, dtype, job_i))
                    # do we already have a local copy?
                    job_output_tar_local_path = os.path.join(work_dir, 'outputs', self._wf_id, str(job_output_tar))
                    if os.path.isfile(job_output_tar_local_path):
                        logger.info(" ... local copy found at: " + job_output_tar_local_path)
                        rc.add_replica('local', job_output_tar, 'file://' + job_output_tar_local_path)

                    # Add job
                    job = self._job(name='strax-wrapper.sh')
                    if desired_sites and len(desired_sites) > 0:
                        # give a hint to glideinWMS for the sites we want (mostly useful for XENONVO in Europe)
                        job.add_profiles(Namespace.CONDOR, '+XENON_DESIRED_Sites', '"' + desired_sites + '"')
                    job.add_profiles(Namespace.CONDOR, 'requirements', requirements)
                    job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))
                    #job.add_profiles(Namespace.CONDOR, 'periodic_remove', periodic_remove)

                    # Note that any changes to this argument list, also means strax-wrapper.sh has to be updated
                    job.add_args(str(dbcfg.number),
                                     dbcfg.strax_context,
                                     dtype,
                                     job_output_tar,
                                     chunk_str)
                    job.add_inputs(straxify, xenon_config, token)
                    job.add_outputs(job_output_tar, stage_out=True)
                    wf.add_jobs(job)

                    # all strax jobs depend on the pre-flight one
                    wf.add_dependency(job, parents=[pre_flight_job])

                    # update combine job
                    combine_job.add_inputs(job_output_tar)
                    wf.add_dependency(job, children=[combine_job])

                    # if there are multiple levels to the workflow, need to have current strax-wrapper depend on
                    # previous combine
                    if dtype_i > 0:
                        wf.add_dependency(job, parents=[combine_jobs[dtype_i - 1]])


        # Write the wf to stdout
        os.chdir(self._generated_dir())
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        wf.add_site_catalog(sc)
        wf.write()

        return wf

    def _plan_and_submit(self, wf):
        '''
        submit the workflow
        '''

        os.chdir(self._generated_dir())
        wf.plan(conf=base_dir + '/workflow/pegasus.conf',
                submit=False,
                sites=['condorpool'],
                staging_sites={'condorpool': 'staging'},
                output_sites=['output'],
                dir=runs_dir,
                relative_dir=self._wf_id
               )


    def _generated_dir(self):
        return os.path.join(work_dir, 'generated', self._wf_id)


    def _workflow_dir(self):
        return os.path.join(runs_dir, self._wf_id)
      
    
    def _validate_x509_proxy(self):
        '''
        ensure $HOME/user_cert exists and has enough time left
        '''
        logger.debug('Verifying that the ~/user_cert proxy has enough lifetime')
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

        if run_on_submit_node: 
            job.add_selector_profile(execution_site='local')
            # no other attributes on a local job
            return job

        job.add_profiles(Namespace.CONDOR, 'request_cpus', str(cores))
        job.add_profiles(Namespace.CONDOR, 'request_disk', str(disk))

        # increase memory if the first attempt fails
        memory = 'ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, %d, %d)' \
                 %(memory, memory * 3)
        job.add_profiles(Namespace.CONDOR, 'request_memory', memory)

        return job


    def _data_find_chunks(self, rucio_dataset):
        '''
        Look up which chunk files are in the dataset - return a dict where the keys are the
        chunks, and the values a dict of locations
        '''

        logger.debug('Querying Rucio for files in the data set ' + rucio_dataset)
        result = rc.ListFiles(rucio_dataset)
        chunks_files = [f['name'] for f in result if 'json' not in f['name']]
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
        #if len(sites) == 0 and len(exprs) == 0:
        #    raise RuntimeError(f'no valid sites in {my_rses}')

        final_expr = ' || '.join(exprs)
        desired_sites = ','.join(sites)
        logger.debug('Site expression from RSEs list: ' + final_expr)
        logger.debug('XENON_DESIRED_Sites from RSEs list (mostly used for European sites): ' + desired_sites)
        return final_expr, desired_sites


    def _exclude_sites(self):
        '''
        Exclude sites from the user _dbcfgs file
        '''
    
        if not config.has_option('Outsource', 'exclude_sites'):
            return ''

        sites = config.get_list('Outsource', 'exclude_sites')

        exprs = []
        for site in sites:
            exprs.append('GLIDEIN_Site =!= "%s"' %(site))
        return ' && '.join(exprs)


    def _generate_sc(self):

        sc = SiteCatalog()

        # local site - this is the submit host
        local = Site("local")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path='{}/scratch/{}'.format(work_dir, self._wf_id))
        scratch_dir.add_file_servers(FileServer('file:///{}/scratch/{}'.format(work_dir, self._wf_id), Operation.ALL))
        storage_dir = Directory(Directory.LOCAL_STORAGE, path='{}/outputs/{}'.format(work_dir, self._wf_id))
        storage_dir.add_file_servers(FileServer('file:///{}/outputs/{}'.format(work_dir, self._wf_id), Operation.ALL))
        local.add_directories(scratch_dir, storage_dir)

        local.add_profiles(Namespace.ENV, HOME=os.environ['HOME'])
        local.add_profiles(Namespace.ENV, GLOBUS_LOCATION='')
        local.add_profiles(Namespace.ENV, PATH='/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/bin:/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/condabin:/usr/bin:/bin')
        local.add_profiles(Namespace.ENV, LD_LIBRARY_PATH='/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/lib64:/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/lib')
        local.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ['USER'])
        local.add_profiles(Namespace.ENV, X509_USER_PROXY=os.environ['HOME'] + '/user_cert')
        local.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")

        # output site
        output = Site("output")
        _outputdir = f'/xenon/xenonnt/processing-scratch/outputs/{getpass.getuser()}'
        output_dir = Directory(Directory.LOCAL_STORAGE, path=_outputdir)
        output_dir.add_file_servers(FileServer('gsiftp://ceph-gridftp2.grid.uchicago.edu:2811/cephfs/srm' + _outputdir,
                                               Operation.ALL))
        output.add_directories(output_dir)

        # staging site
        staging = Site("staging")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path='/xenon_dcache/workflow_scratch/{}'.format(getpass.getuser()))
        scratch_dir.add_file_servers(FileServer('gsiftp://xenon-gridftp.grid.uchicago.edu:2811/xenon/workflow_scratch/{}'.format(getpass.getuser()), Operation.ALL))
        staging.add_directories(scratch_dir)

        # condorpool
        condorpool = Site("condorpool")
        condorpool.add_profiles(Namespace.PEGASUS, style='condor')
        condorpool.add_profiles(Namespace.CONDOR, universe='vanilla')

        condorpool.add_profiles(Namespace.CONDOR, key='+SingularityImage',
                                value='"/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:latest"')

        # ignore the site settings - the container will set all this up inside
        condorpool.add_profiles(Namespace.ENV, OSG_LOCATION='')
        condorpool.add_profiles(Namespace.ENV, GLOBUS_LOCATION='')
        condorpool.add_profiles(Namespace.ENV, PYTHONPATH='')
        condorpool.add_profiles(Namespace.ENV, PERL5LIB='')
        condorpool.add_profiles(Namespace.ENV, LD_LIBRARY_PATH='')

        condorpool.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ['USER'])
        condorpool.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")

        sc.add_sites(local,
                     staging,
                     output,
                     condorpool)

        return sc

