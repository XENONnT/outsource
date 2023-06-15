#!/usr/bin/env python3

import json
import getpass
import logging
import os
import re
import sys
import numpy as np
from tqdm import tqdm
import time
import shutil
import cutax
from datetime import datetime
from straxen import __version__ as straxen_version
import straxen
from admix.utils import RAW_DTYPES

from pprint import pprint


from utilix.rundb import DB, xent_collection, cmt_global_valid_range, cmt_local_valid_range
from outsource.Config import config, pegasus_path, base_dir, work_dir, runs_dir
from outsource.Shell import Shell
from outsource.RunConfig import DEPENDS_ON, DBConfig


# Pegasus environment
# sys.path.insert(0, os.path.join(pegasus_path, 'lib64/python3.6/site-packages'))
# os.environ['PATH'] = os.path.join(pegasus_path, '../bin') + ':' + os.environ['PATH']
from Pegasus.api import *

#logging.basicConfig(level=config.logging_level)
logger = logging.getLogger()

DEFAULT_IMAGE = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:latest"

db = DB()

PER_CHUNK_DTYPES = ['records', 'peaklets', 'hitlets_nv', 'afterpulses', 'led_calibration']


class Outsource:

    # Data availability to site selection map
    _rse_site_map = {
        'UC_OSG_USERDISK':    {'expr': 'GLIDEIN_Country == "US"'},
        'UC_DALI_USERDISK':   {'expr': 'GLIDEIN_Country == "US"'},
        'CCIN2P3_USERDISK':   {'desired_sites': 'CCIN2P3',  'expr': 'GLIDEIN_Site == "CCIN2P3"'},
        'CNAF_TAPE_USERDISK': {},
        'CNAF_USERDISK':      {'desired_sites': 'CNAF',     'expr': 'GLIDEIN_Site == "CNAF"'},
        'LNGS_USERDISK':      {},
        'NIKHEF2_USERDISK':   {'desired_sites': 'NIKHEF',   'expr': 'GLIDEIN_Site == "NIKHEF"'},
        'NIKHEF_USERDISK':    {'desired_sites': 'NIKHEF',   'expr': 'GLIDEIN_Site == "NIKHEF"'},
        'SURFSARA_USERDISK':  {'desired_sites': 'SURFsara', 'expr': 'GLIDEIN_Site == "SURFsara"'},
        'WEIZMANN_USERDISK':  {'desired_sites': 'Weizmann', 'expr': 'GLIDEIN_Site == "Weizmann"'},
        'SDSC_USERDISK': {'expr': 'GLIDEIN_ResourceName == "SDSC-Expanse"'}
    }

    # transformation map (high level name -> script)
    _transformations_map = {'combine': 'combine-wrapper.sh',
                            'download': 'strax-wrapper.sh',
                            'records': 'strax-wrapper.sh',
                            'peaklets': 'strax-wrapper.sh',
                            'events': 'strax-wrapper.sh',
                            'peaksHE': 'strax-wrapper.sh',
                            'nv_hitlets': 'strax-wrapper.sh',
                            'nv_events':  'strax-wrapper.sh',
                            'mv': 'strax-wrapper.sh',
                            'ap': 'strax-wrapper.sh',
                            'led': 'strax-wrapper.sh'
                           }

    # jobs details for a given datatype
    job_kwargs = {'records': dict(name='records', memory=3000),
                  'peaklets': dict(name='peaklets', memory=4000),
                  'event_info_double': dict(name='events', memory=12000, disk=15000, cores=1),
                  'peak_basics_he': dict(name='peaksHE', memory=5000, cores=1),
                  'hitlets_nv': dict(name='nv_hitlets', memory=5000),
                  'events_nv': dict(name='nv_events', memory=8000, disk=20000),
                  'events_mv': dict(name='mv', memory=1700),
                  'afterpulses': dict(name='ap', memory=3000),
                  'led_calibration': dict(name='led', memory=4000)
                  }

    def __init__(self, runlist, context_name,
                 #cmt_version='global_v5',
                 wf_id=None, force_rerun=False,
                 upload_to_rucio=True, update_db=True,
                 debug=False, image=DEFAULT_IMAGE):
        '''
        Creates a new Outsource object. Specifying a list of DBConfig objects required.
        '''

        if not isinstance(runlist, list):
            raise RuntimeError('Outsource expects a list of DBConfigs to run')

        self._runlist = runlist

        # setup context
        self.context_name = context_name
        self.context = getattr(cutax.contexts, context_name)(cut_list=None)

        # Load from xenon_config
        self.xsede = config.getboolean('Outsource', 'use_xsede', fallback=False)
        self.debug = debug
        self.singularity_image = image
        self._initial_dir = os.getcwd()
        self.force_rerun = force_rerun
        self.upload_to_rucio = upload_to_rucio
        self.update_db = update_db

        # logger
        console = logging.StreamHandler()
        # default log level - make logger/console match
        logger.setLevel(logging.WARNING)
        console.setLevel(logging.WARNING)
        # debug - where to get this from?
        # if debug:
        #     logger.setLevel(logging.DEBUG)
        #     console.setLevel(logging.DEBUG)
        # formatter
        formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        console.setFormatter(formatter)
        if (logger.hasHandlers()):
            logger.handlers.clear()
        logger.addHandler(console)
        
        # Determine a unique id for the workflow. If none passed, looks at the dbconfig.
        # If only one dbconfig is provided, use the workflow id of that object.
        # If more than one is provided, make one up.
        if wf_id:
            self._wf_id = wf_id
        else:
            if len(self._runlist) == 1:
                self._wf_id = f'{self._runlist[0]:06d}'
            else:
                self._wf_id = datetime.now().strftime("%Y-%m-%d")

        self.wf_dir = os.path.join(runs_dir,
                                   f'straxen_v{straxen_version}',
                                   context_name,
                                   self._wf_id)

        self.dtype_valid_cache = {}

    def submit_workflow(self, force=False):
        '''
        Main interface to submitting a new workflow
        '''

        # does workflow already exist?
        workflow_path = self.wf_dir
        if os.path.exists(workflow_path):
            if force:
                logger.warning(f"Overwriting workflow at {workflow_path}. CTRL C now to stop.")
                time.sleep(10)
                shutil.rmtree(workflow_path)
            else:
                logger.error(f"Workflow already exists at {workflow_path} . Exiting.")
                return

        # work dirs
        try:
            os.makedirs(self._generated_dir(), 0o755)
        except OSError:
            pass
        try:
            os.makedirs(runs_dir, 0o755) #  0o755 means read/write/execute for owner, read/execute for everyone else
        except OSError:
            pass
        
        # ensure we have a proxy with enough time left
        self._validate_x509_proxy()

        # generate the workflow
        wf = self._generate_workflow()

        # submit the workflow
        self._plan_and_submit(wf)

        # return to initial dir
        os.chdir(self._initial_dir)
    
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
        for human, script in self._transformations_map.items():
            t = Transformation(human,
                               site='local',
                               pfn='file://' + base_dir + '/workflow/' + script,
                               is_stageable=True)
            tc.add_transformations(t)

        # scripts some exectuables might need

        straxify = File('runstrax.py')
        rc.add_replica('local', 'runstrax.py', 'file://' + base_dir + '/workflow/runstrax.py')
        
        combinepy = File('combine.py')
        rc.add_replica('local', 'combine.py', 'file://' + base_dir + '/workflow/combine.py')

        # add common data files to the replica catalog
        xenon_config = File('.xenon_config')
        rc.add_replica('local', '.xenon_config', 'file://' + config.config_path)

        token = File('.dbtoken')
        rc.add_replica('local', '.dbtoken', 'file://' + os.path.join(os.environ['HOME'], '.dbtoken'))

        # cutax tarball
        cutax_tarball = File('cutax.tar.gz')
        if 'CUTAX_LOCATION' not in os.environ:
            logger.warning("No CUTAX_LOCATION env variable found. Using the latest by default!")
            tarball_path = '/xenon/xenonnt/software/cutax/latest.tar.gz'
        else:
            tarball_path = os.environ['CUTAX_LOCATION'].replace('.', '-') + '.tar.gz'
            logger.warning(f"Using cutax: {tarball_path}")

        rc.add_replica('local', 'cutax.tar.gz', tarball_path)

        iterator = self._runlist if len(self._runlist) == 1 else tqdm(self._runlist)

        # keep track of what runs we submit, useful for bookkeeping
        runlist = []

        for run in iterator:

            dbcfg = DBConfig(run, self.context, force_rerun=self.force_rerun)

            # check if this run needs to be processed
            if len(dbcfg.needs_processed) > 0:
                logger.debug('Adding run ' + str(dbcfg.number) + ' to the workflow')
            else:
                logger.debug(f"Run {dbcfg.number} is already processed with context {self.context_name}")
                continue

            requirements_base = 'HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org'
            # should we use XSEDE?
            # if self.xsede:
            #     requirements_base += ' && GLIDEIN_ResourceName == "SDSC-Expanse"'
            requirements_us = requirements_base + ' && GLIDEIN_Country == "US"'
            # requirements_for_highlevel = requirements_base + '&& GLIDEIN_ResourceName == "MWT2" && ' \
            #                                                  '!regexp("campuscluster.illinois.edu", Machine)'

            # will have combine jobs for all the PER_CHUNK_DTYPES we passed
            combine_jobs = {}

            # get dtypes to process
            for dtype_i, dtype in enumerate(dbcfg.needs_processed):
                # these dtypes need raw data
                if dtype in ['peaklets', 'peak_basics_he', 'hitlets_nv', 'events_mv',
                             'afterpulses', 'led_calibration'
                             ]:
                    # check that raw data exist for this run
                    if not all([dbcfg._raw_data_exists(raw_type=d) for d in DEPENDS_ON[dtype]]):
                        continue

                # can we process this dtype of this run?
                if dtype in self.dtype_valid_cache:
                    start_valid, end_valid = self.dtype_valid_cache[dtype]
                else:
                    start_valid, end_valid = self.dtype_validity(dtype)
                    self.dtype_valid_cache[dtype] = (start_valid, end_valid)

                if not start_valid < dbcfg.start < end_valid:
                    #print(f"Can't process {dtype} for Run {dbcfg.number}.")
                    continue

                logger.debug(f"|-----> adding {dtype}")
                if dbcfg.number not in runlist:
                    runlist.append(dbcfg.number)
                rses = dbcfg.rses[dtype]
                if len(rses) == 0:
                    if dtype == 'raw_records':
                        raise RuntimeError(f'Unable to find a raw records location for {dbcfg.number}')
                    else:
                        logger.debug(f'No data found for {dbcfg.number} {dtype}... '
                                     f'hopefully those will be created by the workflow')
            
                # determine the job requirements based on the data locations
                # for standalone downloads, only target us
                if dbcfg.standalone_download:
                    rses = ['UC_OSG_USERDISK']
                sites_expression, desired_sites = self._determine_target_sites(rses)

                # hs06_test_run limits the run to a set of compute nodes at UChicago with a known HS06 factor
                if config.has_option('Outsource', 'hs06_test_run') and \
                   config.getboolean('Outsource', 'hs06_test_run') == True:
                    requirements_base = requirements_base + ' && GLIDEIN_ResourceName == "MWT2" && regexp("uct2-c4[1-7]", Machine)'

                # general compute jobs
                _requirements_base = requirements_base if len(rses) > 0 else requirements_us
                requirements = _requirements_base + (' && (' + sites_expression + ')') * (len(sites_expression) > 0)

                if self._exclude_sites():
                    requirements = requirements + ' && (' + self._exclude_sites()  + ')'
                    requirements_us = requirements_us + ' && (' + self._exclude_sites()  + ')'

                if dtype in PER_CHUNK_DTYPES:
                    # Set up the combine job first - we can then add to that job inside the chunk file loop
                    # only need combine job for low-level stuff
                    combine_job = self._job('combine', disk=30000)
                    combine_job.add_profiles(Namespace.CONDOR, 'requirements', requirements)
                    combine_job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))
                    combine_job.add_inputs(combinepy, xenon_config, cutax_tarball)
                    combine_output_tar_name = f'{dbcfg.number:06d}-{dtype}-combined.tar.gz'
                    combine_output_tar = File(combine_output_tar_name)
                    combine_job.add_outputs(combine_output_tar, stage_out=True)
                    combine_job.add_args(str(dbcfg.number),
                                         dtype,
                                         self.context_name,
                                         combine_output_tar_name,
                                         str(self.upload_to_rucio).lower(),
                                         str(self.update_db).lower()
                                        )

                    wf.add_jobs(combine_job)
                    combine_jobs[dtype] = (combine_job, combine_output_tar)

                    # add jobs, one for each input file
                    n_chunks = dbcfg.nchunks(dtype)

                    # hopefully temporary
                    if n_chunks is None:
                        scope = 'xnt_%06d' % dbcfg.number
                        dataset = "raw_records-%s" % dbcfg.hashes['raw_records']
                        did = "%s:%s" % (scope, dataset)
                        chunk_list = self._data_find_chunks(did)
                        n_chunks = len(chunk_list)

                    chunk_list = np.arange(n_chunks)
                    njobs = int(np.ceil(n_chunks / dbcfg.chunks_per_job))

                    # Loop over the chunks
                    for job_i in range(njobs):
                        chunks = chunk_list[dbcfg.chunks_per_job*job_i:dbcfg.chunks_per_job*(job_i + 1)]
                        chunk_str = " ".join([str(c) for c in chunks])

                        logger.debug(" ... adding job for chunk files: " + chunk_str)

                        # standalone download is a special case where we download data from rucio first, which
                        # is useful for testing and when using dedicated clusters with storage such as XSEDE
                        data_tar = None
                        download_job = None
                        if dbcfg.standalone_download:
                            data_tar = File('%06d-data-%s-%04d.tar.gz' % (dbcfg.number, dtype, job_i))
                            download_job = self._job(name='download')
                            download_job.add_profiles(Namespace.CONDOR, 'requirements', requirements)
                            download_job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))

                            download_job.add_args(str(dbcfg.number),
                                         self.context_name,
                                         dtype,
                                         data_tar,
                                         'download-only',
                                         str(self.upload_to_rucio).lower(),
                                         str(self.update_db).lower(),
                                         chunk_str,
                                        )
                            download_job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                            download_job.add_outputs(data_tar, stage_out=False)
                            wf.add_jobs(download_job)
                            #wf.add_dependency(download_job, parents=[pre_flight_job])

                        # output files
                        job_output_tar = File('%06d-output-%s-%04d.tar.gz' % (dbcfg.number, dtype, job_i))
                        # do we already have a local copy?
                        job_output_tar_local_path = os.path.join(work_dir, 'outputs', self._wf_id, str(job_output_tar))
                        if os.path.isfile(job_output_tar_local_path):
                            logger.info(" ... local copy found at: " + job_output_tar_local_path)
                            rc.add_replica('local', job_output_tar, 'file://' + job_output_tar_local_path)

                        # Add job

                        job = self._job(**self.job_kwargs[dtype])
                        if desired_sites and len(desired_sites) > 0:
                            # give a hint to glideinWMS for the sites we want (mostly useful for XENONVO in Europe)
                            job.add_profiles(Namespace.CONDOR, '+XENON_DESIRED_Sites', '"' + desired_sites + '"')
                        job.add_profiles(Namespace.CONDOR, 'requirements', requirements)
                        job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))
                        #job.add_profiles(Namespace.CONDOR, 'periodic_remove', periodic_remove)

                        job.add_args(str(dbcfg.number),
                                     self.context_name,
                                     dtype,
                                     job_output_tar,
                                     'false', # if not dbcfg.standalone_download else 'no-download',
                                     str(self.upload_to_rucio).lower(),
                                     str(self.update_db).lower(),
                                     chunk_str,
                                     )

                        job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                        job.add_outputs(job_output_tar, stage_out=True)
                        wf.add_jobs(job)

                        # all strax jobs depend on the pre-flight or a download job
                        if download_job:
                            job.add_inputs(data_tar)
                            wf.add_dependency(job, parents=[download_job])
                        else:
                            pass
                            # wf.add_dependency(job, parents=[pre_flight_job])

                        # update combine job
                        combine_job.add_inputs(job_output_tar)
                        wf.add_dependency(job, children=[combine_job])

                        parent_combines = []
                        for d in DEPENDS_ON[dtype]:
                            if combine_jobs.get(d):
                                parent_combines.append(combine_jobs.get(d))

                        if len(parent_combines):
                            wf.add_dependency(job, parents=parent_combines)

                else:
                    # high level data.. we do it all on one job
                    # Add job
                    job = self._job(**self.job_kwargs[dtype])
                    # https://support.opensciencegrid.org/support/solutions/articles/12000028940-working-with-tensorflow-gpus-and-containers
                    requirements_for_highlevel = requirements + ' && (HAS_AVX2 || HAS_AVX)'
                    job.add_profiles(Namespace.CONDOR, 'requirements', requirements_for_highlevel)
                    job.add_profiles(Namespace.CONDOR, 'priority', str(dbcfg.priority))

                    # Note that any changes to this argument list, also means strax-wrapper.sh has to be updated
                    job.add_args(str(dbcfg.number),
                                 self.context_name,
                                 dtype,
                                 'no_output_here',
                                 'false', #if not dbcfg.standalone_download else 'no-download',
                                 str(self.upload_to_rucio).lower(),
                                 str(self.update_db).lower()
                                 )

                    job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                    wf.add_jobs(job)

                    # if there are multiple levels to the workflow, need to have current strax-wrapper depend on
                    # previous combine

                    for d in DEPENDS_ON[dtype]:
                        if d in combine_jobs:
                            cj, cj_output = combine_jobs[d]
                            wf.add_dependency(job, parents=[cj])
                            job.add_inputs(cj_output)

        # Write the wf to stdout
        os.chdir(self._generated_dir())
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        wf.add_site_catalog(sc)
        wf.write()

        # save the runlist
        np.savetxt('runlist.txt', runlist, fmt='%0d')

        return wf

    def _plan_and_submit(self, wf):
        '''
        submit the workflow
        '''

        os.chdir(self._generated_dir())
        wf.plan(conf=base_dir + '/workflow/pegasus.conf',
                submit=not self.debug,
                sites=['condorpool'],
                staging_sites={'condorpool': 'staging'},
                output_sites=None,
                dir=os.path.dirname(self.wf_dir),
                relative_dir=self._wf_id
               )
        # copy the runlist file
        shutil.copy('runlist.txt', self.wf_dir)

        print(f"Worfklow written to \n\n\t{self.wf_dir}\n\n")

    def _generated_dir(self):
        return os.path.join(work_dir, 'generated', self._wf_id)


    def _workflow_dir(self):
        return os.path.join(self.wf_dir, self._wf_id)
      
    
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


        # increase memory/disk if the first attempt fails
        memory = f"ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, {memory}, (DAGNodeRetry + 1)*{memory})"

        disk_str = f"ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, {disk}, {2*disk})"

        job.add_profiles(Namespace.CONDOR, 'request_disk', disk_str)
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

        # make sure we do not request XENON1T sites we do not need
        if len(sites) == 0:
            sites.append('NONE')

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
        #local.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")
        local.add_profiles(Namespace.ENV, RUCIO_ACCOUNT='production')

        # staging site
        staging = Site("staging")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path='/xenon_dcache/workflow_scratch/{}'.format(getpass.getuser()))
        scratch_dir.add_file_servers(FileServer('gsiftp://xenon-gridftp.grid.uchicago.edu:2811/xenon/workflow_scratch/{}'.format(getpass.getuser()), Operation.ALL))
        staging.add_directories(scratch_dir)
        
        # staging ISI
        staging_isi = Site("staging-isi")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path='/lizard/scratch-90-days/XENONnT/{}'.format(getpass.getuser()))
        scratch_dir.add_file_servers(FileServer('gsiftp://workflow.isi.edu:2811/lizard/scratch-90-days/XENONnT/{}'.format(getpass.getuser()), Operation.ALL))
        staging_isi.add_directories(scratch_dir)

        # condorpool
        condorpool = Site("condorpool")
        condorpool.add_profiles(Namespace.PEGASUS, style='condor')
        condorpool.add_profiles(Namespace.CONDOR, universe='vanilla')
        condorpool.add_profiles(Namespace.CONDOR, key='+ProjectName', value='"xenon1t"')
        condorpool.add_profiles(Namespace.CONDOR, key='+SingularityImage',
                                value=f'"{self.singularity_image}"')
        condorpool.add_profiles(Namespace.CONDOR, key='periodic_remove',
                                value="((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > 18000)) || "
                                      "((JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 30)) "
                                )
        if self.xsede:
            condorpool.add_profiles(Namespace.CONDOR, key='+Desired_Allocations', value='"Expanse"')

        # ignore the site settings - the container will set all this up inside
        condorpool.add_profiles(Namespace.ENV, OSG_LOCATION='')
        condorpool.add_profiles(Namespace.ENV, GLOBUS_LOCATION='')
        condorpool.add_profiles(Namespace.ENV, PYTHONPATH='')
        condorpool.add_profiles(Namespace.ENV, PERL5LIB='')
        condorpool.add_profiles(Namespace.ENV, LD_LIBRARY_PATH='')

        condorpool.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ['USER'])
        condorpool.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")
        condorpool.add_profiles(Namespace.ENV, RUCIO_ACCOUNT='production')

        sc.add_sites(local,
                     staging,
                     staging_isi,
                     #output,
                     condorpool)

        return sc

    def dtype_validity(self, dtype):
        st = self.context
        cmt_used = {d: [] for d in st.provided_dtypes()}
        cmt_options = straxen.get_corrections.get_cmt_options(st)
        gain_converter = {'to_pe_model': 'pmt_000_gain_xenonnt',
                          'to_pe_model_nv': 'n_veto_000_gain_xenonnt',
                          'to_pe_model_mv': 'mu_veto_000_gain_xenonnt'
                          }

        for opt, cmt_info in cmt_options.items():
            for d, plugin in st._plugin_class_registry.items():
                if opt in plugin.takes_config:
                    #
                    if isinstance(cmt_info, dict):
                        collname = cmt_info['correction']
                        strax_opt = cmt_info['strax_option']
                    else:
                        collname = cmt_info[0]
                        strax_opt = cmt_info
                    if isinstance(strax_opt, str) and 'cmt://' in strax_opt:
                        path, kwargs = straxen.URLConfig.split_url_kwargs(strax_opt)
                        version = kwargs['version']
                    else:
                        version = strax_opt[1]

                    collname = gain_converter.get(collname, collname)
                    cmt_used[d].append((collname, version))

        dependencies = self.get_dependencies(dtype)
        dependencies.append(dtype)
        start = datetime(1970, 1, 1)
        end = datetime(2070, 1, 1)
        posrec_corrs = ['fdc_map', 's1_xyz_map']
        for d in dependencies:
            for corr, local_version in cmt_used[d]:
                if corr in posrec_corrs:
                    for algo in ['mlp', 'gcn', 'cnn']:
                        corr_start, corr_end = cmt_local_valid_range(f"{corr}_{algo}", local_version)
                        if corr_start > start:
                            start = corr_start
                        if corr_end < end:
                            end = corr_end
                else:
                    corr_start, corr_end = cmt_local_valid_range(corr, local_version)
                    if corr_start > start:
                        start = corr_start
                    if corr_end < end:
                        end = corr_end
        return start, end

    def get_dependencies(self, dtype):
        dependencies = []

        def _get_dependencies(_dtype):
            plugin = self.context._plugin_class_registry[_dtype]
            depends_on = plugin.depends_on
            if type(depends_on) == type('string'):
                depends_on = [depends_on]
            else:
                depends_on = [x for x in depends_on]

            dependencies.extend(depends_on)
            if dtype in RAW_DTYPES:
                return
            for dep in depends_on:
                _get_dependencies(dep)

        _get_dependencies(dtype)
        dependencies = list(set(dependencies))
        return dependencies
