#!/usr/bin/env python3

import os
import getpass
import logging
import time
import shutil
from datetime import datetime

import numpy as np
from tqdm import tqdm
import cutax
from utilix.rundb import DB

from outsource.config import config, base_dir, DEPENDS_ON, DBConfig
from outsource.shell import Shell


# Pegasus environment
from Pegasus.api import (
    Operation,
    Namespace,
    Workflow,
    File,
    Directory,
    FileServer,
    Job,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    ReplicaCatalog,
)

# logging.basicConfig(level=config.logging_level)
logger = logging.getLogger()

IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"
COMBINE_WRAPPER = "combine-wrapper.sh"
STRAX_WRAPPER = "strax-wrapper.sh"
COMBINE_MEMORY = config.getint("Outsource", "combine_memory")
COMBINE_DISK = config.getint("Outsource", "combine_disk")
PEAKLETS_MEMORY = config.getint("Outsource", "peaklets_memory")
PEAKLETS_DISK = config.getint("Outsource", "peaklets_disk")
EVENTS_MEMORY = config.getint("Outsource", "events_memory")
EVENTS_DISK = config.getint("Outsource", "events_disk")

PER_CHUNK_DTYPES = ["records", "peaklets", "hitlets_nv", "afterpulses", "led_calibration"]
NEED_RAW_DATA_DTYPES = [
    "peaklets",
    "peak_basics_he",
    "hitlets_nv",
    "events_mv",
    "afterpulses",
    "led_calibration",
]

db = DB()


class Outsource:
    # Data availability to site selection map.
    # desired_sites mean condor will try to run the job on those sites
    _rse_site_map = {
        "UC_OSG_USERDISK": {"expr": 'GLIDEIN_Country == "US"'},
        "UC_DALI_USERDISK": {"expr": 'GLIDEIN_Country == "US"'},
        "UC_MIDWAY_USERDISK": {"expr": 'GLIDEIN_Country == "US"'},
        "CCIN2P3_USERDISK": {"desired_sites": "CCIN2P3", "expr": 'GLIDEIN_Site == "CCIN2P3"'},
        "CNAF_TAPE_USERDISK": {},
        "CNAF_USERDISK": {},
        "LNGS_USERDISK": {},
        "NIKHEF2_USERDISK": {"desired_sites": "NIKHEF", "expr": 'GLIDEIN_Site == "NIKHEF"'},
        "NIKHEF_USERDISK": {"desired_sites": "NIKHEF", "expr": 'GLIDEIN_Site == "NIKHEF"'},
        "SURFSARA_USERDISK": {"desired_sites": "SURFsara", "expr": 'GLIDEIN_Site == "SURFsara"'},
        "WEIZMANN_USERDISK": {"desired_sites": "Weizmann", "expr": 'GLIDEIN_Site == "Weizmann"'},
        "SDSC_USERDISK": {"expr": 'GLIDEIN_ResourceName == "SDSC-Expanse"'},
        "SDSC_NSDF_USERDISK": {"expr": 'GLIDEIN_Country == "US"'},
    }

    # transformation map (high level name -> script)
    _transformations_map = {
        "combine": COMBINE_WRAPPER,
        "download": STRAX_WRAPPER,
        "records": STRAX_WRAPPER,
        "peaklets": STRAX_WRAPPER,
        "peak_basics": STRAX_WRAPPER,
        "events": STRAX_WRAPPER,
        "event_shadow": STRAX_WRAPPER,
        "peaks_he": STRAX_WRAPPER,
        "nv_hitlets": STRAX_WRAPPER,
        "nv_events": STRAX_WRAPPER,
        "mv": STRAX_WRAPPER,
        "afterpulses": STRAX_WRAPPER,
        "led": STRAX_WRAPPER,
    }

    # jobs details for a given datatype
    # disk is in KB, memory in MB
    job_kwargs = {
        "combine": dict(name="combine", memory=COMBINE_MEMORY, disk=COMBINE_DISK),
        "download": dict(name="download", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
        "records": dict(name="records", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
        "peaklets": dict(name="peaklets", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
        "peak_basics": dict(name="peak_basics", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "event_info_double": dict(name="events", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "event_shadow": dict(name="event_shadow", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "peak_basics_he": dict(name="peaks_he", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "hitlets_nv": dict(name="nv_hitlets", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
        "events_nv": dict(name="nv_events", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "ref_mon_nv": dict(name="ref_mon_nv", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "events_mv": dict(name="mv", memory=EVENTS_MEMORY, disk=EVENTS_DISK),
        "afterpulses": dict(name="afterpulses", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
        "led_calibration": dict(name="led", memory=PEAKLETS_MEMORY, disk=PEAKLETS_DISK),
    }

    _x509_proxy = os.getenv("X509_USER_PROXY")

    def __init__(
        self,
        runlist,
        context_name,
        xedocs_version,
        image,
        workflow_id=None,
        upload_to_rucio=True,
        update_db=True,
        force_rerun=False,
        debug=True,
    ):
        """Creates a new Outsource object.

        Specifying a list of DBConfig objects required.
        """

        if not isinstance(runlist, list):
            raise RuntimeError("Outsource expects a list of DBConfigs to run")

        self._runlist = runlist

        # setup context
        self.context_name = context_name
        self.xedocs_version = xedocs_version
        self.context = getattr(cutax.contexts, context_name)(xedocs_version=self.xedocs_version)

        # Load from xenon_config
        self.debug = debug
        # Assume that if the image is not a full path, it is a name
        if not os.path.exists(image):
            self.image_tag = image
            self.singularity_image = f"{IMAGE_PREFIX}{image}"
        else:
            self.image_tag = image.split(":")[-1]
            self.singularity_image = image
        self.force_rerun = force_rerun
        self.upload_to_rucio = upload_to_rucio
        self.update_db = update_db

        self.set_logger()

        self.work_dir = config.get("Outsource", "work_dir")

        # User can provide a name for the workflow, otherwise it will be the current time
        self._setup_workflow_id(workflow_id)
        # Pegasus workflow directory
        self.workflow_dir = os.path.join(self.work_dir, self.workflow_id)
        self.generated_dir = os.path.join(self.workflow_dir, "generated")
        self.runs_dir = os.path.join(self.workflow_dir, "runs")
        self.outputs_dir = os.path.join(self.workflow_dir, "outputs")
        self.scratch_dir = os.path.join(self.workflow_dir, "scratch")

    def set_logger(self):
        console = logging.StreamHandler()
        # default log level - make logger/console match
        # debug - where to get this from?
        if self.debug:
            logger.setLevel(logging.DEBUG)
            console.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)
            console.setLevel(logging.WARNING)
        # formatter
        # formatter = logging.Formatter(
        #     "%(asctime)s %(levelname)7s:  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        # )
        # console.setFormatter(formatter)
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.addHandler(console)

    def _setup_workflow_id(self, workflow_id):
        """Set up the workflow ID."""
        # Determine a unique id for the workflow. If none passed, looks at the runlist.
        # If only one run_id is provided, use the run_id of that object.
        # If more than one is provided, use current time.
        if workflow_id:
            self.workflow_id = workflow_id
        else:
            if len(self._runlist) == 1:
                workflow_id = (
                    self.image_tag,
                    self.context_name,
                    self.xedocs_version,
                    f"{self._runlist[0]:06d}",
                )
            else:
                workflow_id = (
                    self.image_tag,
                    self.context_name,
                    self.xedocs_version,
                    datetime.now().strftime("%Y%m%d%H%M"),
                )
            self.workflow_id = "-".join(workflow_id)

    @property
    def x509_proxy(self):
        assert self._x509_proxy, "Please provide a valid X509_USER_PROXY environment variable."
        return self._x509_proxy

    @property
    def workflow(self):
        return os.path.join(self.generated_dir, "workflow.yml")

    @property
    def runlist(self):
        return os.path.join(self.generated_dir, "runlist.txt")

    def submit(self, force=False):
        """Main interface to submitting a new workflow."""

        # does workflow already exist?
        if os.path.exists(self.workflow_dir):
            if force:
                logger.warning(f"Overwriting workflow at {self.workflow_dir}. CTRL+C now to stop.")
                time.sleep(10)
                shutil.rmtree(self.workflow_dir)
            else:
                raise RuntimeError(f"Workflow already exists at {self.workflow_dir}.")

        # work dirs
        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.runs_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)

        # ensure we have a proxy with enough time left
        self._validate_x509_proxy()

        # generate the workflow
        wf = self._generate_workflow()

        # submit the workflow
        self._plan_and_submit(wf)

    def _generate_workflow(self):
        """Use the Pegasus API to build an abstract graph of the workflow."""

        # Create a abstract dag
        wf = Workflow("xenonnt")
        # Initialize the catalogs
        sc = self._generate_sc()
        tc = self._generate_tc()
        rc = self._generate_rc()

        # add executables to the wf-level transformation catalog
        for job_type, script in self._transformations_map.items():
            t = Transformation(
                job_type,
                site="local",
                pfn=f"file://{base_dir}/workflow/{script}",
                is_stageable=True,
            )
            tc.add_transformations(t)

        # scripts some exectuables might need
        straxify = File("runstrax.py")
        rc.add_replica("local", "runstrax.py", f"file://{base_dir}/workflow/runstrax.py")
        combinepy = File("combine.py")
        rc.add_replica("local", "combine.py", f"file://{base_dir}/workflow/combine.py")

        # add common data files to the replica catalog
        xenon_config = File(".xenon_config")
        rc.add_replica("local", ".xenon_config", f"file://{config.config_path}")

        # token needed for DB connection
        token = File(".dbtoken")
        rc.add_replica(
            "local", ".dbtoken", "file://" + os.path.join(os.environ["HOME"], ".dbtoken")
        )

        # cutax tarball
        cutax_tarball = File("cutax.tar.gz")
        if "CUTAX_LOCATION" not in os.environ:
            logger.warning("No CUTAX_LOCATION env variable found. Using the latest by default!")
            tarball_path = "/ospool/uc-shared/project/xenon/xenonnt/software/cutax/latest.tar.gz"
        else:
            tarball_path = os.environ["CUTAX_LOCATION"].replace(".", "-") + ".tar.gz"
            logger.warning(f"Using cutax: {tarball_path}")
        rc.add_replica("local", "cutax.tar.gz", tarball_path)

        # runs
        iterator = self._runlist if len(self._runlist) == 1 else tqdm(self._runlist)

        # keep track of what runs we submit, useful for bookkeeping
        runlist = []
        for run in iterator:
            dbcfg = DBConfig(run, self.context, force_rerun=self.force_rerun)

            # check if this run needs to be processed
            if len(dbcfg.needs_processed) > 0:
                logger.debug(f"Adding run {dbcfg.number:06d} to the workflow")
            else:
                logger.debug(
                    f"Run {dbcfg.number:06d} is already processed with context {self.context_name}"
                )
                continue

            requirements_base = "HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org"
            requirements_base += " && PORT_2880 && PORT_8000 && PORT_27017"
            requirements_base += ' && (Microarch >= "x86_64-v3")'
            requirements_base_us = requirements_base + ' && GLIDEIN_Country == "US"'
            if config.getboolean("Outsource", "us_only", fallback=False):
                requirements_base = requirements_base_us

            # hs06_test_run limits the run to a set of compute nodes
            # at UChicago with a known HS06 factor
            if config.getboolean("Outsource", "hs06_test_run", fallback=False):
                requirements_base += (
                    ' && GLIDEIN_ResourceName == "MWT2" && regexp("uct2-c4[1-7]", Machine)'
                )
            # this_site_only limits the run to a set of compute nodes at UChicago for testing
            this_site_only = config.get("Outsource", "this_site_only", fallback="")
            if this_site_only:
                requirements_base += f' && GLIDEIN_ResourceName == "{this_site_only}"'

            # will have combine jobs for all the PER_CHUNK_DTYPES we passed
            combine_jobs = {}

            # get dtypes to process
            for dtype_i, dtype in enumerate(dbcfg.needs_processed):
                # these dtypes need raw data
                if dtype in NEED_RAW_DATA_DTYPES:
                    # check that raw data exist for this run
                    if not all([dbcfg._raw_data_exists(raw_type=d) for d in DEPENDS_ON[dtype]]):
                        logger.info(f"Doesn't have raw data for {dtype} of run {run}, skipping")
                        continue

                logger.debug(f"|-----> adding {dtype}")
                if dbcfg.number not in runlist:
                    runlist.append(dbcfg.number)
                rses = dbcfg.rses[dtype]
                if len(rses) == 0:
                    if dtype == "raw_records":
                        raise RuntimeError(
                            f"Unable to find a raw records location for {dbcfg.number:06d}"
                        )
                    else:
                        logger.debug(
                            f"No data found for {dbcfg.number:06d}-{dtype}... "
                            f"hopefully those will be created by the workflow"
                        )

                # determine the job requirements based on the data locations
                rses_specified = config.get("Outsource", "raw_records_rse").split(",")
                # for standalone downloads, only target us
                if dbcfg.standalone_download:
                    rses = rses_specified

                # For low level data, we only want to run on sites
                # that we specified for raw_records_rse
                if dtype in NEED_RAW_DATA_DTYPES:
                    rses = list(set(rses) & set(rses_specified))
                    assert len(rses) > 0, (
                        f"No sites found for {dbcfg.number:06d}-{dtype}, "
                        "since no intersection between the available rses "
                        f"{rses} and the specified raw_records_rses {rses_specified}"
                    )

                sites_expression, desired_sites = self._determine_target_sites(rses)
                # general compute jobs
                requirements = requirements_base if len(rses) > 0 else requirements_base_us
                if sites_expression:
                    requirements += f" && ({sites_expression})"
                # us nodes
                requirements_us = requirements_base_us
                # add excluded nodes
                if self._exclude_sites:
                    requirements += f" && ({self._exclude_sites})"
                    requirements_us += f" && ({self._exclude_sites})"

                if dtype in PER_CHUNK_DTYPES:
                    # Set up the combine job first -
                    # we can then add to that job inside the chunk file loop
                    # only need combine job for low-level stuff
                    combine_job = self._job(
                        "combine", disk=self.job_kwargs["combine"]["disk"], cores=4
                    )
                    # combine jobs must happen in the US
                    combine_job.add_profiles(Namespace.CONDOR, "requirements", requirements_us)
                    # priority is given in the order they were submitted
                    combine_job.add_profiles(Namespace.CONDOR, "priority", f"{dbcfg.priority}")
                    combine_job.add_inputs(combinepy, xenon_config, cutax_tarball, token)
                    combine_output_tar_name = f"{dbcfg.number:06d}-{dtype}-combined.tar.gz"
                    combine_output_tar = File(combine_output_tar_name)
                    combine_job.add_outputs(
                        combine_output_tar, stage_out=(not self.upload_to_rucio)
                    )
                    combine_job.add_args(
                        f"{dbcfg.number}",
                        dtype,
                        self.context_name,
                        combine_output_tar_name,
                        f"{self.upload_to_rucio}".lower(),
                        f"{self.update_db}".lower(),
                    )

                    wf.add_jobs(combine_job)
                    combine_jobs[dtype] = (combine_job, combine_output_tar)

                    # add jobs, one for each input file
                    n_chunks = dbcfg.nchunks(dtype)

                    chunk_list = np.arange(n_chunks)
                    njobs = int(np.ceil(n_chunks / dbcfg.chunks_per_job))

                    # Loop over the chunks
                    for job_i in range(njobs):
                        chunks = chunk_list[
                            dbcfg.chunks_per_job * job_i : dbcfg.chunks_per_job * (job_i + 1)
                        ]
                        chunk_str = " ".join([f"{c}" for c in chunks])

                        logger.debug(f" ... adding job for chunk files: {chunk_str}")

                        # standalone download is a special case where we download data
                        # from rucio first, which is useful for testing and when using
                        # dedicated clusters with storage
                        if dbcfg.standalone_download:
                            data_tar = File(f"{dbcfg.number:06d}-{dtype}-data-{job_i:04d}.tar.gz")
                            download_job = self._job(
                                "download", disk=self.job_kwargs["download"]["disk"]
                            )
                            download_job.add_profiles(
                                Namespace.CONDOR, "requirements", requirements
                            )
                            download_job.add_profiles(
                                Namespace.CONDOR, "priority", f"{dbcfg.priority}"
                            )
                            download_job.add_args(
                                f"{dbcfg.number}",
                                self.context_name,
                                dtype,
                                data_tar,
                                "download-only",
                                f"{self.upload_to_rucio}".lower(),
                                f"{self.update_db}".lower(),
                                chunk_str,
                            )
                            download_job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                            download_job.add_outputs(data_tar, stage_out=False)
                            wf.add_jobs(download_job)

                        # output files
                        job_output_tar = File(
                            f"{dbcfg.number:06d}-{dtype}-output-{job_i:04d}.tar.gz"
                        )
                        # do we already have a local copy?
                        job_output_tar_local_path = os.path.join(
                            self.outputs_dir, f"{job_output_tar}"
                        )
                        if os.path.isfile(job_output_tar_local_path):
                            logger.info(f" ... local copy found at: {job_output_tar_local_path}")
                            rc.add_replica(
                                "local", job_output_tar, f"file://{job_output_tar_local_path}"
                            )

                        # Add job
                        job = self._job(**self.job_kwargs[dtype])
                        if desired_sites and len(desired_sites) > 0:
                            # give a hint to glideinWMS for the sites
                            # we want(mostly useful for XENONVO in Europe)
                            job.add_profiles(
                                Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"'
                            )
                        job.add_profiles(Namespace.CONDOR, "requirements", requirements)
                        job.add_profiles(Namespace.CONDOR, "priority", f"{dbcfg.priority}")
                        # job.add_profiles(Namespace.CONDOR, 'periodic_remove', periodic_remove)

                        job.add_args(
                            f"{dbcfg.number}",
                            self.context_name,
                            dtype,
                            job_output_tar,
                            "false",  # if not dbcfg.standalone_download else 'no-download',
                            f"{self.upload_to_rucio}".lower(),
                            f"{self.update_db}".lower(),
                            chunk_str,
                        )

                        job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                        job.add_outputs(job_output_tar, stage_out=(not self.upload_to_rucio))
                        wf.add_jobs(job)

                        # all strax jobs depend on the pre-flight or a download job,
                        # but pre-flight jobs have been outdated so it is not necessary.
                        if dbcfg.standalone_download:
                            job.add_inputs(data_tar)
                            wf.add_dependency(job, parents=[download_job])

                        # update combine job
                        combine_job.add_inputs(job_output_tar)
                        wf.add_dependency(job, children=[combine_job])

                        parent_combines = []
                        for d in DEPENDS_ON[dtype]:
                            if d in combine_jobs:
                                parent_combines.append(combine_jobs.get(d))

                        if len(parent_combines):
                            wf.add_dependency(job, parents=parent_combines)
                else:
                    # high level data.. we do it all on one job
                    # output files
                    job_output_tar = File(f"{dbcfg.number:06d}-{dtype}-output.tar.gz")

                    # Add job
                    job = self._job(**self.job_kwargs[dtype], cores=2)
                    # https://support.opensciencegrid.org/support/solutions/articles/12000028940-working-with-tensorflow-gpus-and-containers
                    job.add_profiles(Namespace.CONDOR, "requirements", requirements)
                    job.add_profiles(Namespace.CONDOR, "priority", f"{dbcfg.priority}")

                    # Note that any changes to this argument list,
                    # also means strax-wrapper.sh has to be updated
                    job.add_args(
                        f"{dbcfg.number}",
                        self.context_name,
                        dtype,
                        job_output_tar,
                        "false",  # if not dbcfg.standalone_download else 'no-download',
                        f"{self.upload_to_rucio}".lower(),
                        f"{self.update_db}".lower(),
                    )

                    job.add_inputs(straxify, xenon_config, token, cutax_tarball)
                    job.add_outputs(
                        job_output_tar, stage_out=True
                    )  # as long as we are giving outputs
                    wf.add_jobs(job)

                    # if there are multiple levels to the workflow,
                    # need to have current strax-wrapper depend on previous combine

                    for d in DEPENDS_ON[dtype]:
                        if d in combine_jobs:
                            cj, cj_output = combine_jobs[d]
                            wf.add_dependency(job, parents=[cj])
                            job.add_inputs(cj_output)

        # Write the wf to stdout
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        wf.add_site_catalog(sc)
        wf.write(file=self.workflow)

        # save the runlist
        np.savetxt(self.runlist, runlist, fmt="%0d")

        return wf

    def _plan_and_submit(self, wf):
        """Submit the workflow."""

        wf.plan(
            conf=f"{base_dir}/workflow/pegasus.conf",
            submit=not self.debug,
            sites=["condorpool"],
            staging_sites={"condorpool": "staging-davs"},
            output_sites=["staging-davs"],
            dir=os.path.dirname(self.runs_dir),
            relative_dir=os.path.basename(self.runs_dir),
        )
        # copy the runlist file
        shutil.copy(self.runlist, self.runs_dir)

        logger.info(f"Worfklow written to \n\n\t{self.runs_dir}\n\n")

    def _validate_x509_proxy(self, min_valid_hours=20):
        """Ensure $X509_USER_PROXY exists and has enough time left.

        This is necessary only if you are going to use Rucio.
        """
        x509_user_proxy = os.getenv("X509_USER_PROXY")
        assert x509_user_proxy, "Please provide a valid X509_USER_PROXY environment variable."

        logger.debug("Verifying that the X509_USER_PROXY proxy has enough lifetime")
        shell = Shell(f"grid-proxy-info -timeleft -file {x509_user_proxy}")
        shell.run()
        valid_hours = int(shell.get_outerr()) / 60 / 60
        if valid_hours < min_valid_hours:
            raise RuntimeError(
                f"User proxy is only valid for {valid_hours} hours. "
                f"Minimum required is {min_valid_hours} hours."
            )

    def _job(self, name, run_on_submit_node=False, cores=1, memory=1_700, disk=1_000_000):
        """Wrapper for a Pegasus job, also sets resource requirement profiles.

        Memory in unit of MB, and disk in unit of MB.
        """
        job = Job(name)

        if run_on_submit_node:
            job.add_selector_profile(execution_site="local")
            # no other attributes on a local job
            return job

        job.add_profiles(Namespace.CONDOR, "request_cpus", f"{cores}")

        # increase memory/disk if the first attempt fails
        memory = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry == 0, {memory}, (DAGNodeRetry + 1)*{memory})"
        )
        disk_str = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry == 0, {disk}, (DAGNodeRetry + 1)*{disk})"
        )
        job.add_profiles(Namespace.CONDOR, "request_disk", disk_str)
        job.add_profiles(Namespace.CONDOR, "request_memory", memory)

        return job

    def _data_find_chunks(self, rc, rucio_dataset):
        """
        Look up which chunk files are in the dataset - return a dict where the keys are the
        chunks, and the values a dict of locations
        """

        logger.debug("Querying Rucio for files in the data set " + rucio_dataset)
        result = rc.ListFiles(rucio_dataset)
        chunks_files = [f["name"] for f in result if "json" not in f["name"]]
        return chunks_files

    def _determine_target_sites(self, rses):
        """Given a list of RSEs, limit the runs for sites for those
        locations."""

        exprs = []
        sites = []
        for rse in rses:
            if rse in self._rse_site_map:
                if "expr" in self._rse_site_map[rse]:
                    exprs.append(self._rse_site_map[rse]["expr"])
                if "desired_sites" in self._rse_site_map[rse]:
                    sites.append(self._rse_site_map[rse]["desired_sites"])

        # make sure we do not request XENON1T sites we do not need
        if len(sites) == 0:
            sites.append("NONE")

        final_expr = " || ".join(exprs)
        desired_sites = ",".join(sites)
        logger.debug("Site expression from RSEs list: " + final_expr)
        logger.debug(
            "XENON_DESIRED_Sites from RSEs list (mostly used for European sites): " + desired_sites
        )
        return final_expr, desired_sites

    @property
    def _exclude_sites(self):
        """Exclude sites from the user _dbcfgs file."""

        if not config.has_option("Outsource", "exclude_sites"):
            return ""

        sites = config.get_list("Outsource", "exclude_sites")
        exprs = []
        for site in sites:
            exprs.append(f'GLIDEIN_Site =!= "{site}"')
        return " && ".join(exprs)

    def _generate_sc(self):
        sc = SiteCatalog()

        # local site - this is the submit host
        local = Site("local")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path=f"{self.scratch_dir}")
        scratch_dir.add_file_servers(FileServer(f"file:///{self.scratch_dir}", Operation.ALL))
        storage_dir = Directory(Directory.LOCAL_STORAGE, path=self.outputs_dir)
        storage_dir.add_file_servers(FileServer(f"file:///{self.outputs_dir}", Operation.ALL))
        local.add_directories(scratch_dir, storage_dir)

        local.add_profiles(Namespace.ENV, HOME=os.environ["HOME"])
        local.add_profiles(Namespace.ENV, GLOBUS_LOCATION="")
        local.add_profiles(
            Namespace.ENV,
            PATH="/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/bin:/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/condabin:/usr/bin:/bin",  # noqa
        )
        local.add_profiles(
            Namespace.ENV,
            LD_LIBRARY_PATH="/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/lib64:/cvmfs/xenon.opensciencegrid.org/releases/nT/development/anaconda/envs/XENONnT_development/lib",  # noqa
        )
        local.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ["USER"])
        local.add_profiles(Namespace.ENV, X509_USER_PROXY=self.x509_proxy)
        # local.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")  # noqa
        if not self.debug:
            local.add_profiles(Namespace.ENV, RUCIO_ACCOUNT="production")
        # improve python logging / suppress depreciation warnings (from gfal2 for example)
        local.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        local.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        # staging site
        staging = Site("staging")
        scratch_dir = Directory(
            Directory.SHARED_SCRATCH,
            path="/ospool/uc-shared/project/xenon/wf-scratch/{}".format(getpass.getuser()),
        )
        scratch_dir.add_file_servers(
            FileServer(
                "osdf:///ospool/uc-shared/project/xenon/wf-scratch/{}".format(getpass.getuser()),
                Operation.ALL,
            )
        )
        staging.add_directories(scratch_dir)

        # staging site - davs
        staging_davs = Site("staging-davs")
        scratch_dir = Directory(
            Directory.SHARED_SCRATCH, path="/xenon/scratch/{}".format(getpass.getuser())
        )
        scratch_dir.add_file_servers(
            FileServer(
                "gsidavs://xenon-gridftp.grid.uchicago.edu:2880/xenon/scratch/{}".format(
                    getpass.getuser()
                ),
                Operation.ALL,
            )
        )
        staging_davs.add_directories(scratch_dir)

        # output on davs
        output_dir = Directory(
            Directory.LOCAL_STORAGE, path="/xenon/output/{}".format(getpass.getuser())
        )
        output_dir.add_file_servers(
            FileServer(
                "gsidavs://xenon-gridftp.grid.uchicago.edu:2880/xenon/output/{}".format(
                    getpass.getuser()
                ),
                Operation.ALL,
            )
        )
        staging_davs.add_directories(output_dir)

        # condorpool
        condorpool = Site("condorpool")
        condorpool.add_profiles(Namespace.PEGASUS, style="condor")
        condorpool.add_profiles(Namespace.CONDOR, universe="vanilla")
        # we need the x509 proxy for Rucio transfers
        condorpool.add_profiles(Namespace.CONDOR, key="x509userproxy", value=self.x509_proxy)
        condorpool.add_profiles(
            Namespace.CONDOR, key="+SingularityImage", value=f'"{self.singularity_image}"'
        )

        # ignore the site settings - the container will set all this up inside
        condorpool.add_profiles(Namespace.ENV, OSG_LOCATION="")
        condorpool.add_profiles(Namespace.ENV, GLOBUS_LOCATION="")
        condorpool.add_profiles(Namespace.ENV, PYTHONPATH="")
        condorpool.add_profiles(Namespace.ENV, PERL5LIB="")
        condorpool.add_profiles(Namespace.ENV, LD_LIBRARY_PATH="")

        condorpool.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ["USER"])
        condorpool.add_profiles(
            Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s"
        )
        if not self.debug:
            condorpool.add_profiles(Namespace.ENV, RUCIO_ACCOUNT="production")

        # improve python logging / suppress depreciation warnings (from gfal2 for example)
        condorpool.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        condorpool.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        sc.add_sites(
            local,
            staging_davs,
            # output,
            condorpool,
        )
        return sc

    def _generate_tc(self):
        return TransformationCatalog()

    def _generate_rc(self):
        return ReplicaCatalog()
