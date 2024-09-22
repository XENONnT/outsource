#!/usr/bin/env python3

import os
import sys
import getpass
import time
import shutil
from datetime import datetime
import numpy as np
from tqdm import tqdm
import utilix
from utilix import DB, uconfig
from utilix.x509 import _validate_x509_proxy
from utilix.tarball import Tarball
from utilix.config import setup_logger, set_logging_level
import cutax

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

from outsource.config import base_dir, RunConfig, PER_CHUNK_DTYPES, NEED_RAW_DATA_DTYPES

IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"
COMBINE_WRAPPER = "combine-wrapper.sh"
PROCESS_WRAPPER = "process-wrapper.sh"
COMBINE_MEMORY = uconfig.getint("Outsource", "combine_memory")
COMBINE_DISK = uconfig.getint("Outsource", "combine_disk")
PEAKLETS_MEMORY = uconfig.getint("Outsource", "peaklets_memory")
PEAKLETS_DISK = uconfig.getint("Outsource", "peaklets_disk")
EVENTS_MEMORY = uconfig.getint("Outsource", "events_memory")
EVENTS_DISK = uconfig.getint("Outsource", "events_disk")

db = DB()


class Outsource:
    # Transformation map (high level name -> script)
    _transformations_map = {
        "combine": COMBINE_WRAPPER,
        "download": PROCESS_WRAPPER,
        "records": PROCESS_WRAPPER,
        "peaklets": PROCESS_WRAPPER,
        "peak_basics": PROCESS_WRAPPER,
        "events": PROCESS_WRAPPER,
        "event_shadow": PROCESS_WRAPPER,
        "peaks_he": PROCESS_WRAPPER,
        "nv_hitlets": PROCESS_WRAPPER,
        "nv_events": PROCESS_WRAPPER,
        "mv": PROCESS_WRAPPER,
        "afterpulses": PROCESS_WRAPPER,
        "led": PROCESS_WRAPPER,
    }

    # Jobs details for a given datatype
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

    def __init__(
        self,
        runlist,
        context_name,
        xedocs_version,
        image,
        workflow_id=None,
        upload_to_rucio=True,
        update_db=True,
        force=False,
        debug=True,
    ):
        self.logger = setup_logger(
            "outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING")
        )
        # Reduce the logging of request and urllib3
        set_logging_level(
            "urllib3", uconfig.get("Outsource", "db_logging_level", fallback="WARNING")
        )

        # Assume that if the image is not a full path, it is a name
        if not os.path.exists(image):
            self.image_tag = image
            self.singularity_image = f"{IMAGE_PREFIX}{image}"
        else:
            self.image_tag = image.split(":")[-1]
            self.singularity_image = image

        # Check if the environment used to run this script is consistent with the container
        if self.image_tag not in sys.executable:
            raise EnvironmentError(
                f"The current environment's python: {sys.executable} "
                f"is not consistent with the aimed container: {self.image_tag}. "
                "Please use the following command to activate the correct environment: \n"
                f"source /cvmfs/xenon.opensciencegrid.org/releases/nT/{self.image_tag}/setup.sh"
            )

        if not isinstance(runlist, list):
            raise RuntimeError("Outsource expects a list of run_id")
        self._runlist = runlist

        # Setup context
        self.context_name = context_name
        self.xedocs_version = xedocs_version
        self.context = getattr(cutax.contexts, context_name)(xedocs_version=self.xedocs_version)

        self.debug = debug
        self.force = force
        self.upload_to_rucio = upload_to_rucio
        self.update_db = update_db

        # Load from xenon_config
        self.work_dir = uconfig.get("Outsource", "work_dir")

        # User can provide a name for the workflow, otherwise it will be the current time
        self._setup_workflow_id(workflow_id)
        # Pegasus workflow directory
        self.workflow_dir = os.path.join(self.work_dir, self.workflow_id)
        self.generated_dir = os.path.join(self.workflow_dir, "generated")
        self.runs_dir = os.path.join(self.workflow_dir, "runs")
        self.outputs_dir = os.path.join(self.workflow_dir, "outputs")
        self.scratch_dir = os.path.join(self.workflow_dir, "scratch")

    @property
    def workflow(self):
        return os.path.join(self.generated_dir, "workflow.yml")

    @property
    def runlist(self):
        return os.path.join(self.generated_dir, "runlist.txt")

    @property
    def pegasus_config(self):
        """Pegasus configurations."""
        pconfig = {}
        pconfig["pegasus.metrics.app"] = "XENON"
        pconfig["pegasus.data.configuration"] = "nonsharedfs"
        # provide a full kickstart record, including the environment.
        # Even for successful jobs.
        pconfig["pegasus.gridstart.arguments"] = "-f"
        pconfig["pegasus.mode"] = "development"
        # give jobs a total of {retry} + 1 tries
        pconfig["dagman.retry"] = uconfig.getint("Outsource", "dagman_retry", fallback=2)
        # make sure we do start too many jobs at the same time
        pconfig["dagman.maxidle"] = uconfig.getint("Outsource", "dagman_maxidle", fallback=5_000)
        # total number of jobs cap
        pconfig["dagman.maxjobs"] = uconfig.getint("Outsource", "dagman_maxjobs", fallback=300)
        # transfer parallelism
        pconfig["pegasus.transfer.threads"] = 1

        # Help Pegasus developers by sharing performance data (optional)
        pconfig["pegasus.monitord.encoding"] = "json"
        pconfig["pegasus.catalog.workflow.amqp.url"] = (
            "amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows"
        )
        # Temporary bypassing integrity check in pegasus
        pconfig["pegasus.integrity.checking"] = "none"
        return pconfig

    def _job(self, name, run_on_submit_node=False, cores=1, memory=1_700, disk=1_000):
        """Wrapper for a Pegasus job, also sets resource requirement profiles.

        Memory and disk in unit of MB.
        """
        job = Job(name)

        if run_on_submit_node:
            job.add_selector_profile(execution_site="local")
            # No other attributes on a local job
            return job

        job.add_profiles(Namespace.CONDOR, "request_cpus", cores)

        # Increase memory/disk if the first attempt fails
        memory = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry == 0, {memory}, (DAGNodeRetry + 1)*{memory})"
        )
        disk_str = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry == 0, {disk * 1_000}, (DAGNodeRetry + 1)*{disk * 1_000})"
        )
        job.add_profiles(Namespace.CONDOR, "request_disk", disk_str)
        job.add_profiles(Namespace.CONDOR, "request_memory", memory)

        return job

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
            PATH=(
                f"/cvmfs/xenon.opensciencegrid.org/releases/nT/{self.image_tag}/anaconda/envs/XENONnT_development/bin:"  # noqa
                f"/cvmfs/xenon.opensciencegrid.org/releases/nT/{self.image_tag}/anaconda/condabin:"
                "/usr/bin:/bin"
            ),
        )
        local.add_profiles(
            Namespace.ENV,
            LD_LIBRARY_PATH=(
                f"/cvmfs/xenon.opensciencegrid.org/releases/nT/{self.image_tag}/anaconda/envs/XENONnT_development/lib64:"  # noqa
                f"/cvmfs/xenon.opensciencegrid.org/releases/nT/{self.image_tag}/anaconda/envs/XENONnT_development/lib"  # noqa
            ),
        )
        local.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ["USER"])
        local.add_profiles(Namespace.ENV, X509_USER_PROXY=os.environ["X509_USER_PROXY"])
        # local.add_profiles(Namespace.ENV, RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s")  # noqa
        if not self.debug:
            local.add_profiles(Namespace.ENV, RUCIO_ACCOUNT="production")
        # Improve python logging / suppress depreciation warnings (from gfal2 for example)
        local.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        local.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        # staging site
        staging = Site("staging")
        scratch_dir = Directory(
            Directory.SHARED_SCRATCH,
            path=f"/ospool/uc-shared/project/xenon/wf-scratch/{getpass.getuser()}",
        )
        scratch_dir.add_file_servers(
            FileServer(
                f"osdf:///ospool/uc-shared/project/xenon/wf-scratch/{getpass.getuser()}",
                Operation.ALL,
            )
        )
        staging.add_directories(scratch_dir)

        # staging site - davs
        staging_davs = Site("staging-davs")
        scratch_dir = Directory(
            Directory.SHARED_SCRATCH, path=f"/xenon/scratch/{getpass.getuser()}"
        )
        scratch_dir.add_file_servers(
            FileServer(
                f"gsidavs://xenon-gridftp.grid.uchicago.edu:2880/xenon/scratch/{getpass.getuser()}",
                Operation.ALL,
            )
        )
        staging_davs.add_directories(scratch_dir)

        # output on davs
        output_dir = Directory(Directory.LOCAL_STORAGE, path=f"/xenon/output/{getpass.getuser()}")
        output_dir.add_file_servers(
            FileServer(
                f"gsidavs://xenon-gridftp.grid.uchicago.edu:2880/xenon/output/{getpass.getuser()}",
                Operation.ALL,
            )
        )
        staging_davs.add_directories(output_dir)

        # condorpool
        condorpool = Site("condorpool")
        condorpool.add_profiles(Namespace.PEGASUS, style="condor")
        condorpool.add_profiles(Namespace.CONDOR, universe="vanilla")
        # We need the x509 proxy for Rucio transfers
        condorpool.add_profiles(Namespace.CONDOR, "x509userproxy", os.environ["X509_USER_PROXY"])
        condorpool.add_profiles(
            Namespace.CONDOR, "+SingularityImage", f'"{self.singularity_image}"'
        )

        # Ignore the site settings - the container will set all this up inside
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

        # Improve python logging / suppress depreciation warnings (from gfal2 for example)
        condorpool.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        condorpool.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        sc.add_sites(
            local,
            staging_davs,
            condorpool,
        )
        return sc

    def _generate_tc(self):
        return TransformationCatalog()

    def _generate_rc(self):
        return ReplicaCatalog()

    def make_tarballs(self):
        """Make tarballs of Ax-based packages if they are in editable user-
        installed mode."""
        tarballs = []
        tarball_paths = []
        for package_name in ["strax", "straxen", "cutax"]:
            _tarball = Tarball(self.generated_dir, package_name)
            if not Tarball.get_installed_git_repo(package_name):
                # Packages should not be non-editable user-installed
                if Tarball.is_user_installed(package_name):
                    raise RuntimeError(
                        f"You should install {package_name} in non-editable user-installed mode."
                    )
                # cutax is special because it is not installed in site-pacakges of the environment
                if package_name == "cutax":
                    if "CUTAX_LOCATION" not in os.environ:
                        raise RuntimeError(
                            "cutax should either be editable user-installed from a git repo "
                            "or patched by the software environment by CUTAX_LOCATION."
                        )
                    tarball = File(_tarball.tarball_name)
                    tarball_path = (
                        "/ospool/uc-shared/project/xenon/xenonnt/software"
                        f"/cutax/v{cutax.__version__}.tar.gz"
                    )
                else:
                    continue
            else:
                _tarball.create_tarball()
                tarball = File(_tarball.tarball_name)
                tarball_path = _tarball.tarball_path
                self.logger.warning(
                    f"Using tarball of user installed package {package_name} at {tarball_path}."
                )
            tarballs.append(tarball)
            tarball_paths.append(tarball_path)
        return tarballs, tarball_paths

    def _generate_workflow(self):
        """Use the Pegasus API to build an abstract graph of the workflow."""

        # Create a abstract dag
        wf = Workflow("xenonnt")
        # Initialize the catalogs
        sc = self._generate_sc()
        tc = self._generate_tc()
        rc = self._generate_rc()

        # Add executables to the wf-level transformation catalog
        for job_type, script in self._transformations_map.items():
            t = Transformation(
                job_type,
                site="local",
                pfn=f"file://{base_dir}/workflow/{script}",
                is_stageable=True,
            )
            tc.add_transformations(t)

        # scripts some exectuables might need
        processpy = File("process.py")
        rc.add_replica("local", "process.py", f"file://{base_dir}/workflow/process.py")
        combinepy = File("combine.py")
        rc.add_replica("local", "combine.py", f"file://{base_dir}/workflow/combine.py")

        # script to install packages
        installsh = File("install.sh")
        rc.add_replica(
            "local",
            "install.sh",
            f"file://{os.path.join(os.path.dirname(utilix.__file__), 'install.sh')}",
        )

        # Add common data files to the replica catalog
        xenon_config = File(".xenon_config")
        rc.add_replica("local", ".xenon_config", f"file://{uconfig.config_path}")

        # token needed for DB connection
        token = File(".dbtoken")
        rc.add_replica(
            "local", ".dbtoken", "file://" + os.path.join(os.environ["HOME"], ".dbtoken")
        )

        tarballs, tarball_paths = self.make_tarballs()
        for tarball, tarball_path in zip(tarballs, tarball_paths):
            rc.add_replica("local", tarball, tarball_path)

        # runs
        iterator = self._runlist if len(self._runlist) == 1 else tqdm(self._runlist)

        # Keep track of what runs we submit, useful for bookkeeping
        runlist = []
        for run_id in iterator:
            dbcfg = RunConfig(self.context, run_id, force=self.force)

            # Check if this run_id needs to be processed
            if len(dbcfg.needs_processed) > 0:
                self.logger.debug(f"Adding run_id {dbcfg.run_id:06d} to the workflow")
            else:
                self.logger.debug(
                    f"Run {dbcfg.run_id:06d} is already processed with context {self.context_name}"
                )
                continue

            # Will have combine jobs for all the PER_CHUNK_DTYPES we passed
            combine_jobs = {}

            # Get dtypes to process
            for dtype_i, dtype in enumerate(dbcfg.needs_processed):
                # These dtypes need raw data
                if dtype in NEED_RAW_DATA_DTYPES:
                    # Check that raw data exist for this run_id
                    if not all(
                        [dbcfg._raw_data_exists(raw_type=d) for d in dbcfg.depends_on(dtype)]
                    ):
                        self.logger.error(
                            f"Doesn't have raw data for {dtype} of run_id {run_id}, skipping"
                        )
                        continue

                self.logger.debug(f"Adding {dbcfg.key_for(dtype)}")
                if dbcfg.run_id not in runlist:
                    runlist.append(dbcfg.run_id)
                rses = dbcfg.dependencies_rses[dtype]
                if len(rses) == 0:
                    if dtype == "raw_records":
                        raise RuntimeError(
                            f"Unable to find a raw records location for {dbcfg.run_id:06d}"
                        )
                    else:
                        self.logger.warning(
                            f"No data found as the dependency of {dbcfg.key_for(dtype)}. "
                            f"Hopefully those will be created by the workflow."
                        )

                rses_specified = uconfig.get("Outsource", "raw_records_rse").split(",")
                # For standalone downloads, only target US
                if dbcfg.standalone_download:
                    rses = rses_specified

                # For low level data, we only want to run_id on sites
                # that we specified for raw_records_rse
                if dtype in NEED_RAW_DATA_DTYPES:
                    rses = list(set(rses) & set(rses_specified))
                    assert len(rses) > 0, (
                        f"No sites found for {dbcfg.key_for(dtype)}, "
                        "since no intersection between the available rses "
                        f"{rses} and the specified raw_records_rses {rses_specified}"
                    )

                sites_expression, desired_sites = dbcfg._determine_target_sites(rses)
                self.logger.debug(f"Site expression from RSEs list: {sites_expression}")
                self.logger.debug(
                    "XENON_DESIRED_Sites from RSEs list "
                    f"(mostly used for European sites): {desired_sites}"
                )

                requirements, requirements_us = dbcfg.get_requirements(rses)

                if dtype in PER_CHUNK_DTYPES:
                    # Add jobs, one for each input file
                    n_chunks = dbcfg.nchunks(dtype)

                    chunk_list = np.arange(n_chunks)
                    njobs = int(np.ceil(n_chunks / dbcfg.chunks_per_job))
                    chunk_str_list = []

                    # Loop over the chunks
                    for job_i in range(njobs):
                        chunks = chunk_list[
                            dbcfg.chunks_per_job * job_i : dbcfg.chunks_per_job * (job_i + 1)
                        ]
                        chunk_str = " ".join([f"{c}" for c in chunks])
                        chunk_str_list.append(chunk_str)

                    # Set up the combine job first -
                    # we can then add to that job inside the chunk file loop
                    # only need combine job for low-level stuff
                    combine_job = self._job(
                        "combine", disk=self.job_kwargs["combine"]["disk"], cores=4
                    )
                    # combine jobs must happen in the US
                    combine_job.add_profiles(Namespace.CONDOR, "requirements", requirements_us)
                    # priority is given in the order they were submitted
                    combine_job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)
                    combine_job.add_inputs(installsh, combinepy, xenon_config, token, *tarballs)
                    combine_output_tar_name = f"{dbcfg.key_for(dtype)}-combined.tar.gz"
                    combine_output_tar = File(combine_output_tar_name)
                    combine_job.add_outputs(
                        combine_output_tar, stage_out=(not self.upload_to_rucio)
                    )
                    combine_job.add_args(
                        dbcfg.run_id,
                        self.context_name,
                        self.xedocs_version,
                        combine_output_tar_name,
                        f"{self.upload_to_rucio}".lower(),
                        f"{self.update_db}".lower(),
                        *(f'"{c}"' for c in chunk_str_list),
                    )

                    wf.add_jobs(combine_job)
                    combine_jobs[dtype] = (combine_job, combine_output_tar)

                    # Loop over the chunks
                    for job_i in range(njobs):
                        chunk_str = chunk_str_list[job_i]

                        self.logger.debug(f" ... adding job for chunk files: {chunk_str}")

                        # standalone_download is a special case where we download data
                        # from rucio first, which is useful for testing and when using
                        # dedicated clusters with storage
                        if dbcfg.standalone_download:
                            tar_filename = File(f"{dbcfg.key_for(dtype)}-data-{job_i:04d}.tar.gz")
                            download_job = self._job(
                                "download", disk=self.job_kwargs["download"]["disk"]
                            )
                            download_job.add_profiles(
                                Namespace.CONDOR, "requirements", requirements
                            )
                            download_job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)
                            download_job.add_args(
                                dbcfg.run_id,
                                self.context_name,
                                self.xedocs_version,
                                dtype,
                                tar_filename,
                                "download-only",
                                f"{self.upload_to_rucio}".lower(),
                                f"{self.update_db}".lower(),
                                chunk_str,
                            )
                            download_job.add_inputs(
                                installsh, processpy, xenon_config, token, *tarballs
                            )
                            download_job.add_outputs(tar_filename, stage_out=False)
                            wf.add_jobs(download_job)

                        # output files
                        job_output_tar = File(f"{dbcfg.key_for(dtype)}-output-{job_i:04d}.tar.gz")
                        # Do we already have a local copy?
                        job_output_tar_local_path = os.path.join(
                            self.outputs_dir, f"{job_output_tar}"
                        )
                        if os.path.isfile(job_output_tar_local_path):
                            self.logger.info(
                                f" ... local copy found at: {job_output_tar_local_path}"
                            )
                            rc.add_replica(
                                "local", job_output_tar, f"file://{job_output_tar_local_path}"
                            )

                        # Add job
                        job = self._job(**self.job_kwargs[dtype])
                        if desired_sites:
                            # Give a hint to glideinWMS for the sites we want
                            # (mostly useful for XENON VO in Europe).
                            # Glideinwms is the provisioning system.
                            # It starts pilot jobs (glideins) at sites when you
                            # have idle jobs in the queue.
                            # Most of the jobs you run to the OSPool (Open Science Pool),
                            # but you do have a few sites where you have allocations at,
                            # and those are labeled XENON VO (Virtual Organization).
                            # The "+" has to be used by non-standard HTCondor attributes.
                            # The attribute has to have double quotes,
                            # otherwise HTCondor will try to evaluate it as an expression.
                            job.add_profiles(
                                Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"'
                            )
                        job.add_profiles(Namespace.CONDOR, "requirements", requirements)
                        job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)

                        job.add_args(
                            dbcfg.run_id,
                            self.context_name,
                            self.xedocs_version,
                            dtype,
                            job_output_tar,
                            "false" if not dbcfg.standalone_download else "no-download",
                            f"{self.upload_to_rucio}".lower(),
                            f"{self.update_db}".lower(),
                            chunk_str,
                        )

                        job.add_inputs(installsh, processpy, xenon_config, token, *tarballs)
                        job.add_outputs(job_output_tar, stage_out=(not self.upload_to_rucio))
                        wf.add_jobs(job)

                        # All strax jobs depend on the pre-flight or a download job,
                        # but pre-flight jobs have been outdated so it is not necessary.
                        if dbcfg.standalone_download:
                            job.add_inputs(tar_filename)
                            wf.add_dependency(job, parents=[download_job])

                        # Update combine job
                        combine_job.add_inputs(job_output_tar)
                        wf.add_dependency(job, children=[combine_job])

                        parent_combines = []
                        for d in dbcfg.depends_on(dtype):
                            if d in combine_jobs:
                                parent_combines.append(combine_jobs.get(d))

                        if len(parent_combines):
                            wf.add_dependency(job, parents=parent_combines)
                else:
                    # High level data.. we do it all on one job
                    # output files
                    job_output_tar = File(f"{dbcfg.key_for(dtype)}-output.tar.gz")

                    # Add job
                    job = self._job(**self.job_kwargs[dtype], cores=2)
                    # https://support.opensciencegrid.org/support/solutions/articles/12000028940-working-with-tensorflow-gpus-and-containers
                    job.add_profiles(Namespace.CONDOR, "requirements", requirements)
                    job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)

                    # Note that any changes to this argument list,
                    # also means process-wrapper.sh has to be updated
                    job.add_args(
                        dbcfg.run_id,
                        self.context_name,
                        self.xedocs_version,
                        dtype,
                        job_output_tar,
                        "false" if not dbcfg.standalone_download else "no-download",
                        f"{self.upload_to_rucio}".lower(),
                        f"{self.update_db}".lower(),
                    )

                    job.add_inputs(installsh, processpy, xenon_config, token, *tarballs)
                    # As long as we are giving outputs
                    job.add_outputs(job_output_tar, stage_out=True)
                    wf.add_jobs(job)

                    # If there are multiple levels to the workflow,
                    # need to have current process-wrapper.sh depend on previous combine-wrapper.sh

                    for d in dbcfg.depends_on(dtype):
                        if d in combine_jobs:
                            cj, cj_output = combine_jobs[d]
                            wf.add_dependency(job, parents=[cj])
                            job.add_inputs(cj_output)

        # Write the wf to stdout
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        wf.add_site_catalog(sc)
        wf.write(file=self.workflow)

        # Save the runlist
        np.savetxt(self.runlist, runlist, fmt="%0d")

        return wf

    def _plan_and_submit(self, wf):
        """Submit the workflow."""

        wf.plan(
            submit=not self.debug,
            cluster=["horizontal"],
            cleanup="none",
            sites=["condorpool"],
            verbose=3 if self.debug else 0,
            staging_sites={"condorpool": "staging-davs"},
            output_sites=["staging-davs"],
            dir=os.path.dirname(self.runs_dir),
            relative_dir=os.path.basename(self.runs_dir),
            **self.pegasus_config,
        )

    def submit(self, force=False):
        """Main interface to submitting a new workflow."""

        # Does workflow already exist?
        if os.path.exists(self.workflow_dir):
            if force:
                self.logger.warning(
                    f"Overwriting workflow at {self.workflow_dir}. Press ctrl+C now to stop."
                )
                time.sleep(10)
                shutil.rmtree(self.workflow_dir)
            else:
                raise RuntimeError(f"Workflow already exists at {self.workflow_dir}.")

        # Ensure we have a proxy with enough time left
        _validate_x509_proxy()

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.runs_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)

        # Generate the workflow
        wf = self._generate_workflow()

        if len(wf.jobs):
            # Submit the workflow
            self._plan_and_submit(wf)

        if self.debug:
            wf.graph(
                output=os.path.join(self.generated_dir, "workflow_graph.dot"), label="xform-id"
            )
            wf.graph(
                output=os.path.join(self.generated_dir, "workflow_graph.svg"), label="xform-id"
            )
