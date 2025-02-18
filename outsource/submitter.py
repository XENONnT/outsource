import os
import sys
import json
import shutil
import getpass
from itertools import chain
from datetime import datetime, timezone
import numpy as np
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

from outsource.config import RunConfig
from outsource.meta import DETECTOR_DATA_TYPES
from outsource.utils import get_context, get_to_save_data_types


base_dir = os.path.abspath(os.path.dirname(__file__))


IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"
COMBINE_WRAPPER = "combine-wrapper.sh"
PROCESS_WRAPPER = "process-wrapper.sh"
UNTAR_WRAPPER = "untar.sh"

db = DB()


class Submitter:
    # Transformation map (high level name -> script)
    _transformations_map = {}
    _transformations_map.update(
        dict(
            zip(
                [f"combine_{det}" for det in DETECTOR_DATA_TYPES],
                [COMBINE_WRAPPER] * len(DETECTOR_DATA_TYPES),
            )
        )
    )
    _transformations_map.update(
        dict(
            zip(
                [f"untar_{det}" for det in DETECTOR_DATA_TYPES],
                [UNTAR_WRAPPER] * len(DETECTOR_DATA_TYPES),
            )
        )
    )
    _transformations_map.update(
        dict(
            zip(
                [f"lower_{det}" for det in DETECTOR_DATA_TYPES],
                [PROCESS_WRAPPER] * len(DETECTOR_DATA_TYPES),
            )
        )
    )
    _transformations_map.update(
        dict(
            zip(
                [f"upper_{det}" for det in DETECTOR_DATA_TYPES],
                [PROCESS_WRAPPER] * len(DETECTOR_DATA_TYPES),
            )
        )
    )

    user_installed_packages = False

    def __init__(
        self,
        runlist,
        context_name,
        xedocs_version,
        image,
        workflow_id=None,
        rucio_upload=False,
        rundb_update=False,
        ignore_processed=False,
        stage=False,
        resources_test=False,
        stage_out_lower=False,
        stage_out_combine=False,
        stage_out_upper=False,
        debug=False,
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
        self.context = get_context(context_name, self.xedocs_version)

        self.ignore_processed = ignore_processed
        self.stage = stage
        self.rucio_upload = rucio_upload
        self.rundb_update = rundb_update
        if not self.rucio_upload and self.rundb_update:
            raise RuntimeError("Rucio upload must be enabled when updating the RunDB.")
        self.resources_test = resources_test
        self.stage_out_lower = stage_out_lower
        self.stage_out_combine = stage_out_combine
        self.stage_out_upper = stage_out_upper
        self.local_transfer = self.stage_out_lower | self.stage_out_combine | self.stage_out_upper
        self.debug = debug

        # Load from XENON_CONFIG
        self.work_dir = uconfig.get("Outsource", "work_dir")

        # Need to know whether used self-installed packages before assigning the workflow_id
        self._setup_packages()
        # The current time will always be part of the workflow_id
        self._setup_workflow_id(workflow_id)
        # Pegasus workflow directory
        self.workflow_dir = os.path.join(self.work_dir, self.workflow_id)
        self.generated_dir = os.path.join(self.workflow_dir, "generated")
        self.runs_dir = os.path.join(self.workflow_dir, "runs")
        self.outputs_dir = os.path.join(self.workflow_dir, "outputs")
        self.scratch_dir = os.path.join(self.workflow_dir, "scratch")

    @property
    def x509_user_proxy(self):
        return os.path.join(self.generated_dir, ".x509_user_proxy")

    @property
    def dbtoken(self):
        return os.path.join(self.generated_dir, ".dbtoken")

    @property
    def xenon_config(self):
        return os.path.join(self.generated_dir, ".xenon_config")

    @property
    def workflow(self):
        return os.path.join(self.generated_dir, "workflow.yml")

    @property
    def runlist(self):
        return os.path.join(self.generated_dir, "runlist.txt")

    @property
    def summary(self):
        return os.path.join(self.generated_dir, "summary.json")

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
        pconfig["dagman.maxjobs"] = uconfig.getint("Outsource", "dagman_maxjobs", fallback=5_000)
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

    def _job(self, name, run_on_submit_node=False, cores=1, memory=2_000, disk=2_000):
        """Wrapper for a Pegasus job, also sets resource requirement profiles.

        Memory and disk in unit of MB.

        """
        job = Job(name)

        if run_on_submit_node:
            job.add_selector_profile(execution_site="local")
            # No other attributes on a local job
            return job

        job.add_profiles(Namespace.CONDOR, "request_cpus", cores)
        # Environment variables to control the number of threads
        job.add_profiles(Namespace.ENV, OMP_NUM_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, MKL_NUM_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, OPENBLAS_NUM_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, BLIS_NUM_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, NUMEXPR_NUM_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, GOTO_NUM_THREADS=f"{cores}")
        # Limiting CPU usage of TensorFlow
        job.add_profiles(Namespace.ENV, TF_NUM_INTRAOP_THREADS=f"{cores}")
        job.add_profiles(Namespace.ENV, TF_NUM_INTEROP_THREADS=f"{cores}")
        # For unknown reason, this does not work on limiting CPU usage of JAX
        job.add_profiles(
            Namespace.ENV,
            XLA_FLAGS=f"--xla_cpu_multi_thread_eigen=false intra_op_parallelism_threads={cores}",
        )
        # But this works, from https://github.com/jax-ml/jax/discussions/22739
        job.add_profiles(Namespace.ENV, NPROC=f"{cores}")
        job.add_profiles(Namespace.ENV, TF_ENABLE_ONEDNN_OPTS="0")

        # Increase memory/disk if the first attempt fails
        # The first dagman_static_retry memory/disk will be the same to the first attempt
        # The memory/disk will be increased by the number of retries afterwards
        dagman_static_retry = uconfig.getint("Outsource", "dagman_static_retry", fallback=0)
        if dagman_static_retry == 0:
            extra_retry = "(DAGNodeRetry + 1)"
        elif dagman_static_retry == 1:
            extra_retry = "DAGNodeRetry"
        else:
            extra_retry = f"(DAGNodeRetry - {dagman_static_retry - 1})"
        memory_str = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry <= {dagman_static_retry}, "
            f"{int(memory)}, "
            f"{extra_retry} * {int(memory)})"
        )
        disk_str = (
            "ifthenelse(isundefined(DAGNodeRetry) || "
            f"DAGNodeRetry <= {dagman_static_retry}, "
            f"{int(disk * 1_000)}, "
            f"{extra_retry} * {int(disk * 1_000)})"
        )
        job.add_profiles(Namespace.CONDOR, "request_disk", disk_str)
        job.add_profiles(Namespace.CONDOR, "request_memory", memory_str)

        # Stream output and error
        # Allows to see the output of the job in real time
        job.add_profiles(Namespace.CONDOR, "stream_output", "True")
        job.add_profiles(Namespace.CONDOR, "stream_error", "True")

        return job

    def _setup_workflow_id(self, workflow_id):
        """Set up the workflow ID."""
        # Determine a unique id for the workflow. If none passed, looks at the runlist.
        # If only one run_id is provided, use the run_id of that object + current time.
        # If more than one is provided, use current time.
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
        if workflow_id:
            workflow_id = (workflow_id, now)
        else:
            if len(self._runlist) == 1:
                workflow_id = (
                    self.image_tag,
                    self.context_name,
                    self.xedocs_version,
                    f"{self._runlist[0]:06d}",
                    now,
                )
            else:
                workflow_id = (self.image_tag, self.context_name, self.xedocs_version, now)
        if self.user_installed_packages:
            workflow_id = ("user", *workflow_id)
        self.workflow_id = "-".join(workflow_id)

    def _generate_sc(self):
        sc = SiteCatalog()

        # condorpool
        condorpool = Site("condorpool")
        condorpool.add_profiles(Namespace.PEGASUS, style="condor")
        condorpool.add_profiles(Namespace.CONDOR, universe="vanilla")
        # We need the x509 proxy for Rucio transfers
        condorpool.add_profiles(Namespace.CONDOR, "x509userproxy", self.x509_user_proxy)
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
        if not self.rucio_upload:
            condorpool.add_profiles(Namespace.ENV, RUCIO_ACCOUNT="production")

        # Improve python logging / suppress depreciation warnings (from gfal2 for example)
        condorpool.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        condorpool.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        # staging site - davs
        staging_davs = Site("staging-davs")
        scratch_dir_path = f"/xenon/scratch/{getpass.getuser()}/{self.workflow_id}"
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path=scratch_dir_path)
        scratch_dir.add_file_servers(
            FileServer(
                f"gsidavs://xenon-gridftp.grid.uchicago.edu:2880{scratch_dir_path}",
                Operation.ALL,
            )
        )
        staging_davs.add_directories(scratch_dir)

        # local site - this is the submit host
        local = Site("local")
        scratch_dir = Directory(Directory.SHARED_SCRATCH, path=self.scratch_dir)
        scratch_dir.add_file_servers(FileServer(f"file:///{self.scratch_dir}", Operation.ALL))
        output_dir = Directory(Directory.LOCAL_STORAGE, path=self.outputs_dir)
        output_dir.add_file_servers(FileServer(f"file:///{self.outputs_dir}", Operation.ALL))
        local.add_directories(scratch_dir, output_dir)

        # local.add_profiles(Namespace.ENV, HOME=os.environ["HOME"])
        local.add_profiles(Namespace.ENV, GLOBUS_LOCATION="")
        local.add_profiles(
            Namespace.ENV,
            PATH=(
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/envs/XENONnT_development/bin:"
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/condabin:"
                "/usr/bin:/bin"
            ),
        )
        local.add_profiles(
            Namespace.ENV,
            LD_LIBRARY_PATH=(
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/envs/XENONnT_development/lib64:"
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/envs/XENONnT_development/lib"
            ),
        )
        local.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ["USER"])
        local.add_profiles(Namespace.ENV, X509_USER_PROXY=self.x509_user_proxy)
        # local.add_profiles(
        #     Namespace.ENV,
        #     RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s",
        # )
        if not self.rucio_upload:
            local.add_profiles(Namespace.ENV, RUCIO_ACCOUNT="production")
        # Improve python logging / suppress depreciation warnings (from gfal2 for example)
        local.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        local.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")

        if not self.local_transfer:
            output_dir_path = f"/xenon/output/{getpass.getuser()}/{self.workflow_id}"
            output_dir = Directory(Directory.LOCAL_STORAGE, path=output_dir_path)
            output_dir.add_file_servers(
                FileServer(
                    f"gsidavs://xenon-gridftp.grid.uchicago.edu:2880{output_dir_path}",
                    Operation.ALL,
                )
            )
            staging_davs.add_directories(output_dir)

        sc.add_sites(local, staging_davs, condorpool)
        return sc

    def _generate_tc(self):
        return TransformationCatalog()

    def _generate_rc(self):
        return ReplicaCatalog()

    def _setup_packages(self):
        """Get the list of user-installed packages."""
        package_names = uconfig.getlist("Outsource", "user_install_package", fallback=[])
        if "cutax" not in package_names:
            raise RuntimeError(
                "cutax must be in the list of user_install_package "
                f"in the XENON_CONFIG configuration, but got {package_names}."
            )
        # Check if the package in not in the official software environment
        check_package_names = uconfig.getlist(
            "Outsource", "check_user_install_package", fallback=[]
        )
        check_package_names += [
            "strax",
            "straxen",
            "cutax",
            "rucio",
            "utilix",
            "admix",
            "outsource",
        ]
        for package_name in set(check_package_names) - set(package_names):
            if Tarball.get_installed_git_repo(package_name) or Tarball.is_user_installed(
                package_name
            ):
                raise RuntimeError(
                    f"{package_name} should be either in user_install_package "
                    "in the XENON_CONFIG configuration after being installed in editable mode, "
                    "because it is not in the official software environment. "
                    "Or uninstalled from the user-installed environment."
                )
        self.package_names = package_names
        # Get a flag of whether there are user-installed packages
        # to determine the workflow_id later
        for package_name in self.package_names:
            if Tarball.get_installed_git_repo(package_name):
                self.user_installed_packages = True
                break

    def make_tarballs(self):
        """Make tarballs of Ax-based packages if they are in editable user-installed mode."""
        tarballs = []
        tarball_paths = []

        # Install the specified user-installed packages
        for package_name in self.package_names:
            _tarball = Tarball(self.generated_dir, package_name)
            if Tarball.get_installed_git_repo(package_name):
                if self.rucio_upload:
                    raise RuntimeError(
                        f"When using user_install_package, rucio_upload must be False!"
                    )
                _tarball.create_tarball()
                tarball = File(_tarball.tarball_name)
                tarball_path = _tarball.tarball_path
                self.logger.warning(
                    f"Using tarball of user installed package {package_name} at {tarball_path}."
                )
            else:
                # Packages should not be non-editable user-installed
                if Tarball.is_user_installed(package_name):
                    raise RuntimeError(
                        f"You should not install {package_name} in "
                        "non-editable user-installed mode."
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
                        f"/cutax/v{cutax.__version__.replace('.', '-')}.tar.gz"
                    )
                else:
                    continue
            tarballs.append(tarball)
            tarball_paths.append(tarball_path)
        return tarballs, tarball_paths

    def get_rse_sites(self, dbcfg, rses, per_chunk=False):
        """Get the desired sites and requirements for the data_type."""
        raw_records_rses = uconfig.getlist("Outsource", "raw_records_rses")

        if per_chunk:
            # For low level data, we only want to run_id on sites
            # that we specified for raw_records_rses
            rses = list(set(rses) & set(raw_records_rses))
            if not len(rses):
                raise RuntimeError(
                    f"No sites found since no intersection between the available rses "
                    f"{rses} and the specified raw_records_rses {raw_records_rses}"
                )

        desired_sites, requirements = dbcfg.get_requirements(rses)
        return desired_sites, requirements

    def get_key(self, dbcfg, level):
        """Get the key for the output files and check the file name."""
        # output files
        _key = "-".join(
            [dbcfg._run_id] + [f"{d}-{dbcfg.key_for(d).lineage_hash}" for d in level["data_types"]]
        )
        # Sometimes the filename can be too long
        if len(_key) > 255 - len("untar--output.tar.gz.log"):
            self.logger.warning(f"Filename {_key} is too long, will not include hash in it.")
            _key = "-".join([dbcfg._run_id] + list(level["data_types"]))
        if len(_key) > 255 - len("untar--output.tar.gz.log"):
            raise RuntimeError(f"Filename {_key} is still too long.")
        return _key

    def add_higher_processing_job(
        self,
        workflow,
        label,
        level,
        dbcfg,
        installsh,
        processpy,
        combinepy,
        xenon_config,
        dbtoken,
        tarballs,
    ):
        """Add a processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            self.logger.warning(
                f"No data found as the dependency of {tuple(level['data_types'])}. "
                f"Hopefully those will be created by the workflow."
            )

        desired_sites, requirements = self.get_rse_sites(dbcfg, rses, per_chunk=False)

        # High level data.. we do it all on one job
        _key = self.get_key(dbcfg, level)
        job_tar = File(f"{_key}-output.tar.gz")

        # Add job
        job = self._job(
            name=label, cores=level["cores"], memory=level["memory"], disk=level["disk"]
        )
        # https://support.opensciencegrid.org/support/solutions/articles/12000028940-working-with-tensorflow-gpus-and-containers
        job.add_profiles(Namespace.CONDOR, "requirements", requirements)
        job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)

        # Note that any changes to this argument list,
        # also means process-wrapper.sh has to be updated
        job.add_args(
            dbcfg.run_id,
            self.context_name,
            self.xedocs_version,
            -1,
            -1,
            f"{self.rucio_upload}".lower(),
            f"{self.rundb_update}".lower(),
            f"{self.ignore_processed}".lower(),
            f"{self.stage}".lower(),
            job_tar,
            *level["data_types"],
        )

        job.add_inputs(installsh, processpy, xenon_config, dbtoken, *tarballs)
        job.add_outputs(job_tar, stage_out=self.stage_out_upper)
        job.set_stdout(File(f"{job_tar}.log"), stage_out=True)

        # If there are multiple levels to the workflow, need to
        # have current process-wrapper.sh depend on previous combine-wrapper.sh

        if self.stage_out_upper:
            suffix = "_".join(label.split("_")[1:])
            untar_job = self._job(name=f"untar_{suffix}", run_on_submit_node=True)
            untar_job.add_inputs(job_tar)
            untar_job.add_args(job_tar, self.outputs_dir)
            untar_job.set_stdout(File(f"untar-{job_tar}.log"), stage_out=True)
            workflow.add_jobs(untar_job)

        return job, job_tar

    def add_lower_processing_job(
        self,
        workflow,
        label,
        level,
        dbcfg,
        installsh,
        processpy,
        combinepy,
        xenon_config,
        dbtoken,
        tarballs,
    ):
        """Add a per-chunk processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            raise RuntimeError(
                f"No data found as the dependency of {level['data_types'].not_processed}."
            )

        desired_sites, requirements = self.get_rse_sites(dbcfg, rses, per_chunk=True)

        suffix = "_".join(label.split("_")[1:])
        # Set up the combine job first -
        # we can then add to that job inside the chunk file loop
        # only need combine job for low-level stuff
        combine_job = self._job(
            name=f"combine_{suffix}",
            cores=level["combine_cores"],
            memory=level["combine_memory"],
            disk=level["combine_disk"],
        )
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
            combine_job.add_profiles(Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"')
        combine_job.add_profiles(Namespace.CONDOR, "requirements", requirements)
        # priority is given in the order they were submitted
        combine_job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)
        combine_job.add_inputs(installsh, combinepy, xenon_config, dbtoken, *tarballs)
        _key = self.get_key(dbcfg, level)
        combine_tar = File(f"{_key}-output.tar.gz")
        combine_job.add_outputs(combine_tar, stage_out=self.stage_out_combine)
        combine_job.set_stdout(File(f"{combine_tar}.log"), stage_out=True)
        combine_job.add_args(
            dbcfg.run_id,
            self.context_name,
            self.xedocs_version,
            f"{self.rucio_upload}".lower(),
            f"{self.rundb_update}".lower(),
            f"{self.stage}".lower(),
            combine_tar,
            " ".join(map(str, [cs[-1] for cs in level["chunks"]])),
        )

        if self.stage_out_combine:
            untar_job = self._job(name=f"untar_{suffix}", run_on_submit_node=True)
            untar_job.add_inputs(combine_tar)
            untar_job.add_args(combine_tar, self.outputs_dir)
            untar_job.set_stdout(File(f"untar-{combine_tar}.log"), stage_out=True)
            workflow.add_jobs(untar_job)

        # Loop over the chunks
        for job_i in range(len(level["chunks"])):
            self.logger.debug(f"Adding job for per-chunk processing: {level['chunks'][job_i]}")

            # output files
            job_tar = File(f"{_key}-output-{job_i:04d}.tar.gz")

            # Add job
            job = self._job(
                name=label,
                cores=level["cores"],
                memory=level["memory"][job_i],
                disk=level["disk"][job_i],
            )
            if desired_sites:
                job.add_profiles(Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"')
            job.add_profiles(Namespace.CONDOR, "requirements", requirements)
            job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)

            job.add_args(
                dbcfg.run_id,
                self.context_name,
                self.xedocs_version,
                level["chunks"][job_i][0],
                level["chunks"][job_i][1],
                f"{self.rucio_upload}".lower(),
                f"{self.rundb_update}".lower(),
                f"{self.ignore_processed}".lower(),
                f"{self.stage}".lower(),
                job_tar,
                *level["data_types"],
            )

            job.add_inputs(installsh, processpy, xenon_config, dbtoken, *tarballs)
            job.add_outputs(job_tar, stage_out=self.stage_out_lower)
            job.set_stdout(File(f"{job_tar}.log"), stage_out=True)

            workflow.add_jobs(job)

            # Update combine job
            combine_job.add_inputs(job_tar)

        return combine_job, combine_tar

    def add_transformations(self, tc, source, destination, name):
        """Add transformations to the transformation catalog."""
        shutil.copy(source, destination)
        t = Transformation(
            name,
            site="local",
            pfn=f"file://{destination}",
            is_stageable=True,
        )
        tc.add_transformations(t)

    def add_replica(self, rc, source, destination, site, lfn):
        """Add a replica to the replica catalog."""
        shutil.copy(source, destination)
        rc.add_replica(site, lfn, f"file://{destination}")

    def _generate_workflow(self):
        """Use the Pegasus API to build an abstract graph of the workflow."""

        # Create a abstract dag
        workflow = Workflow("outsource_workflow")
        # Initialize the catalogs
        sc = self._generate_sc()
        tc = self._generate_tc()
        rc = self._generate_rc()

        # Add executables to the workflow-level transformation catalog
        for job_type, script in self._transformations_map.items():
            self.add_transformations(
                tc,
                f"{base_dir}/workflow/{script}",
                os.path.join(self.generated_dir, script),
                job_type,
            )

        # scripts some exectuables might need
        if self.resources_test:
            self.add_replica(
                rc,
                f"{base_dir}/workflow/test_resource_usage.py",
                os.path.join(self.generated_dir, "test_resource_usage.py"),
                "local",
                "process.py",
            )
        else:
            self.add_replica(
                rc,
                f"{base_dir}/workflow/process.py",
                os.path.join(self.generated_dir, "process.py"),
                "local",
                "process.py",
            )
        if self.resources_test:
            self.add_replica(
                rc,
                f"{base_dir}/workflow/test_resource_usage.py",
                os.path.join(self.generated_dir, "combine.py"),
                "local",
                "combine.py",
            )
        else:
            self.add_replica(
                rc,
                f"{base_dir}/workflow/combine.py",
                os.path.join(self.generated_dir, "combine.py"),
                "local",
                "combine.py",
            )

        # script to install packages
        self.add_replica(
            rc,
            os.path.join(os.path.dirname(utilix.__file__), "install.sh"),
            os.path.join(self.generated_dir, "install.sh"),
            "local",
            "install.sh",
        )

        # Avoid its change after the job submission
        self.add_replica(
            rc,
            uconfig.get("Outsource", "x509_user_proxy"),
            self.x509_user_proxy,
            "local",
            ".x509_user_proxy",
        )
        self.add_replica(
            rc, os.path.join(os.environ["HOME"], ".dbtoken"), self.dbtoken, "local", ".dbtoken"
        )
        self.add_replica(rc, uconfig.config_path, self.xenon_config, "local", ".xenon_config")

        tarballs, tarball_paths = self.make_tarballs()
        for tarball, tarball_path in zip(tarballs, tarball_paths):
            rc.add_replica("local", tarball, tarball_path)

        # Keep track of what runs we submit, useful for bookkeeping
        runlist = set()
        summary = dict()
        for run_id in self._runlist:
            dbcfg = RunConfig(self.context, run_id, ignore_processed=self.ignore_processed)
            summary[dbcfg._run_id] = dbcfg.data_types
            self.logger.info(f"Adding {dbcfg._run_id} to the workflow.")

            for detector in dbcfg.detectors:
                # Check if this run_id needs to be processed
                if not list(
                    chain.from_iterable(
                        v["data_types"] for v in dbcfg.data_types[detector].values()
                    )
                ):
                    self.logger.debug(
                        f"Run {dbcfg._run_id} detector {detector} is already processed with "
                        f"context {self.context_name} xedocs_version {self.xedocs_version}."
                    )
                    continue

                # Get data_types to process
                combine_tar = None
                for group, (label, level) in enumerate(dbcfg.data_types[detector].items()):
                    if not level["data_types"].not_processed:
                        self.logger.debug(
                            f"Run {dbcfg._run_id} group {label} is already processed with "
                            f"context {self.context_name} xedocs_version {self.xedocs_version}."
                        )
                        continue
                    # Check that raw data exist for this run_id
                    if group == 0:
                        # There will be at most one in data_types
                        data_types = list(level["data_types"].keys())
                        for data_type in data_types:
                            depends_on = dbcfg.depends_on(data_type, lower=not group)
                            for _depends_on in depends_on:
                                if not dbcfg.dependency_exists(data_type=_depends_on):
                                    raise RuntimeError(
                                        f"Unable to find the raw data for {data_types}."
                                    )

                    runlist |= {dbcfg.run_id}

                    args = (
                        workflow,
                        label,
                        level,
                        dbcfg,
                        File("install.sh"),
                        File("process.py"),
                        File("combine.py"),
                        File(".xenon_config"),
                        File(".dbtoken"),
                        tarballs,
                    )
                    if group == 0:
                        combine_job, combine_tar = self.add_lower_processing_job(*args)
                        workflow.add_jobs(combine_job)
                    else:
                        job, job_tar = self.add_higher_processing_job(*args)
                        if combine_tar:
                            job.add_inputs(combine_tar)
                        workflow.add_jobs(job)

        # Write the workflow to stdout
        workflow.add_replica_catalog(rc)
        workflow.add_transformation_catalog(tc)
        workflow.add_site_catalog(sc)
        workflow.write(file=self.workflow)

        # Save the runlist
        np.savetxt(self.runlist, sorted(runlist), fmt="%0d")

        # Save the job summary
        summary["include_data_types"] = sorted(
            uconfig.getlist("Outsource", "include_data_types"),
            key=lambda item: self.context.tree_levels[item]["order"],
        )
        summary["save_data_types"] = sorted(
            get_to_save_data_types(
                self.context,
                list(
                    set().union(
                        *[v.get("submitted", []) for v in summary.values() if isinstance(v, dict)]
                    )
                ),
                rm_lower=False,
            ),
            key=lambda item: self.context.tree_levels[item]["order"],
        )
        summary["save_data_types"] = [
            "-".join(str(self.context.key_for("0", d)).split("-")[1:])
            for d in summary["save_data_types"]
        ]
        with open(self.summary, mode="w") as f:
            f.write(json.dumps(summary, indent=4))

        return workflow

    def _plan_and_submit(self, workflow):
        """Submit the workflow."""

        workflow.plan(
            submit=not self.debug,
            cluster=["horizontal"],
            cleanup="none",
            sites=["condorpool"],
            verbose=3 if self.debug else 0,
            staging_sites={"condorpool": "staging-davs"},
            output_sites=["local" if self.local_transfer else "staging-davs"],
            dir=os.path.dirname(self.runs_dir),
            relative_dir=os.path.basename(self.runs_dir),
            **self.pegasus_config,
        )

    def submit(self):
        """Main interface to submitting a new workflow."""

        # Does workflow already exist?
        if os.path.exists(self.workflow_dir):
            raise RuntimeError(f"Workflow already exists at {self.workflow_dir}.")

        # Ensure we have a proxy with enough time left
        _validate_x509_proxy(
            min_valid_hours=eval(uconfig.get("Outsource", "min_valid_hours", fallback=96))
        )

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.runs_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)

        # Generate the workflow
        workflow = self._generate_workflow()

        if len(workflow.jobs):
            # Submit the workflow
            self._plan_and_submit(workflow)

        if self.debug:
            workflow.graph(
                output=os.path.join(self.generated_dir, "workflow_graph.dot"), label="xform-id"
            )
            # workflow.graph(
            #     output=os.path.join(self.generated_dir, "workflow_graph.svg"), label="xform-id"
            # )
