import os
import shutil
import getpass
import utilix
from utilix import DB, uconfig
from utilix.x509 import _validate_x509_proxy
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

from outsource.utils import get_context
from outsource.submitter import Submitter
from outsource.meta import DETECTOR_DATA_TYPES
from outsource.utils import get_resources_retry

base_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))


IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"
COMBINE_WRAPPER = "combine-wrapper.sh"
PROCESS_WRAPPER = "process-wrapper.sh"
UNTAR_WRAPPER = "untar.sh"

db = DB()

RESOURCES_RETRY = get_resources_retry()


class SubmitterHTCondor(Submitter):
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
        remove_heavy=True,
        resources_test=False,
        stage_out_lower=False,
        stage_out_combine=False,
        stage_out_upper=False,
        debug=False,
        relay=False,
        resubmit=False,
        **kwargs,
    ):
        if relay:
            raise ValueError(
                "HTCondor mode must be used without --relay flag. Please remove --relay"
            )
        if resubmit:
            raise ValueError(
                "HTCondor mode must be used without --resubmit flag. Please remove --resubmit"
            )

        super().__init__(
            runlist=runlist,
            context_name=context_name,
            xedocs_version=xedocs_version,
            image=image,
            workflow_id=workflow_id,
            rucio_upload=rucio_upload,
            rundb_update=rundb_update,
            ignore_processed=ignore_processed,
            stage=stage,
            remove_heavy=remove_heavy,
            resources_test=resources_test,
            debug=debug,
            relay=relay,
            resubmit=resubmit,
        )

        self.stage_out_lower = stage_out_lower
        self.stage_out_combine = stage_out_combine
        self.stage_out_upper = stage_out_upper
        self.local_transfer = self.stage_out_lower | self.stage_out_combine | self.stage_out_upper

        # Pegasus workflow directory
        self.runs_dir = os.path.join(self.workflow_dir, "runs")

        self.context = get_context(context_name, self.xedocs_version)

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
        # make sure we do not start too many jobs at the same time
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
        # User installed packages
        user_install_package = " ".join(self.user_install_package)
        job.add_profiles(Namespace.ENV, "USER_INSTALL_PACKAGE", user_install_package)
        # For unknown reason, this does not work on limiting CPU usage of JAX
        job.add_profiles(
            Namespace.ENV,
            XLA_FLAGS=f"--xla_cpu_multi_thread_eigen=false intra_op_parallelism_threads={cores}",
        )
        # But this works, from https://github.com/jax-ml/jax/discussions/22739
        job.add_profiles(Namespace.ENV, NPROC=f"{cores}")
        job.add_profiles(Namespace.ENV, TF_ENABLE_ONEDNN_OPTS="0")
        job.add_profiles(
            Namespace.CONDOR, "request_memory", RESOURCES_RETRY.format(resources=int(memory))
        )
        job.add_profiles(
            Namespace.CONDOR, "request_disk", RESOURCES_RETRY.format(resources=int(disk * 1_000))
        )
        # https://htcondor.readthedocs.io/en/latest/users-manual/automatic-job-management.html
        # https://htcondor.readthedocs.io/en/latest/apis/python-bindings/tutorials/HTCondor-Introduction.html
        periodic_hold = []
        max_hours_idle = uconfig.get("Outsource", "pegasus_max_hours_idle", fallback=None)
        if max_hours_idle:
            max_hours_idle = RESOURCES_RETRY.format(resources=eval(max_hours_idle) * 3600)
            periodic_hold.append(
                f"JobStatus == 1 && (time() - EnteredCurrentStatus) > {max_hours_idle}"
            )
        max_hours_run = uconfig.get(
            "Outsource", f"pegasus_max_hours_{name.split('_')[0]}", fallback=None
        )
        if max_hours_run:
            max_hours_run = RESOURCES_RETRY.format(resources=eval(max_hours_run) * 3600)
            periodic_hold.append(
                f"JobStatus == 2 && (time() - EnteredCurrentStatus) > {max_hours_run}"
            )
        if periodic_hold:
            if len(periodic_hold) == 1:
                periodic_hold = periodic_hold[0]
            else:
                periodic_hold = [f"({p})" for p in periodic_hold]
                periodic_hold = " || ".join(periodic_hold)
            job.add_profiles(Namespace.CONDOR, "periodic_hold", periodic_hold)

        # Stream output and error
        # Allows to see the output of the job in real time
        if uconfig.getboolean("Outsource", "stream_output", fallback=False):
            job.add_profiles(Namespace.CONDOR, "stream_output", "True")
            job.add_profiles(Namespace.CONDOR, "stream_error", "True")

        return job

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
                f"{self.image_tag}/anaconda/envs/XENONnT_{self.image_tag}/bin:"
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/condabin:"
                "/usr/bin:/bin"
            ),
        )
        local.add_profiles(
            Namespace.ENV,
            LD_LIBRARY_PATH=(
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/envs/XENONnT_{self.image_tag}/lib64:"
                "/cvmfs/xenon.opensciencegrid.org/releases/nT/"
                f"{self.image_tag}/anaconda/envs/XENONnT_{self.image_tag}/lib"
            ),
        )
        local.add_profiles(Namespace.ENV, PEGASUS_SUBMITTING_USER=os.environ["USER"])
        local.add_profiles(Namespace.ENV, X509_USER_PROXY=self.x509_user_proxy)
        # local.add_profiles(
        #     Namespace.ENV,
        #     RUCIO_LOGGING_FORMAT="%(asctime)s  %(levelname)s  %(message)s",
        # )
        # Improve python logging / suppress depreciation warnings (from gfal2 for example)
        local.add_profiles(Namespace.ENV, PYTHONUNBUFFERED="1")
        local.add_profiles(Namespace.ENV, PYTHONWARNINGS="ignore::DeprecationWarning")
        # forbidding Pegasus to modify PYTHONPATH
        local.add_profiles(Namespace.ENV, PEGASUS_UPDATE_PYTHONPATH="0")

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

    def get_rse_sites(self, dbcfg, rses, per_chunk=False):
        """Get the desired sites and requirements for the data_type."""
        raw_records_rses = uconfig.getlist("Outsource", "raw_records_rses")

        if per_chunk:
            # For low level data, we only want to run_id on sites
            # that we specified for raw_records_rses
            rses = sorted(set(rses) & set(raw_records_rses))
            if not len(rses):
                raise RuntimeError(
                    f"No sites found since no intersection between the available rses "
                    f"{rses} and the specified raw_records_rses {raw_records_rses}"
                )

        desired_sites, requirements, site_ranks = dbcfg.get_requirements(rses)
        return desired_sites, requirements, site_ranks

    def add_upper_processing_job(
        self,
        label,
        level,
        dbcfg,
        installsh,
        processpy,
        combinepy,
        xenon_config,
        dbtoken,
    ):
        """Add a processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            self.logger.warning(
                f"No data found as the dependency of {tuple(level['data_types'])}. "
                f"Hopefully those will be created by the workflow."
            )

        desired_sites, requirements, site_ranks = self.get_rse_sites(dbcfg, rses, per_chunk=False)

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
        if desired_sites:
            job.add_profiles(Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"')

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
            f"{self.combine_tar is None}".lower(),
            f"{self.remove_heavy}".lower(),
            "input",
            "output",
            "strax_data",
            job_tar,
            *level["data_types"],
        )

        job.add_inputs(installsh, processpy, xenon_config, dbtoken, *self.tarballs)
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
            self.workflow.add_jobs(untar_job)

        return job, job_tar

    def add_lower_processing_job(
        self,
        label,
        level,
        dbcfg,
        installsh,
        processpy,
        combinepy,
        xenon_config,
        dbtoken,
    ):
        """Add a per-chunk processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            raise RuntimeError(
                f"No data found as the dependency of {level['data_types'].not_processed}."
            )

        desired_sites, requirements, site_ranks = self.get_rse_sites(dbcfg, rses, per_chunk=True)

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
        combine_job.add_profiles(Namespace.CONDOR, "requirements", requirements)
        # priority is given in the order they were submitted
        combine_job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)
        if desired_sites:
            combine_job.add_profiles(Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"')

        combine_job.add_inputs(installsh, combinepy, xenon_config, dbtoken, *self.tarballs)
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
            "input",
            "output",
            "strax_data",
            combine_tar,
            " ".join(map(str, [cs[-1] for cs in level["chunks"]])),
        )

        if self.stage_out_combine:
            untar_job = self._job(name=f"untar_{suffix}", run_on_submit_node=True)
            untar_job.add_inputs(combine_tar)
            untar_job.add_args(combine_tar, self.outputs_dir)
            untar_job.set_stdout(File(f"untar-{combine_tar}.log"), stage_out=True)
            self.workflow.add_jobs(untar_job)

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
                # Give a hint to glideinWMS for the sites we want
                # (mostly useful for XENON VO in Europe).
                # Glideinwms is the provisioning system.
                # It starts pilot jobs (glideins) at sites when you have idle jobs in the queue.
                # Most of the jobs you run to the OSPool (Open Science Pool),
                # but you do have a few sites where you have allocations at,
                # and those are labeled XENON VO (Virtual Organization).
                # The "+" has to be used by non-standard HTCondor attributes.
                # The attribute has to have double quotes,
                # otherwise HTCondor will try to evaluate it as an expression.
                job.add_profiles(Namespace.CONDOR, "+XENON_DESIRED_Sites", f'"{desired_sites}"')
            job.add_profiles(Namespace.CONDOR, "requirements", requirements)
            job.add_profiles(Namespace.CONDOR, "priority", dbcfg.priority)
            # This allows us to set higher priority for EU sites when we have data in EU
            if site_ranks:
                job.add_profiles(Namespace.CONDOR, "rank", site_ranks)

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
                "true",
                f"{self.remove_heavy}".lower(),
                "input",
                "output",
                "strax_data",
                job_tar,
                *level["data_types"],
            )

            job.add_inputs(installsh, processpy, xenon_config, dbtoken, *self.tarballs)
            job.add_outputs(job_tar, stage_out=self.stage_out_lower)
            job.set_stdout(File(f"{job_tar}.log"), stage_out=True)

            self.workflow.add_jobs(job)

            # Update combine job
            combine_job.add_inputs(job_tar)

        return combine_job, combine_tar

    def add_transformations(self, tc, source, destination, name):
        """Add transformations to the transformation catalog."""
        shutil.copy2(source, destination)
        t = Transformation(
            name,
            site="local",
            pfn=f"file://{destination}",
            is_stageable=True,
        )
        tc.add_transformations(t)

    def add_replica(self, rc, source, destination, site, lfn):
        """Add a replica to the replica catalog."""
        shutil.copy2(source, destination)
        rc.add_replica(site, lfn, f"file://{destination}")

    def _submit_run(self, group, label, level, dbcfg):
        """Submit a single run to the workflow."""
        args = (
            label,
            level,
            dbcfg,
            File("install.sh"),
            File("process.py"),
            File("combine.py"),
            File(".xenon_config"),
            File(".dbtoken"),
        )
        if group == 0:
            combine_job, self.combine_tar = self.add_lower_processing_job(*args)
            self.workflow.add_jobs(combine_job)
        else:
            job, job_tar = self.add_upper_processing_job(*args)
            if self.combine_tar:
                job.add_inputs(self.combine_tar)
            self.workflow.add_jobs(job)

    def _generate_workflow(self):
        """Use the Pegasus API to build an abstract graph of the workflow."""

        # Create a abstract dag
        self.workflow = Workflow("outsource_workflow")
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

        for tarball, tarball_path in zip(self.tarballs, self.tarball_paths):
            rc.add_replica("local", File(tarball), tarball_path)

        runlist, summary = self._submit_runs()

        # Write the workflow to stdout
        self.workflow.add_replica_catalog(rc)
        self.workflow.add_transformation_catalog(tc)
        self.workflow.add_site_catalog(sc)
        self.workflow.write(file=self._workflow)

        # Save the runlist and summary
        self.save_runlist(runlist)
        self.update_summary(summary)
        self.save_summary(summary)

    def _plan_and_submit(self):
        """Submit the workflow."""

        self.workflow.plan(
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
        # All submitters need to make tarballs
        self.make_tarballs()

        # Ensure we have a proxy with enough time left
        _validate_x509_proxy(
            min_valid_hours=eval(uconfig.get("Outsource", "x509_min_valid_hours", fallback=96))
        )

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)
        os.makedirs(self.runs_dir, 0o755, exist_ok=True)

        # Generate the workflow
        self._generate_workflow()

        if len(self.workflow.jobs):
            # Submit the workflow
            self._plan_and_submit()

        if self.debug:
            self.workflow.graph(
                output=os.path.join(self.generated_dir, "workflow_graph.dot"), label="xform-id"
            )
            # self.workflow.graph(
            #     output=os.path.join(self.generated_dir, "workflow_graph.svg"), label="xform-id"
            # )
