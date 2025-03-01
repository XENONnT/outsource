import os
import shutil
from utilix import uconfig, batchq
from outsource.submitter import Submitter


base_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))


# suggested default arguments for utilix.batchq.submit_job
BATCHQ_DEFAULT_ARGUMENTS = {
    "exclude_lc_nodes": True,
    "exclude_nodes": uconfig.get("Outsource", "rcc_exclude_nodes", fallback=None),
    "partition": uconfig.get("Outsource", "rcc_partition"),
    "qos": uconfig.get("Outsource", "rcc_partition"),
    "bind": uconfig.getlist("Outsource", "rcc_bind"),
}


CONTAINER_MEMORY_OVERHEAD = 6_000  # MB


class SubmitterSlurm(Submitter):

    # This flag will always be True for the Slurm submitter
    user_installed_packages = True

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
        debug=False,
        relay=False,
        **kwargs,
    ):
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
        )

        self.job_prefix = f"export WORKFLOW_DIR={self.workflow_dir}\n\n"
        x509_user_proxy = os.path.join(
            self.scratch_dir,
            os.path.basename(os.environ["X509_USER_PROXY"]),
        )
        self.job_prefix += f"export X509_USER_PROXY={x509_user_proxy}\n\n"

    def copy_files(self):
        """Copy the necessary files to the workflow directory."""

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)
        os.makedirs(self.scratch_dir, 0o755, exist_ok=True)
        # To prevent different jobs from overwriting each other's files
        os.makedirs(os.path.join(self.scratch_dir, "strax_data"), 0o555, exist_ok=True)
        os.makedirs(os.path.join(self.outputs_dir, "strax_data_rcc"), 0o755, exist_ok=True)
        os.makedirs(
            os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk"), 0o755, exist_ok=True
        )

        # Copy the workflow files
        shutil.copy2(f"{base_dir}/workflow/process-wrapper.sh", self.scratch_dir)
        shutil.copy2(f"{base_dir}/workflow/combine-wrapper.sh", self.scratch_dir)
        shutil.copy2(os.path.join(os.environ["HOME"], ".dbtoken"), self.scratch_dir)
        shutil.copy2(os.environ["XENON_CONFIG"], self.scratch_dir)
        shutil.copy2(os.environ["X509_USER_PROXY"], self.scratch_dir)
        shutil.copy2(f"{base_dir}/workflow/combine.py", self.scratch_dir)
        if self.resources_test:
            shutil.copy2(
                f"{base_dir}/workflow/test_resource_usage.py",
                os.path.join(self.scratch_dir, "process.py"),
            )
        else:
            shutil.copy2(
                f"{base_dir}/workflow/process.py",
                os.path.join(self.scratch_dir, "process.py"),
            )

    def _submit(self, job, jobname, log, **kwargs):
        """Submits job to batch queue which actually runs the analysis."""

        kwargs_to_pop = []
        for key, val in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, val)
                kwargs_to_pop.append(key)
        for kw in kwargs_to_pop:
            kwargs.pop(kw)

        job_id = batchq.submit_job(
            job,
            jobname=jobname,
            log=log,
            verbose=self.debug,
            dry_run=self.debug,
            **{**BATCHQ_DEFAULT_ARGUMENTS, **kwargs},
        )
        if job_id is None and not self.debug:
            raise RuntimeError("Job submission failed.")
        return job_id

    def add_lower_processing_job(self, label, level, dbcfg):
        """Add a per-chunk processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            raise RuntimeError(
                f"No data found as the dependency of {level['data_types'].not_processed}."
            )

        # Get the key for the job
        _key = self.get_key(dbcfg, level)
        jobname = f"{label}_{dbcfg._run_id}"

        # Loop over the chunks
        job_ids = []
        for job_i in range(len(level["chunks"])):
            self.logger.debug(f"Adding job for per-chunk processing: {level['chunks'][job_i]}")
            log = os.path.join(self.outputs_dir, f"{_key}-output-{job_i:04d}.log")

            # Add job
            input = os.path.join(self.scratch_dir, f"{dbcfg._run_id}-{job_i:04d}", "input")
            output = os.path.join(self.scratch_dir, f"{dbcfg._run_id}-{job_i:04d}", "output")
            staging_dir = os.path.join(
                self.scratch_dir, f"{dbcfg._run_id}-{job_i:04d}", "strax_data"
            )
            args = [f"{self.scratch_dir}/process-wrapper.sh"]
            args += [
                dbcfg.run_id,
                self.context_name,
                self.xedocs_version,
                level["chunks"][job_i][0],
                level["chunks"][job_i][1],
                f"{self.rucio_upload}".lower(),
                f"{self.rundb_update}".lower(),
                f"{self.ignore_processed}".lower(),
                f"{self.stage}".lower(),
                f"{self.remove_heavy}".lower(),
                input,
                output,
                staging_dir,
                "X",
            ]
            args += list(level["data_types"])
            args = [str(arg) for arg in args]
            # if self.resources_test:
            job = "set -e\n\n"
            job += f"export PEGASUS_DAG_JOB_ID={label}_ID{self._job_id:07}"
            job += "\n\n"
            job += self.job_prefix + " ".join(args)
            job += "\n\n"
            job += f"mv {output}/* {self.outputs_dir}/strax_data_rcc_per_chunk/"
            self.jobs.append(job)
            batchq_kwargs = {}
            batchq_kwargs["jobname"] = f"{jobname}-{job_i:04d}"
            batchq_kwargs["log"] = log
            batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
            batchq_kwargs["cpus_per_task"] = level["cores"]
            batchq_kwargs["mem_per_cpu"] = level["memory"][job_i] + CONTAINER_MEMORY_OVERHEAD
            batchq_kwargs["hours"] = eval(
                uconfig.get("Outsource", "pegasus_max_hours_lower", fallback=None)
            )
            job_id = self._submit(job, **batchq_kwargs)
            job_ids.append(job_id)
            self._job_id += 1

        suffix = "_".join(label.split("_")[1:])
        jobname = f"combine_{suffix}_{dbcfg._run_id}"
        log = os.path.join(self.outputs_dir, f"{_key}-output.log")
        input = os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk")
        output = os.path.join(self.scratch_dir, f"{dbcfg._run_id}", "output")
        staging_dir = os.path.join(self.scratch_dir, f"{dbcfg._run_id}", "strax_data")
        args = [f"{self.scratch_dir}/combine-wrapper.sh"]
        args += [
            dbcfg.run_id,
            self.context_name,
            self.xedocs_version,
            f"{self.rucio_upload}".lower(),
            f"{self.rundb_update}".lower(),
            f"{self.stage}".lower(),
            f"{self.remove_heavy}".lower(),
            input,
            output,
            staging_dir,
            "X",
            " ".join(map(str, [cs[-1] for cs in level["chunks"]])),
        ]
        args = [str(arg) for arg in args]
        job = "set -e\n\n"
        job += f"export PEGASUS_DAG_JOB_ID=combine_{suffix}_ID{self._job_id:07}"
        job += "\n\n"
        job += self.job_prefix + " ".join(args)
        job += "\n\n"
        job += f"mv {output}/* {self.outputs_dir}/strax_data_rcc/"
        self.jobs.append(job)
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        if not self.debug:
            batchq_kwargs["dependency"] = job_ids
        batchq_kwargs["cpus_per_task"] = level["combine_cores"]
        batchq_kwargs["mem_per_cpu"] = level["combine_memory"] + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = eval(
            uconfig.get("Outsource", "pegasus_max_hours_combine", fallback=None)
        )
        self.job_id = self._submit(job, **batchq_kwargs)
        self._job_id += 1

    def add_upper_processing_job(self, label, level, dbcfg):
        """Add a processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            self.logger.warning(
                f"No data found as the dependency of {tuple(level['data_types'])}. "
                f"Hopefully those will be created by the workflow."
            )

        # Get the key for the job
        _key = self.get_key(dbcfg, level)
        jobname = f"{label}_{dbcfg._run_id}"
        log = os.path.join(self.outputs_dir, f"{_key}-output.log")
        input = os.path.join(self.outputs_dir, "strax_data_rcc")
        output = os.path.join(self.scratch_dir, f"{dbcfg._run_id}", "output")
        staging_dir = os.path.join(self.scratch_dir, f"{dbcfg._run_id}", "strax_data")
        args = [f"{self.scratch_dir}/process-wrapper.sh"]
        args += [
            dbcfg.run_id,
            self.context_name,
            self.xedocs_version,
            -1,
            -1,
            f"{self.rucio_upload}".lower(),
            f"{self.rundb_update}".lower(),
            f"{self.ignore_processed}".lower(),
            f"{self.stage}".lower(),
            f"{self.remove_heavy}".lower(),
            input,
            output,
            staging_dir,
            "X",
        ]
        args += list(level["data_types"])
        args = [str(arg) for arg in args]
        job = "set -e\n\n"
        job += f"export PEGASUS_DAG_JOB_ID={label}_ID{self._job_id:07}"
        job += "\n\n"
        job += self.job_prefix + " ".join(args)
        job += "\n\n"
        job += f"mv {output}/* {self.outputs_dir}/strax_data_rcc/"
        self.jobs.append(job)
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        if not self.debug and self.job_id is not None:
            # self.job_id can be None if there is not lower or combine job
            batchq_kwargs["dependency"] = [self.job_id]
        batchq_kwargs["cpus_per_task"] = level["cores"]
        batchq_kwargs["mem_per_cpu"] = level["memory"] + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = eval(
            uconfig.get("Outsource", "pegasus_max_hours_upper", fallback=None)
        )
        self._submit(job, **batchq_kwargs)
        self._job_id += 1

    def _submit_run(self, group, label, level, dbcfg):
        """Submit a single run to the workflow."""
        if group == 0:
            self.add_lower_processing_job(label, level, dbcfg)
        else:
            self.add_upper_processing_job(label, level, dbcfg)

    def submit(self):
        """Submit the workflow to the batch queue."""

        self._job_id = 0
        self.jobs = []

        # Does workflow already exist?
        if os.path.exists(self.workflow_dir):
            raise RuntimeError(f"Workflow already exists at {self.workflow_dir}.")

        self.copy_files()

        runlist, summary = self._submit_runs()

        self.save_runlist(runlist)
        self.update_summary(summary)
        self.save_summary(summary)

        if self.debug:
            with open(os.path.join(self.generated_dir, "execution.sh"), "w") as f:
                f.write("\n\n".join(self.jobs) + "\n")
