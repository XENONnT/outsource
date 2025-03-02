import os
import json
import shutil
from copy import deepcopy
import utilix
from utilix import uconfig, batchq
import strax
from outsource.utils import get_context, get_chunk_number
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

        # commands to execute before the job
        # WORKFLOW_DIR is the directory where the job will execute
        self.job_prefix = f"export WORKFLOW_DIR={self.workflow_dir}\n\n"
        x509_user_proxy = os.path.join(
            self.scratch_dir,
            os.path.basename(os.environ["X509_USER_PROXY"]),
        )
        self.job_prefix += f"export X509_USER_PROXY={x509_user_proxy}\n\n"
        self.job_prefix += "cd $WORKFLOW_DIR/scratch\n\n"

        os.environ["WORKFLOW_DIR"] = self.workflow_dir
        self.context = get_context(context_name, self.xedocs_version)

    def copy_files(self):
        """Copy the necessary files to the workflow directory."""

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)
        os.makedirs(self.scratch_dir, 0o755, exist_ok=True)
        # To prevent different jobs from overwriting each other's files
        os.makedirs(os.path.join(self.scratch_dir, "strax_data"), 0o555, exist_ok=True)
        os.makedirs(os.path.join(self.outputs_dir, "strax_data_rcc"), 0o755, exist_ok=True)

        # Copy the workflow files
        shutil.copy2(
            os.path.join(os.path.dirname(utilix.__file__), "install.sh"), self.generated_dir
        )
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
            shutil.copy2(
                f"{base_dir}/workflow/test_resource_usage.py",
                os.path.join(self.scratch_dir, "combine.py"),
            )
        else:
            shutil.copy2(
                f"{base_dir}/workflow/process.py",
                os.path.join(self.scratch_dir, "process.py"),
            )
            shutil.copy2(
                f"{base_dir}/workflow/combine.py",
                os.path.join(self.scratch_dir, "combine.py"),
            )

        # Copy resources because some nodes have no internet access
        # src_folder = os.path.join(os.environ["HOME"], "resource_cache")
        # dst_folder = os.path.join(self.scratch_dir, "resource_cache")
        # if not os.path.exists(dst_folder):
        #     shutil.copytree(src_folder, dst_folder)

    def __submit(self, job, jobname, log, **kwargs):
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

    def add_install_job(self):
        """Install user specified packages."""
        log = os.path.join(self.outputs_dir, "install.log")
        job = "set -e\n\n"
        job += self.job_prefix
        job += "cd $WORKFLOW_DIR/generated\n\n"
        job += "export HOME=$WORKFLOW_DIR/scratch\n\n"
        job += ". install.sh strax straxen cutax utilix admix outsource\n"
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = "install"
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        batchq_kwargs["cpus_per_task"] = 1
        batchq_kwargs["mem_per_cpu"] = 2_000 + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = 1
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.install_job_id = self.n_job
        # self.install_job_id = self.__submit(job, **batchq_kwargs)
        self.n_job += 1

    def is_stored(self, run_id, data_types, chunks=None, check_root_data_type=True):
        """Check if the data is already stored."""
        done = []
        for data_type in data_types:
            if self.context.is_stored(
                run_id,
                data_type,
                chunk_number=get_chunk_number(
                    self.context,
                    run_id,
                    data_type,
                    chunks=chunks,
                    check_root_data_type=check_root_data_type,
                ),
            ):
                done.append(data_type)
        return done

    def add_lower_processing_job(self, label, level, dbcfg):
        """Add a per-chunk processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            raise RuntimeError(
                f"No data found as the dependency of {level['data_types'].not_processed}."
            )

        # Get the key for the job
        # _key = self.get_key(dbcfg, level)
        jobname = f"{label}_{dbcfg._run_id}"
        suffix = "_".join(label.split("_")[1:])

        # Add the upper input directory to the storage
        # If the per-chunk processing is done, the data will be moved to the upper input directory
        # Remember to remove it before returning
        self.context.storage.append(
            strax.DataDirectory(
                os.path.join(self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "input"),
                readonly=True,
            ),
        )

        done = self.is_stored(dbcfg._run_id, level["data_types"], check_root_data_type=False)
        if len(done) == len(level["data_types"]):
            self.logger.debug(f"Skipping job for processing: {list(level['data_types'].keys())}")
            self.context.storage.pop()
            return

        # Loop over the chunks
        os.makedirs(
            os.path.join(self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "input"),
            0o755,
            exist_ok=True,
        )
        job_ids = []
        for job_i in range(len(level["chunks"])):
            done = self.is_stored(
                dbcfg._run_id,
                level["data_types"],
                chunks=level["chunks"][job_i],
            )
            if len(done) == len(level["data_types"]):
                self.logger.debug(
                    f"Skipping job for per-chunk processing: {level['chunks'][job_i]}"
                )
                continue
            self.logger.debug(f"Adding job for per-chunk processing: {level['chunks'][job_i]}")
            # log = os.path.join(self.outputs_dir, f"{_key}-output-{job_i:04d}.log")
            log = os.path.join(self.outputs_dir, f"{jobname}-{job_i:04d}.log")

            # Add job
            input = os.path.join(self.scratch_dir, f"{jobname}-{job_i:04d}", "input")
            output = os.path.join(self.scratch_dir, f"{jobname}-{job_i:04d}", "output")
            staging_dir = os.path.join(self.scratch_dir, f"{jobname}-{job_i:04d}", "strax_data")
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
                "true",
                f"{self.remove_heavy}".lower(),
                input,
                output,
                staging_dir,
                "X",
            ]
            args += list(level["data_types"])
            args = [str(arg) for arg in args]
            job = "set -e\n\n"
            job += f"export PEGASUS_DAG_JOB_ID={label}_ID{self.n_job:07}"
            job += "\n\n"
            job += self.job_prefix + " ".join(args)
            job += "\n\n"
            # All related strax data will either be in input,
            # output or strax_data while the job is running
            # The strax data will be moved to the output directory after the job is done
            job += f"mv {output}/* "
            job += os.path.join(self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "input")
            job += "\n\n"
            batchq_kwargs = {}
            batchq_kwargs["jobname"] = f"{jobname}-{job_i:04d}"
            batchq_kwargs["log"] = log
            batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
            # The job to install the user packages is a dependency
            if not self.debug and self.install_job_id is not None:
                batchq_kwargs["dependency"] = self.install_job_id
            batchq_kwargs["cpus_per_task"] = level["cores"]
            batchq_kwargs["mem_per_cpu"] = level["memory"][job_i] + CONTAINER_MEMORY_OVERHEAD
            batchq_kwargs["hours"] = eval(
                uconfig.get("Outsource", "pegasus_max_hours_lower", fallback=None)
            )
            self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
            # job_id = self.__submit(job, **batchq_kwargs)
            job_ids.append(self.n_job)
            self.n_job += 1

        if not job_ids:
            self.logger.warning(
                f"No jobs for per-chunk processing are submitted for {level['chunks']}."
            )
            self.context.storage.pop()
            return
        # Add combine job
        jobname = f"combine_{suffix}_{dbcfg._run_id}"
        # log = os.path.join(self.outputs_dir, f"{_key}-output.log")
        log = os.path.join(self.outputs_dir, f"{jobname}-output.log")
        # Though this is a combine job, its results will be put in upper level folder
        input = os.path.join(self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "input")
        output = os.path.join(self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "output")
        staging_dir = os.path.join(
            self.scratch_dir, f"upper_{suffix}_{dbcfg._run_id}", "strax_data"
        )
        args = [f"{self.scratch_dir}/combine-wrapper.sh"]
        args += [
            dbcfg.run_id,
            self.context_name,
            self.xedocs_version,
            f"{self.rucio_upload}".lower(),
            f"{self.rundb_update}".lower(),
            f"{self.stage}".lower(),
            input,
            output,
            staging_dir,
            "X",
            " ".join(map(str, [cs[-1] for cs in level["chunks"]])),
        ]
        args = [str(arg) for arg in args]
        job = "set -e\n\n"
        job += f"export PEGASUS_DAG_JOB_ID=combine_{suffix}_ID{self.n_job:07}"
        job += "\n\n"
        job += self.job_prefix + " ".join(args)
        job += "\n\n"
        job += f"mv {output}/* {self.outputs_dir}/strax_data_rcc/\n"
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        if not self.debug:
            batchq_kwargs["dependency"] = []
            for job_id in [self.install_job_id] + job_ids:
                if job_id is None:
                    continue
                batchq_kwargs["dependency"].append(job_id)
            if len(batchq_kwargs["dependency"]) == 0:
                batchq_kwargs.pop("dependency")
            elif len(batchq_kwargs["dependency"]) == 1:
                batchq_kwargs["dependency"] = batchq_kwargs["dependency"][0]
        batchq_kwargs["cpus_per_task"] = level["combine_cores"]
        batchq_kwargs["mem_per_cpu"] = level["combine_memory"] + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = eval(
            uconfig.get("Outsource", "pegasus_max_hours_combine", fallback=None)
        )
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.last_combine_job_id = self.n_job
        # self.last_combine_job_id = self.__submit(job, **batchq_kwargs)
        self.n_job += 1
        self.context.storage.pop()

    def add_upper_processing_job(self, label, level, dbcfg):
        """Add a processing job to the workflow."""
        rses = set.intersection(*[set(v["rses"]) for v in level["data_types"].values()])
        if len(rses) == 0:
            self.logger.warning(
                f"No data found as the dependency of {tuple(level['data_types'])}. "
                f"Hopefully those will be created by the workflow."
            )

        done = self.is_stored(dbcfg._run_id, level["data_types"])
        if len(done) == len(level["data_types"]):
            self.logger.debug(f"Skipping job for processing: {list(level['data_types'].keys())}")
            return

        # Get the key for the job
        # _key = self.get_key(dbcfg, level)
        jobname = f"{label}_{dbcfg._run_id}"
        log = os.path.join(self.outputs_dir, f"{jobname}-output.log")
        input = os.path.join(self.scratch_dir, jobname, "input")
        output = os.path.join(self.scratch_dir, jobname, "output")
        staging_dir = os.path.join(self.scratch_dir, jobname, "strax_data")
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
            f"{self.last_combine_job_id is None}".lower(),
            f"{self.remove_heavy}".lower(),
            input,
            output,
            staging_dir,
            "X",
        ]
        args += list(level["data_types"])
        args = [str(arg) for arg in args]
        job = "set -e\n\n"
        job += f"export PEGASUS_DAG_JOB_ID={label}_ID{self.n_job:07}"
        job += "\n\n"
        job += self.job_prefix + " ".join(args)
        job += "\n\n"
        job += f"mv {output}/* {self.outputs_dir}/strax_data_rcc/\n"
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        if not self.debug:
            batchq_kwargs["dependency"] = []
            for job_id in [self.install_job_id, self.last_combine_job_id]:
                if job_id is None:
                    continue
                batchq_kwargs["dependency"].append(job_id)
            if len(batchq_kwargs["dependency"]) == 0:
                batchq_kwargs.pop("dependency")
            elif len(batchq_kwargs["dependency"]) == 1:
                batchq_kwargs["dependency"] = batchq_kwargs["dependency"][0]
        batchq_kwargs["cpus_per_task"] = level["cores"]
        batchq_kwargs["mem_per_cpu"] = level["memory"] + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = eval(
            uconfig.get("Outsource", "pegasus_max_hours_upper", fallback=None)
        )
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.n_job += 1

    def _submit_run(self, group, label, level, dbcfg):
        """Submit a single run to the workflow."""
        if group == 0:
            self.add_lower_processing_job(label, level, dbcfg)
        else:
            self.add_upper_processing_job(label, level, dbcfg)

    def _submit(self):
        """Actually submit the workflow to the batch queue."""
        # Do nothing if installation job is the only job
        if len(self.jobs) == 1 and self.install_job_id is not None:
            return
        n_jobs = sorted(list(self.jobs.keys()))
        for n_job in n_jobs:
            job = self.jobs[n_job]
            if "dependency" not in job["batchq_kwargs"]:
                dependency = None
            elif "dependency" in job["batchq_kwargs"] is None:
                dependency = None
            elif isinstance(job["batchq_kwargs"]["dependency"], int):
                dependency = self.jobs[job["batchq_kwargs"]["dependency"]]["job_id"]
            elif isinstance(job["batchq_kwargs"]["dependency"], list):
                dependency = [
                    self.jobs[job_id]["job_id"] for job_id in job["batchq_kwargs"]["dependency"]
                ]
            self.jobs[n_job]["job_id"] = self.__submit(
                job["command"],
                **{
                    **job["batchq_kwargs"],
                    "dependency": dependency,
                },
            )

    def submit(self):
        """Submit the workflow to the batch queue."""

        self.n_job = 0
        self.jobs = {}

        # Copy the necessary files to the workflow directory
        self.copy_files()

        # Install user specified packages
        if self.user_installed_packages:
            self.add_install_job()
        else:
            self.install_job_id = None

        # Prepare for the job submission instruction
        runlist, summary = self._submit_runs()

        # Actually submit the jobs
        if not self.debug:
            self._submit()

        self.save_runlist(runlist)
        self.update_summary(summary)
        self.save_summary(summary)

        with open(os.path.join(self.generated_dir, "workflow.json"), mode="w") as f:
            f.write(json.dumps(self.jobs, indent=4))

        if self.debug:
            if len(self.jobs) == 1 and self.install_job_id is not None:
                return
            commands = [v["command"] for v in self.jobs.values()]
            with open(os.path.join(self.generated_dir, "commands.sh"), "w") as f:
                f.write("\n\n".join(commands) + "\n")
