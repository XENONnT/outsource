import os
import json
import time
import shutil
from copy import deepcopy
import numpy as np
import utilix
from utilix import uconfig, batchq
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
SALTAX = uconfig.getboolean("Outsource", "saltax", fallback=False)


CONTAINER_MEMORY_OVERHEAD = 6_000  # MB


class SubmitterSlurm(Submitter):

    # This flag will always be True for the Slurm submitter
    # Because the software environment will be set up in the scratch directory
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
        resubmit=False,
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
            resubmit=resubmit,
        )

        # commands to execute before the job
        # SLURM_WORKFLOW_DIR is the directory where the job will execute
        self.job_prefix = f"export SLURM_WORKFLOW_DIR={self.workflow_dir}\n\n"
        x509_user_proxy = os.path.join(
            self.scratch_dir,
            os.path.basename(os.environ["X509_USER_PROXY"]),
        )
        self.job_prefix += f"export X509_USER_PROXY={x509_user_proxy}\n\n"
        # To use the installed cutax
        self.job_prefix += "unset CUTAX_LOCATION\n\n"
        self.job_prefix += "unset PYTHONPATH\n\n"
        self.job_prefix += "cd $SLURM_WORKFLOW_DIR/scratch\n\n"

        os.environ["SLURM_WORKFLOW_DIR"] = self.workflow_dir
        self.context = get_context(context_name, self.xedocs_version)

        # The finished runs
        self._done = []

    @property
    def finished(self):
        return os.path.join(self.generated_dir, "finished.txt")

    @property
    def _submission(self):
        return os.path.join(self.generated_dir, "submission.json")

    def copy_files(self):
        """Copy the necessary files to the workflow directory."""

        os.makedirs(self.generated_dir, 0o755, exist_ok=True)
        os.makedirs(self.outputs_dir, 0o755, exist_ok=True)
        os.makedirs(self.scratch_dir, 0o755, exist_ok=True)
        # To prevent different jobs from overwriting each other's files
        os.makedirs(os.path.join(self.scratch_dir, "strax_data"), 0o755, exist_ok=True)
        os.makedirs(os.path.join(self.outputs_dir, "strax_data_rcc"), 0o755, exist_ok=True)
        os.makedirs(
            os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk"), 0o755, exist_ok=True
        )

        # Copy the workflow files
        shutil.copy2(
            os.path.join(os.path.dirname(utilix.__file__), "install.sh"), self.generated_dir
        )
        shutil.copy2(f"{base_dir}/workflow/process-wrapper.sh", self.scratch_dir)
        shutil.copy2(f"{base_dir}/workflow/combine-wrapper.sh", self.scratch_dir)
        shutil.copy2(os.path.join(os.environ["HOME"], ".dbtoken"), self.scratch_dir)
        shutil.copy2(
            os.environ["XENON_CONFIG"],
            os.path.join(self.scratch_dir, ".xenon_config"),
        )
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

        rcc_retries_sleep = uconfig.getint("Outsource", "rcc_retries_sleep", fallback=10)
        rcc_max_jobs = uconfig.getint("Outsource", "rcc_max_jobs", fallback=None)
        job_id = None
        partition = uconfig.get("Outsource", "rcc_partition")
        while job_id is None:
            if rcc_max_jobs is not None:
                n_jobs = batchq.count_jobs(partition)
                while n_jobs >= rcc_max_jobs:
                    self.logger.info(
                        f"Too many jobs ({n_jobs}) on {partition} partition. "
                        f"Waiting for {rcc_retries_sleep} seconds. "
                        "Job submittion will resume when the number of jobs "
                        f"is less than {rcc_max_jobs}."
                    )
                    time.sleep(rcc_retries_sleep)
                    n_jobs = batchq.count_jobs(partition)

            def submit():
                job_id = batchq.submit_job(
                    job,
                    jobname=jobname,
                    log=log,
                    **kwargs,
                )
                return job_id

            job_id = submit()
            while job_id is None:
                self.logger.info(
                    f"Job submission failed on {partition} partition. "
                    f"Waiting for {rcc_retries_sleep} seconds."
                )
                time.sleep(rcc_retries_sleep)
                job_id = submit()
        return job_id

    def add_install_job(self):
        """Install user specified packages."""
        log = os.path.join(self.outputs_dir, "install.log")
        job = "set -e\n\n"
        job += self.job_prefix
        job += "cd $SLURM_WORKFLOW_DIR/generated\n\n"
        job += "export HOME=$SLURM_WORKFLOW_DIR/scratch\n\n"
        job += f". install.sh {' '.join(self.user_install_package)}\n"
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = "install"
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
        batchq_kwargs["cpus_per_task"] = 1
        batchq_kwargs["mem_per_cpu"] = 2_000 + CONTAINER_MEMORY_OVERHEAD
        batchq_kwargs["hours"] = 1
        batchq_kwargs = {**BATCHQ_DEFAULT_ARGUMENTS, **batchq_kwargs}
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.install_job_id = self.n_job
        # self.install_job_id = self.__submit(job, **batchq_kwargs)
        self.n_job += 1

    def is_stored(self, run_id, data_types, chunks=None, check_root_data_type=True):
        """Check if the data is already stored."""
        if self.ignore_processed:
            return []

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

        done = self.is_stored(dbcfg._run_id, level["data_types"], check_root_data_type=False)
        if len(done) == len(level["data_types"]):
            self.logger.debug(f"Skipping job for processing: {list(level['data_types'].keys())}")
            self.lower_done = True
            return
        if self.upper_only:
            self.logger.debug(f"Skipping lower-level processing for {dbcfg._run_id}.")
            return

        # Loop over the chunks
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
            input = os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk")
            output = os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk")
            staging_dir = os.path.join(self.scratch_dir, "strax_data")
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
            batchq_kwargs = {}
            batchq_kwargs["jobname"] = f"{jobname}-{job_i:04d}"
            batchq_kwargs["log"] = log
            batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
            # The job to install the user packages is a dependency
            if self.install_job_id is not None:
                batchq_kwargs["dependency"] = self.install_job_id
            batchq_kwargs["cpus_per_task"] = level["cores"]
            batchq_kwargs["mem_per_cpu"] = level["memory"][job_i] + CONTAINER_MEMORY_OVERHEAD
            batchq_kwargs["hours"] = eval(
                uconfig.get("Outsource", "pegasus_max_hours_lower", fallback=None)
            )
            batchq_kwargs = {**BATCHQ_DEFAULT_ARGUMENTS, **batchq_kwargs}
            self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
            # job_id = self.__submit(job, **batchq_kwargs)
            job_ids.append(self.n_job)
            self.n_job += 1

        # job_ids can be empty in relay mode
        # Add combine job
        jobname = f"combine_{suffix}_{dbcfg._run_id}"
        # log = os.path.join(self.outputs_dir, f"{_key}-output.log")
        log = os.path.join(self.outputs_dir, f"{jobname}.log")
        if job_ids:
            input = os.path.join(self.outputs_dir, "strax_data_rcc_per_chunk")
        else:
            # This might be a combine-only job
            input = os.path.join(self.outputs_dir, "strax_data_osg_per_chunk")
        output = os.path.join(self.outputs_dir, "strax_data_rcc")
        staging_dir = os.path.join(self.scratch_dir, "strax_data")
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
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
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
        batchq_kwargs = {**BATCHQ_DEFAULT_ARGUMENTS, **batchq_kwargs}
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.last_combine_job_id = self.n_job
        # self.last_combine_job_id = self.__submit(job, **batchq_kwargs)
        self.n_job += 1

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
            if not self.lower_done:
                raise RuntimeError(
                    f"Data {done} of {dbcfg._run_id} is already stored for upper-level processing "
                    "but lower-level processing is not done. This is not allowed. "
                    "The lower-level results must be avaliable before the upper-level processing. "
                    "You can remove the lower-level results and resubmit the workflow."
                )
            self.upper_done = True
            self.logger.debug(f"Skipping job for processing: {list(level['data_types'].keys())}")
            return

        # Get the key for the job
        # _key = self.get_key(dbcfg, level)
        jobname = f"{label}_{dbcfg._run_id}"
        log = os.path.join(self.outputs_dir, f"{jobname}.log")
        input = os.path.join(self.outputs_dir, "strax_data_rcc")
        output = os.path.join(self.outputs_dir, "strax_data_rcc")
        staging_dir = os.path.join(self.scratch_dir, "strax_data")
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
        batchq_kwargs = {}
        batchq_kwargs["jobname"] = jobname
        batchq_kwargs["log"] = log
        batchq_kwargs["container"] = f"xenonnt-{self.image_tag}.simg"
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
        batchq_kwargs = {**BATCHQ_DEFAULT_ARGUMENTS, **batchq_kwargs}
        self.jobs[self.n_job] = {"command": job, "batchq_kwargs": deepcopy(batchq_kwargs)}
        self.n_job += 1

    def _submit_run(self, group, label, level, dbcfg):
        """Submit a single run to the workflow."""
        if group == 0:
            self.lower_done = False
            self.add_lower_processing_job(label, level, dbcfg)
        else:
            self.upper_done = False
            if self.upper_only and not self.lower_done:
                self.logger.debug(f"Skipping upper-level processing for {dbcfg._run_id}.")
                return
            self.add_upper_processing_job(label, level, dbcfg)
            if self.upper_done:
                self._done.append(dbcfg.run_id)

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

    def save_finished(self, runlist):
        """Save the runlist."""
        np.savetxt(self.finished, sorted(runlist), fmt="%d")

    def save_submission(self):
        """Save the submission to a file."""
        with open(self._submission, mode="w") as f:
            f.write(json.dumps(self.jobs, indent=4))

    def recover_submission(self):
        """Load the submission from a file."""
        with open(self._submission, "r") as file:
            self.jobs = json.load(file)
        # Convert the keys to integers
        self.jobs = {int(k): v for k, v in self.jobs.items()}
        if self.jobs[0]["batchq_kwargs"]["jobname"] == "install":
            self.install_job_id = 0
        else:
            self.install_job_id = None

    def submit(self):
        """Submit the workflow to the batch queue."""

        if not ((self.relay or self.resubmit) and self.debug):
            # All submitters need to make tarballs
            self.make_tarballs()
            # Copy the necessary files to the workflow directory
            self.copy_files()

        if not self.resubmit:
            self.n_job = 0
            self.jobs = {}

            # Install user specified packages
            if self.user_installed_packages:
                self.add_install_job()
            else:
                self.install_job_id = None

            # Prepare for the job submission instruction
            runlist, summary = self._submit_runs()

            # Save wotkflow.json first, because job submission may fail
            self.save_submission()
            self.save_finished(self._done)

            # Actually submit the jobs
            if not self.debug:
                self._submit()

            self.save_runlist(runlist)
            self.update_summary(summary)
            self.save_summary(summary)
            self.save_submission()
        else:
            self.recover_submission()
            if not self.debug:
                self._submit()
            self.save_submission()

        if self.debug:
            if len(self.jobs) == 1 and self.install_job_id is not None:
                return
            commands = [v["command"] for v in self.jobs.values()]
            with open(os.path.join(self.generated_dir, "commands.sh"), "w") as f:
                f.write("\n\n".join(commands) + "\n")
