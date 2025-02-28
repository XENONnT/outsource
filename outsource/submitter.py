import os
import sys
import json
from itertools import chain
from datetime import datetime, timezone
import numpy as np
from utilix import uconfig
from utilix.config import setup_logger, set_logging_level

from outsource.config import RunConfig
from outsource.utils import get_context
from outsource.utils import get_to_save_data_types


IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"


class Submitter:

    # Whether in OSG-RCC relay mode
    relay = False

    # This is a flag to indicate that the user has installed the packages
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
        self.logger = setup_logger(
            "outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING")
        )
        # Reduce the logging of request and urllib3
        set_logging_level(
            "urllib3", uconfig.get("Outsource", "db_logging_level", fallback="WARNING")
        )

        # Whether in OSG-RCC relay mode
        self.relay = relay

        # Load from XENON_CONFIG
        self.work_dir = uconfig.get("Outsource", "work_dir")
        # The current time will always be part of the workflow_id, except in relay mode
        if self.relay:
            # If in relay mode, use the workflow_id passed in
            if workflow_id is None:
                raise RuntimeError("Workflow ID must be passed in relay mode.")
            self.workflow_id = workflow_id
        else:
            self._setup_workflow_id(workflow_id)

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
        self.remove_heavy = remove_heavy
        self.rucio_upload = rucio_upload
        self.rundb_update = rundb_update
        if not self.rucio_upload and self.rundb_update:
            raise RuntimeError("Rucio upload must be enabled when updating the RunDB.")
        self.resources_test = resources_test
        self.debug = debug

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

    def _submit_runs(self):
        """Loop over the runs and submit the jobs to the workflow."""
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
                self.combine_tar = None
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

                    self._submit_run(group, label, level, dbcfg)

        return runlist, summary

    def save_runlist(self, runlist):
        """Save the runlist."""
        np.savetxt(self.runlist, sorted(runlist), fmt="%0d")

    def update_summary(self, summary):
        """Update the job summary."""
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

    def save_summary(self, summary):
        """Save the job summary."""
        with open(self.summary, mode="w") as f:
            f.write(json.dumps(summary, indent=4))
