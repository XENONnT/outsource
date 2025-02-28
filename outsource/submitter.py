import os
import sys
from datetime import datetime, timezone
from utilix import uconfig
from utilix.config import setup_logger, set_logging_level

from outsource.utils import get_context


IMAGE_PREFIX = "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:"


class Submitter:

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
