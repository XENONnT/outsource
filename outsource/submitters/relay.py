import os
from copy import deepcopy
import yaml
import numpy as np
from utilix import uconfig
from outsource.meta import DETECTOR_DATA_TYPES
from outsource.config import RunConfig
from outsource.submitters.slurm import SubmitterSlurm


class SubmitterRelay(SubmitterSlurm):
    """Submitter for OSG-RCC.

    The instance will first read the workflow file and extract
    1. The list of run IDs
    2. The per-chunk resources needed
    The information about the data_types and the resources needed
    for each job will still be deduced from XENON_CONFIG.
    It will then submit the jobs the same as SubmitterSlurm.

    """

    def __init__(self, *arg, **kwargs):
        super().__init__(*arg, **kwargs)
        if not self.relay:
            raise ValueError(
                "OSG-RCC mode must be used with --relay flag. Please specify --osg_rcc --relay"
            )
        self.upper_only = uconfig.getboolean("Outsource", "rcc_upper_only", fallback=False)

        if self._runlist:
            raise ValueError("Runlist cannot be specified with --osg_rcc")
        if not os.path.exists(self._workflow):
            raise FileNotFoundError(f"Workflow file not found: {self._workflow}")
        self._runlist, self.chunks_lists = self.parse_workflow(self._workflow)
        self.logger.info(f"Workflow file {self._workflow} parsed successfully.")
        self.logger.info(f"Found {len(self._runlist)} runs in the workflow.")
        if os.path.exists(self.finished):
            self._done = np.loadtxt(self.finished, dtype=int, ndmin=1).tolist()
        self.logger.info(f"{len(self._done)} runs has been processed.")
        self._runlist = sorted(set(self._runlist) - set(self._done))
        self.logger.info(f"Found {len(self._runlist)} runs to process.")

    @property
    def finished(self):
        return os.path.join(self.generated_dir, "finished.txt")

    @staticmethod
    def parse_workflow(workflow):
        """Parse the workflow."""
        empty_resources = dict()
        for detector in DETECTOR_DATA_TYPES:
            empty_resources[detector] = []
        with open(workflow, "r") as file:
            data = yaml.safe_load(file)
        run_ids = []
        chunks_lists = {}
        for job in data["jobs"]:
            if job["type"] != "job":
                continue
            run_id = job["arguments"][0]
            run_ids.append(run_id)
            if "lower" not in job["name"]:
                continue
            detector = "_".join(job["name"].split("_")[1:])
            chunks_lists.setdefault(
                run_id,
                deepcopy(empty_resources),
            )
            # Extract how the raw_records are distributed in jobs
            chunks_lists[run_id][detector].append(job["arguments"][3:5])
        run_ids = sorted(set(run_ids))
        for run_id in chunks_lists:
            for detector in chunks_lists[run_id]:
                chunks_lists[run_id][detector] = sorted(
                    chunks_lists[run_id][detector], key=lambda x: x[0]
                )
        return run_ids, chunks_lists

    def get_dbcfg(self, run_id):
        """Get the run configuration."""
        dbcfg = RunConfig(self.context, run_id, ignore_processed=self.ignore_processed)
        # Get the resources needed for each job
        dbcfg.resources_assignment(chunks_lists=self.chunks_lists[run_id])
        return dbcfg
