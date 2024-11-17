import time
from itertools import chain
from typing import Dict, Any
import numpy as np
from utilix import DB, uconfig, xent_collection
import admix

from outsource.meta import PER_CHUNK_DATA_TYPES, DETECTOR_DATA_TYPES, LED_MODES
from outsource.utils import (
    get_possible_dependencies,
    get_to_save_data_types,
    per_chunk_storage_root_data_type,
)


LOWER_DISK = uconfig.getint("Outsource", "lower_disk", fallback=None)
LOWER_MEMORY = uconfig.getint("Outsource", "lower_memory", fallback=None)
COMBINE_DISK = uconfig.getint("Outsource", "combine_disk", fallback=None)
COMBINE_MEMORY = uconfig.getint("Outsource", "combine_memory", fallback=None)
UPPER_DISK = uconfig.getint("Outsource", "upper_disk", fallback=None)
UPPER_MEMORY = uconfig.getint("Outsource", "upper_memory", fallback=None)
LOWER_CPUS = uconfig.getint("Outsource", "lower_cpus", fallback=1)
COMBINE_CPUS = uconfig.getint("Outsource", "combine_cpus", fallback=1)
UPPER_CPUS = uconfig.getint("Outsource", "upper_cpus", fallback=1)

db = DB()
coll = xent_collection()


class DataTypes(dict):
    """A class to store the data_type and its status.

    It is a dict with keys missing, processed, and rses.

    """

    @property
    def not_processed(self):
        return [key for key in self if self[key]["missing"]]

    @property
    def only_data_type(self):
        return list(self.keys())[0]


class RunConfig:
    """The configuration of how a run will be processed.

    The class will focus on the RSE and instruction to the submitter.

    """

    # Data availability to site selection map.
    # desired_sites mean condor will try to run the job on those sites
    rse_site_map = {
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

    chunks_per_job = uconfig.getint("Outsource", "chunks_per_job", fallback=None)

    def __init__(self, context, run_id, ignore_processed=False, standalone_download=False):
        self.context = context
        self.run_id = run_id
        self.ignore_processed = ignore_processed
        self.standalone_download = standalone_download

        # Default job priority - workflows will be given priority
        # in the order they were submitted.
        self.priority = 2250000000 - int(time.time())
        if self.priority <= 0:
            raise ValueError("Priority must be positive")

        self.run_data = db.get_data(self.run_id)
        self.set_requirements_base()

        # Get the detectors and start time of this run
        cursor = coll.find_one({"number": self.run_id}, {"detectors": 1, "mode": 1})
        self.detectors = cursor["detectors"]
        self.mode = cursor["mode"]
        if not isinstance(self.detectors, list):
            raise ValueError(f"Detectors needs to be a list, not a {type(self.detectors)}")

        # Get the data_type that need to be processed
        # and determine which rse the input data is on
        self.data_types = self.deduce_data_types()

        # Get the resources needed for each job
        self.resources_assignment()

    @property
    def _run_id(self):
        return f"{self.run_id:06d}"

    def key_for(self, data_type):
        return self.context.key_for(self._run_id, data_type)

    def _led_mode(self, data_types):
        """Modify the data_types based on the mode.

        LED calibration is special because its detector is still tpc but the mode is not the normal
        tpc runs mode.

        """
        if self.mode in LED_MODES:
            # If we are using LED data, only process those data_types
            # For this context, see if we have that data yet
            data_types = set(data_types) & set(LED_MODES[self.mode])
        else:
            # If we are not, don't process those data_types
            data_types = set(data_types) - set().union(*LED_MODES.values())
        return data_types

    def depends_on(self, data_type):
        """Get the data_type this one depends on.

        The result will be either data_type in same level of PER_CHUNK_DATA_TYPES or the
        root_data_types.

        """
        possible_dependencies = get_possible_dependencies(self.context)
        # Get the highest data_type in the possible dependencies
        data_types = sorted(
            self.context.get_dependencies(data_type) & (possible_dependencies),
            key=lambda x: self.context.tree_levels[x]["level"],
            reverse=True,
        )
        return data_types[0]

    def deduce_data_types(self):
        """Returns the data_types we need to process and why they need to be processed."""
        # Do we need to process? read from XENON_CONFIG
        include_data_types = uconfig.getlist("Outsource", "include_data_types")
        all_possible_data_types = set(
            chain.from_iterable(v["possible"] for v in DETECTOR_DATA_TYPES.values())
        )
        excluded_data_types = set(include_data_types) - all_possible_data_types
        if excluded_data_types:
            raise ValueError(
                f"Find data_types not supported in include_data_types: {excluded_data_types}. "
                f"It should include only subset of {all_possible_data_types}."
            )

        # Modify the data_types based on the mode
        include_data_types = self._led_mode(include_data_types)

        ret = {"submitted": []}
        # Here we must try to divide the include_data_types
        # into before and after the PER_CHUNK_DATA_TYPES
        for detector in self.detectors:
            ret[detector] = dict()
            # There are two group labels for the data_types
            data_types_group_labels = [f"lower_{detector}", f"upper_{detector}"]
            possible_data_types = DETECTOR_DATA_TYPES[detector]["possible"]
            # Possible data_types for this detector
            data_types = set(include_data_types) & set(possible_data_types)
            if not data_types:
                # If nothing to process
                for label in data_types_group_labels:
                    ret[detector][label] = {"data_types": DataTypes()}
                continue
            per_chunk_data_types = set(PER_CHUNK_DATA_TYPES) & set(possible_data_types)
            # Modify the data_types based on the mode again
            per_chunk_data_types = self._led_mode(per_chunk_data_types)

            if DETECTOR_DATA_TYPES[detector]["per_chunk"]:
                # Sanity check of per-chunk data_type
                if len(per_chunk_data_types) != 1:
                    raise ValueError("Why is there no per-chunk data_type deduced?")
                root_data_type = per_chunk_storage_root_data_type(
                    self.context, self._run_id, list(per_chunk_data_types)[0]
                )
                if root_data_type is None:
                    raise ValueError("Why is there no root_data_type deduced?")

            # In reprocessing, group the data_types into lower and higher
            # Lower is per-chunk storage, higher is not, they must be separated
            data_types_groups = [
                per_chunk_data_types,
                (per_chunk_data_types | data_types) - per_chunk_data_types,
            ]

            for group, (label, data_types_group) in enumerate(
                zip(data_types_group_labels, data_types_groups)
            ):
                ret[detector][label] = {"data_types": DataTypes()}
                for data_type in data_types_group:
                    ret[detector][label]["data_types"][data_type] = {
                        "missing": [],
                        "processed": [],
                        "rses": [],
                    }
                    # Expand the data_type to data_types that need to be saved
                    _data_types = get_to_save_data_types(self.context, data_type, rm_lower=group)
                    # Get Rucio Storage Elements of data_type that needed to be processed.
                    depends_on = self.depends_on(data_type)
                    hash = self.key_for(depends_on).lineage_hash
                    ret[detector][label]["data_types"][data_type]["depends_on"] = depends_on
                    ret[detector][label]["data_types"][data_type]["rses"] = list(
                        set(db.get_rses(self.run_id, depends_on, hash))
                    )
                    if self.ignore_processed:
                        ret[detector][label]["data_types"][data_type]["missing"] += list(
                            _data_types
                        )
                        continue
                    for _data_type in _data_types:
                        hash = self.key_for(_data_type).lineage_hash
                        rses = db.get_rses(self.run_id, _data_type, hash)
                        # If this data is not on any rse, reprocess it, or we are asking for a rerun
                        if rses:
                            ret[detector][label]["data_types"][data_type]["missing"].append(
                                _data_type
                            )
                        else:
                            ret[detector][label]["data_types"][data_type]["processed"].append(
                                _data_type
                            )

                ret[detector][label]["data_types"] = DataTypes(
                    sorted(
                        ret[detector][label]["data_types"].items(),
                        key=lambda item: self.context.tree_levels[item[0]]["order"],
                    )
                )
            # Summarize the submitted data_types for a detector
            ret["submitted"] += list(
                set().union(*[v["data_types"].not_processed for v in ret[detector].values()])
            )
            if len(ret["submitted"]) != len(set(ret["submitted"])):
                raise ValueError("Why are there duplicated data_types in different detectors?")
        return ret

    def resources_assignment(self):
        """Adaptively assign resources based on the size of data."""
        data_kind_collection, data_type_collection = self.context.get_data_kinds()
        for detector in self.detectors:
            _detector: Dict[str, Any] = DETECTOR_DATA_TYPES[detector]
            depends_on = set(
                chain.from_iterable(
                    [
                        v["depends_on"]
                        for v in self.data_types[detector][f"{_level}_{detector}"][
                            "data_types"
                        ].values()
                    ]
                    for _level in ["lower", "upper"]
                )
            )
            if not depends_on:
                continue

            # Meta data of the dependency
            depends_on &= self.context.root_data_types
            if len(depends_on) != 1:
                raise ValueError("Why is there no or more than one dependency deduced?")
            depends_on = list(depends_on)[0]
            data = self.dependency_exists(depends_on)

            # Number of items of the dependency
            files = self.list_files(depends_on, verbose=True)
            compressed_sizes = (
                np.array([f["bytes"] for f in files if "metadata" not in f["name"]]) / 1e6
            )
            compression_ratio = sum(compressed_sizes) / data["meta"]["size_mb"]
            actual_sizes = compressed_sizes / compression_ratio
            n_depends_on = actual_sizes * 1e6 / self.context.data_itemsize(depends_on)

            # Which data_types will be saved and their itemsizes
            missing_data_types = dict()
            itemsizes = dict()
            for _level in ["lower", "upper"]:
                missing_data_types[_level] = set(
                    chain.from_iterable(
                        v["missing"]
                        for v in self.data_types[detector][f"{_level}_{detector}"][
                            "data_types"
                        ].values()
                    )
                )
                itemsizes[_level] = np.array(
                    [
                        self.context.data_itemsize(data_type)
                        for data_type in missing_data_types[_level]
                    ]
                )

            # Calculate the disk usage ratio in MB
            # For one item in the dependency, how much disk space is needed in MB
            ratios = dict()
            for _level in ["lower", "upper"]:
                ratios[_level] = []
                for data_type in missing_data_types[_level]:
                    data_kind = data_type_collection[data_type]
                    ratios[_level].append(
                        _detector["rate"].get(data_kind, 0)
                        * _detector["compression"].get(data_kind, 0)
                    )
                ratios[_level] = np.array(ratios[_level])
            # coefficients = [1, 1, 1, 2]  # if we remove tarred folder while tarring
            coefficients = [2, 0.5, 2, 1.5]  # if we do not remove tarred folder while tarring
            # The lower level disk usage
            disk_ratio = dict()
            disk_ratio["lower"] = coefficients[0] * (itemsizes["lower"] * ratios["lower"]).sum()
            # The upper level also needs to consider the disk usage of the lower level
            disk_ratio["upper"] = coefficients[1] * disk_ratio["lower"]
            disk_ratio["upper"] += coefficients[2] * (itemsizes["upper"] * ratios["upper"]).sum()
            # The combine level disk usage is replica of the lower level w/o raw_records*
            disk_ratio["combine"] = coefficients[3] * (itemsizes["lower"] * ratios["lower"]).sum()
            # The raw_records* disk usage is calculated in the last
            if self.data_types[detector][f"lower_{detector}"]["data_types"]:
                disk_ratio["lower"] += self.context.data_itemsize(depends_on) * compression_ratio
            else:
                disk_ratio["upper"] += self.context.data_itemsize(depends_on) * compression_ratio
            for k in disk_ratio:
                disk_ratio[k] /= 1e6

            # Assign chunks to be calculated in lower jobs
            n_chunks = len(compressed_sizes)
            if self.chunks_per_job is None:
                # If chunks_per_job is not set, assign chunks based on disk usage
                rough_disk = uconfig.getint("Outsource", "rough_disk")
                chunks_list = []
                _chunk_i = 0
                for chunk_i in range(1, n_chunks + 1):
                    # We are aggressive here to use more than
                    # rough_disk assigned because storage is cheap
                    if (
                        n_depends_on[_chunk_i:chunk_i].sum() * disk_ratio["lower"] > rough_disk
                        or chunk_i == n_chunks
                    ):
                        chunks_list.append([_chunk_i, chunk_i])
                        _chunk_i = chunk_i
            else:
                # If chunks_per_job is set, assign chunks based on chunks_per_job
                njobs = int(np.ceil(n_chunks / self.chunks_per_job))
                chunks_list = []
                for job_i in range(njobs):
                    chunks = list(range(n_chunks))[
                        self.chunks_per_job * job_i : self.chunks_per_job * (job_i + 1)
                    ]
                    chunks_list.append([chunks[0], chunks[-1] + 1])

            # Calculate the disk and memory requirement in MB
            self.data_types[detector][f"lower_{detector}"]["chunks"] = chunks_list

            self.data_types[detector][f"lower_{detector}"]["cores"] = LOWER_CPUS
            self.data_types[detector][f"lower_{detector}"]["combine_cores"] = COMBINE_CPUS
            self.data_types[detector][f"upper_{detector}"]["cores"] = UPPER_CPUS

            if LOWER_DISK is not None:
                self.data_types[detector][f"lower_{detector}"]["disk"] = np.full(
                    len(chunks_list), LOWER_DISK, dtype=float
                )
            else:
                self.data_types[detector][f"lower_{detector}"]["disk"] = np.array(
                    [n_depends_on[c[0] : c[-1]].sum() * disk_ratio["lower"] for c in chunks_list]
                )
            if LOWER_MEMORY is not None:
                self.data_types[detector][f"lower_{detector}"]["memory"] = np.full(
                    len(chunks_list), LOWER_MEMORY, dtype=float
                )
            else:
                self.data_types[detector][f"lower_{detector}"]["memory"] = np.polyval(
                    _detector["memory"]["lower"],
                    [actual_sizes[c[0] : c[-1]].max() for c in chunks_list],
                )
            if UPPER_DISK is not None:
                self.data_types[detector][f"upper_{detector}"]["disk"] = UPPER_DISK
            else:
                self.data_types[detector][f"upper_{detector}"]["disk"] = (
                    n_depends_on.sum() * disk_ratio["upper"]
                )
            if UPPER_MEMORY is not None:
                self.data_types[detector][f"upper_{detector}"]["memory"] = UPPER_MEMORY
            else:
                self.data_types[detector][f"upper_{detector}"]["memory"] = np.polyval(
                    _detector["memory"]["upper"], actual_sizes.sum()
                )
            if COMBINE_DISK is not None:
                self.data_types[detector][f"lower_{detector}"]["combine_disk"] = COMBINE_DISK
            else:
                self.data_types[detector][f"lower_{detector}"]["combine_disk"] = (
                    n_depends_on.sum() * disk_ratio["combine"]
                )
            if COMBINE_MEMORY is not None:
                self.data_types[detector][f"lower_{detector}"]["combine_memory"] = COMBINE_MEMORY
            else:
                self.data_types[detector][f"lower_{detector}"]["combine_memory"] = np.polyval(
                    _detector["memory"]["combine"], actual_sizes.sum()
                )
            for md in ["disk", "memory"]:
                for label in self.data_types[detector]:
                    for prefix in ["", "combine_"]:
                        if prefix + md not in self.data_types[detector][label]:
                            continue
                        self.data_types[detector][label][prefix + md] *= _detector["redundancy"][md]
                        self.data_types[detector][label][prefix + md] = self.data_types[detector][
                            label
                        ][prefix + md].tolist()

    def dependency_exists(self, data_type="raw_records"):
        """Returns a boolean for whether the dependency exists in rucio and is accessible.

        It is a simplified version of DB.get_rses, and faster to just go through RunDB.

        """

        for data in self.run_data:
            if (
                data["type"] == data_type
                and data["host"] == "rucio-catalogue"
                and data["status"] == "transferred"
                and data["location"] in uconfig.getlist("Outsource", "raw_records_rse")
                and "TAPE" not in data["location"]
            ):
                return data
        return False

    def list_files(self, data_type, verbose=False):
        hash = self.key_for(data_type).lineage_hash
        did = f"xnt_{self._run_id}:{data_type}-{hash}"
        files = admix.rucio.list_files(did, verbose=verbose)
        return files

    def nchunks(self, data_type):
        # Get the data_type this one depends on
        files = self.list_files(self.depends_on(data_type))
        # Subtract 1 for metadata
        return len(files) - 1

    def set_requirements_base(self):
        requirements_base = "HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org"
        requirements_base += " && PORT_2880 && PORT_8000 && PORT_27017"
        requirements_base += ' && (Microarch >= "x86_64-v3")'
        requirements_base_us = requirements_base + ' && GLIDEIN_Country == "US"'
        if uconfig.getboolean("Outsource", "us_only", fallback=False):
            requirements_base = requirements_base_us

        # hs06_test_run limits the run_id to a set of compute nodes
        # at UChicago with a known HS06 factor
        if uconfig.getboolean("Outsource", "hs06_test_run", fallback=False):
            requirements_base += (
                ' && GLIDEIN_ResourceName == "MWT2" && regexp("uct2-c4[1-7]", Machine)'
            )
        # this_site_only limits the run_id to a set of compute nodes at UChicago for testing
        this_site_only = uconfig.get("Outsource", "this_site_only", fallback="")
        if this_site_only:
            requirements_base += f' && GLIDEIN_ResourceName == "{this_site_only}"'
        self.requirements_base = requirements_base
        self.requirements_base_us = requirements_base_us

    @property
    def _exclude_sites(self):
        """Exclude sites from the user _dbcfgs file."""

        if not uconfig.has_option("Outsource", "exclude_sites"):
            return ""

        sites = uconfig.getlist("Outsource", "exclude_sites")
        exprs = []
        for site in sites:
            exprs.append(f'GLIDEIN_Site =!= "{site}"')
        return " && ".join(exprs)

    def get_requirements(self, rses):
        # Determine the job requirements based on the data locations
        sites_expression, desired_sites = self._determine_target_sites(rses)
        requirements = self.requirements_base if len(rses) > 0 else self.requirements_base_us
        if sites_expression:
            requirements += f" && ({sites_expression})"
        # US nodes
        requirements_us = self.requirements_base_us
        # Add excluded nodes
        if self._exclude_sites:
            requirements += f" && ({self._exclude_sites})"
            requirements_us += f" && ({self._exclude_sites})"
        return requirements, requirements_us

    def _determine_target_sites(self, rses):
        """Given a list of RSEs, limit the runs for sites for those locations."""

        exprs = []
        sites = []
        for rse in rses:
            if rse in self.rse_site_map:
                if "expr" in self.rse_site_map[rse]:
                    exprs.append(self.rse_site_map[rse]["expr"])
                if "desired_sites" in self.rse_site_map[rse]:
                    sites.append(self.rse_site_map[rse]["desired_sites"])
        exprs = list(set(exprs))
        sites = list(set(sites))

        # make sure we do not request XENON1T sites we do not need
        if len(sites) == 0:
            sites.append("NONE")

        final_expr = " || ".join(exprs)
        desired_sites = ",".join(sites)
        return final_expr, desired_sites
