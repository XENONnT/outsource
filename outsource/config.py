import time
from itertools import chain
from typing import Dict, Any
import numpy as np
from utilix.config import setup_logger
from utilix import DB, uconfig, xent_collection
import admix

from outsource.meta import get_clean_per_chunk_data_types, get_clean_detector_data_types, LED_MODES

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
US_ONLY = uconfig.getboolean("Outsource", "us_only", fallback=False)
EU_ONLY = uconfig.getboolean("Outsource", "eu_only", fallback=False)
SITE_ONLY = uconfig.getboolean("Outsource", "site_only", fallback=False)

# Data availability to site selection map.
# This puts constraints on the sites that can be used for
# processing based on the input RSE for raw_records.

# These are US sites
US_RSES = [
    # Chicago, IL
    "UC_OSG_USERDISK",  # DISK
    "UC_DALI_USERDISK",  # DISK
    "UC_MIDWAY_USERDISK",  # DISK
    # San Diego, CA
    "SDSC_NSDF_USERDISK",  # DISK
]

# These are European sites
EU_RSES = [
    # Amsterdam, NL
    "NIKHEF2_USERDISK",  # DISK
    "SURFSARA_USERDISK",  # TAPE
    "SURFSARA2_USERDISK",  # DISK
    # Paris, FR
    "CCIN2P3_USERDISK",  # TAPE
    "CCIN2P32_USERDISK",  # DISK
    # Bologna, IT
    "CNAF_USERDISK",  # DISK
    "CNAF_TAPE3_USERDISK",  # TAPE
]

US_ONLY_REQUIREMENTS = 'GLIDEIN_Country == "US"'

# Define an expression to give higher priority to EU sites
EU_RANK = (
    '(GLIDEIN_Country == "NL") * 999'
    ' + (GLIDEIN_Country == "FR") * 9'
    ' + (GLIDEIN_Country == "IT") * 9'
)

# In case we want to keep the pipelines separate
# let's add a requirement that the jobs run in the EU only
# we do not have a EU flag, so we use the countries
EU_ONLY_COUNTRIES = uconfig.getlist("Outsource", "eu_only_countries", fallback=["NL", "FR", "IT"])
if len(EU_ONLY_COUNTRIES) > 1:
    EU_ONLY_REQUIREMENTS = (
        "(" + " || ".join(f'GLIDEIN_Country == "{c}"' for c in EU_ONLY_COUNTRIES) + ")"
    )
else:
    EU_ONLY_REQUIREMENTS = f'GLIDEIN_Country == "{EU_ONLY_COUNTRIES[0]}"'

RSE_SITE_MAP = {
    # "NIKHEF2_USERDISK": "NIKHEF",
    "NIKHEF2_USERDISK": "SURFsara",
    "SURFSARA_USERDISK": "SURFsara",
    "SURFSARA2_USERDISK": "SURFsara",
    # "CCIN2P3_USERDISK": "CCIN2P3",
    "CCIN2P3_USERDISK": "SURFsara",
    # "CCIN2P32_USERDISK": "CCIN2P3",
    "CCIN2P32_USERDISK": "SURFsara",
}

MAX_MEMORY = uconfig.getint("Outsource", "max_memory", fallback=12_000)  # in MB
MIN_DISK = uconfig.getint("Outsource", "min_disk", fallback=1_000)  # in MB

logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
db = DB()
coll = xent_collection()


class DataTypes(dict):
    """A class to store the data_type and its status.

    It is a dict with keys missing, processed, and rses.

    """

    @property
    def not_processed(self):
        return [key for key in self if self[key]["missing"]]


class RunConfig:
    """The configuration of how a run will be processed.

    The class will focus on the RSE and instruction to the submitter.

    """

    chunks_per_job = uconfig.getint("Outsource", "chunks_per_job", fallback=None)

    def __init__(self, context, run_id, ignore_processed=False):
        self.rse_constraints: Dict[str, Any] = {}
        for rse in US_RSES:
            self.rse_constraints.setdefault(rse, {})
            if US_ONLY:
                self.rse_constraints[rse]["expr"] = US_ONLY_REQUIREMENTS
        for rse in EU_RSES:
            self.rse_constraints.setdefault(rse, {})
            if EU_ONLY and not SITE_ONLY:
                self.rse_constraints[rse]["expr"] = EU_ONLY_REQUIREMENTS
                self.rse_constraints[rse]["rank"] = EU_RANK
            if SITE_ONLY and rse in RSE_SITE_MAP:
                self.rse_constraints[rse]["expr"] = f'GLIDEIN_Site == "{RSE_SITE_MAP[rse]}"'
                self.rse_constraints[rse]["site"] = RSE_SITE_MAP[rse]

        self.context = context
        self.run_id = run_id
        self.ignore_processed = ignore_processed

        # Sometimes we do not do per-chunk processing
        self._per_chunk_data_types = get_clean_per_chunk_data_types(self.context)
        self._detector_data_types = get_clean_detector_data_types(self.context)

        # Default job priority - workflows will be given priority
        # in the order they were submitted.
        self.priority = 2250000000 - int(time.time())
        if self.priority <= 0:
            raise ValueError("Priority must be positive. Are you from the future?")

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

    @property
    def _run_id(self):
        return f"{self.run_id:06d}"

    def key_for(self, data_type):
        return self.context.key_for(self._run_id, data_type)

    @property
    def is_led_mode(self):
        return self.mode in LED_MODES

    def _led_mode(self, data_types):
        """Modify the data_types based on the mode.

        LED calibration is special because its detector is still tpc but the mode is not the normal
        tpc runs mode.

        """
        if self.is_led_mode:
            # If we are using LED data, only process those data_types
            # For this context, see if we have that data yet
            data_types = set(data_types) & set(LED_MODES[self.mode]["possible"])
        else:
            # If we are not, don't process those data_types
            data_types = set(data_types) - set().union(*[v["possible"] for v in LED_MODES.values()])
        return data_types

    def depends_on(self, data_type, lower=False):
        """Get the data_type this one depends on.

        The result will be either data_type in same level of _per_chunk_data_types or the
        root_data_types.

        """
        possible_dependencies = get_possible_dependencies(self.context, lower=lower)
        # Get the highest data_type in the possible dependencies
        return list(self.context.get_dependencies(data_type) & possible_dependencies)

    def deduce_data_types(self):
        """Returns the data_types we need to process and why they need to be processed."""
        # Do we need to process? read from XENON_CONFIG
        include_data_types = uconfig.getlist("Outsource", "include_data_types")
        all_possible_data_types = set(
            chain.from_iterable(v["possible"] for v in self._detector_data_types.values())
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
        # into before and after the _per_chunk_data_types
        for detector in self.detectors:
            ret[detector] = dict()
            # There are two group labels for the data_types
            data_types_group_labels = [f"lower_{detector}", f"upper_{detector}"]
            possible_data_types = self._detector_data_types[detector]["possible"]
            # Possible data_types for this detector
            data_types = set(include_data_types) & set(possible_data_types)
            if not data_types:
                # If nothing to process
                for label in data_types_group_labels:
                    ret[detector][label] = {"data_types": DataTypes()}
                continue

            per_chunk_data_types = set(self._per_chunk_data_types) & set(possible_data_types)
            # Modify the data_types based on the mode again
            per_chunk_data_types = self._led_mode(per_chunk_data_types)

            if self._detector_data_types[detector]["per_chunk"]:
                # Sanity check of per-chunk data_type
                for per_chunk_data_type in per_chunk_data_types:
                    root_data_type = per_chunk_storage_root_data_type(
                        self.context, self._run_id, per_chunk_data_type
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
                    depends_on = self.depends_on(data_type, lower=not group)
                    ret[detector][label]["data_types"][data_type]["depends_on"] = depends_on
                    _rses = []
                    for _depends_on in depends_on:
                        hash = self.key_for(_depends_on).lineage_hash
                        _rses.append(set(db.get_rses(self.run_id, _depends_on, hash)))
                    ret[detector][label]["data_types"][data_type]["rses"] = list(
                        set.intersection(*_rses)
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
            ret["submitted"] = sorted(
                ret["submitted"],
                key=lambda item: self.context.tree_levels[item]["order"],
            )
            if len(ret["submitted"]) != len(set(ret["submitted"])):
                raise ValueError("Why are there duplicated data_types in different detectors?")
        return ret

    def resources_assignment(self, chunks_lists=None):
        """Adaptively assign resources based on the size of data."""
        data_kind_collection, data_type_collection = self.context.get_data_kinds()
        for detector in self.detectors:
            _detector: Dict[str, Any] = self._detector_data_types[detector]
            depends_on = []
            for _level in ["lower", "upper"]:
                for v in self.data_types[detector][f"{_level}_{detector}"]["data_types"].values():
                    depends_on += v["depends_on"]
            depends_on = set(depends_on)
            if not depends_on:
                continue

            # Meta data of the dependency
            # raw_records_aqmon is special because per-chunk processing is not needed
            depends_on &= self.context.root_data_types - set(("raw_records_aqmon",))
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

            meta = coll.find_one({"number": self.run_id}, {"start": 1, "end": 1})
            total_seconds = (meta["end"] - meta["start"]).total_seconds()
            seconds = np.cumsum(n_depends_on) / n_depends_on.sum() * total_seconds
            keep_seconds = _detector.get("keep_seconds", 0)
            if keep_seconds == 0:
                repeats = [len(n_depends_on)]
            else:
                if keep_seconds > seconds[-1]:
                    repeats = [len(n_depends_on), 0]
                else:
                    index = np.searchsorted(seconds, keep_seconds) + 1
                    repeats = [index, len(n_depends_on) - index]
            assert sum(repeats) == len(n_depends_on)

            # Calculate the disk usage ratio in MB
            # For one item in the dependency, how much disk space is needed in MB
            ratios = dict()
            for _level in ["lower", "upper"]:
                ratios[_level] = []
                for data_type in missing_data_types[_level]:
                    data_kind = data_type_collection[data_type]
                    ratios[_level].append(
                        np.repeat(
                            _detector["rate"].get(data_kind, [0, 0] if keep_seconds else 0), repeats
                        )
                        * _detector["compression"].get(data_kind, 0)
                    )
                ratios[_level] = np.array(ratios[_level]).T.reshape(
                    (sum(repeats), len(missing_data_types[_level]))
                )
            # We do not remove tarred folder while tarring
            # The lower level disk usage
            lower = (ratios["lower"] * itemsizes["lower"]).sum(axis=1)
            upper = (ratios["upper"] * itemsizes["upper"]).sum(axis=1)
            # The raw_records* disk usage is calculated in the last
            if self.data_types[detector][f"lower_{detector}"]["data_types"]:
                lower_raw = self.context.data_itemsize(depends_on) * compression_ratio
                upper_raw = 0
            else:
                upper_raw = self.context.data_itemsize(depends_on) * compression_ratio
                lower_raw = 0

            def get_disk(chunk_i, chunk_j):
                sl = slice(chunk_i, chunk_j)
                max_n = n_depends_on[sl].max()
                disk_ratio = dict()
                disk_ratio["lower"] = np.max(
                    [
                        # (max) raw_records* + uncompressed lower
                        lower_raw * max_n + (lower[sl] * n_depends_on[sl]).sum(),
                        # uncompressed lower + compressed lower
                        2 * (lower[sl] * n_depends_on[sl]).sum(),
                    ],
                    axis=0,
                )
                # The upper level also needs to consider the disk usage of the lower level
                disk_ratio["upper"] = np.max(
                    [
                        # compressed lower + decompressed lower
                        upper_raw * max_n + 2 * (lower[sl] * n_depends_on[sl]).sum(),
                        # decompressed lower + uncompressed upper
                        upper_raw * max_n + ((lower[sl] + upper[sl]) * n_depends_on[sl]).sum(),
                        # uncompressed upper + compressed upper
                        2 * (upper[sl] * n_depends_on[sl]).sum(),
                    ],
                    axis=0,
                )
                # The combine level disk usage is twice of the lower level w/o raw_records*
                # compressed lower + decompressed lower
                disk_ratio["combine"] = 2 * (lower[sl] * n_depends_on[sl]).sum()
                for k in disk_ratio:
                    disk_ratio[k] /= 1e6
                return disk_ratio

            # Assign chunks to be calculated in lower jobs
            n_chunks = len(compressed_sizes)
            if chunks_lists is not None:
                chunks_list = chunks_lists[detector]
            else:
                if self.chunks_per_job is None:
                    # If chunks_per_job is not set, assign chunks based on disk usage
                    rough_disk = uconfig.getint("Outsource", "rough_disk")
                    chunks_list = []
                    _chunk_i = 0
                    for chunk_i in range(1, n_chunks + 1):
                        # We are aggressive here to use more than
                        # rough_disk assigned because storage is cheap
                        disk = get_disk(_chunk_i, chunk_i)["lower"].sum()
                        if disk > rough_disk or chunk_i == n_chunks:
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
                    [get_disk(c[0], c[-1])["lower"].sum() for c in chunks_list]
                )
            if LOWER_MEMORY is not None:
                self.data_types[detector][f"lower_{detector}"]["memory"] = np.full(
                    len(chunks_list), LOWER_MEMORY, dtype=float
                )
            else:
                # If we are in LED mode, use the memory usage from the mode
                if self.is_led_mode:
                    coefficients = LED_MODES[self.mode]["memory"]
                else:
                    coefficients = _detector["memory"]["lower"]
                self.data_types[detector][f"lower_{detector}"]["memory"] = np.polyval(
                    coefficients,
                    [actual_sizes[c[0] : c[-1]].max() for c in chunks_list],
                )
            if UPPER_DISK is not None:
                self.data_types[detector][f"upper_{detector}"]["disk"] = UPPER_DISK
            else:
                self.data_types[detector][f"upper_{detector}"]["disk"] = get_disk(0, n_chunks)[
                    "upper"
                ].sum()
            if UPPER_MEMORY is not None:
                self.data_types[detector][f"upper_{detector}"]["memory"] = UPPER_MEMORY
            else:
                self.data_types[detector][f"upper_{detector}"]["memory"] = np.polyval(
                    _detector["memory"]["upper"], actual_sizes.sum()
                )
            if COMBINE_DISK is not None:
                self.data_types[detector][f"lower_{detector}"]["combine_disk"] = COMBINE_DISK
            else:
                self.data_types[detector][f"lower_{detector}"]["combine_disk"] = get_disk(
                    0, n_chunks
                )["combine"].sum()
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
                        usage = np.array(self.data_types[detector][label][prefix + md])
                        usage *= _detector["redundancy"][md]
                        if md == "memory" and usage.max() > MAX_MEMORY:
                            raise ValueError(
                                f"Memory usage {usage.max()} is too high for "
                                f"{detector} {label} {prefix + md}!"
                            )
                        if md == "disk" and usage.min() < MIN_DISK:
                            logger.warning(
                                f"Disk usage {usage.max()} is too low for "
                                f"{detector} {label} {prefix + md}! "
                                f"Will be set to {MIN_DISK}."
                            )
                            usage = np.maximum(usage, MIN_DISK)
                        self.data_types[detector][label][prefix + md] = usage.tolist()

    def dependency_exists(self, data_type="raw_records"):
        """Returns a boolean for whether the dependency exists in rucio and is accessible.

        It is a simplified version of DB.get_rses, and faster to just go through RunDB.

        """

        for rse in uconfig.getlist("Outsource", "raw_records_rses"):
            for data in self.run_data:
                if (
                    data["type"] == data_type
                    and data["host"] == "rucio-catalogue"
                    and data["status"] == "transferred"
                    and data["location"] == rse
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
        files = self.list_files(self.depends_on(data_type, lower=True))
        # Subtract 1 for metadata
        return len(files) - 1

    def set_requirements_base(self):
        requirements_base = (
            "HAS_SINGULARITY && HAS_CVMFS_xenon_opensciencegrid_org"
            " && PORT_2880 && PORT_8000 && PORT_27017"
            ' && Microarch >= "x86_64-v3"'
        )

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

    def _determine_target_sites(self, rses):
        """Given a list of RSEs, limit the runs for sites for those locations."""

        exprs = []
        sites = []
        ranks = []
        for rse in rses:
            if rse in self.rse_constraints:
                if "expr" in self.rse_constraints[rse]:
                    exprs.append(self.rse_constraints[rse]["expr"])
                if "site" in self.rse_constraints[rse]:
                    sites.append(self.rse_constraints[rse]["site"])
                if "rank" in self.rse_constraints[rse]:
                    ranks.append(self.rse_constraints[rse]["rank"])
        exprs = sorted(set(exprs))
        sites = sorted(set(sites))
        ranks = sorted(set(ranks))

        # make sure we do not request XENON1T sites we do not need
        if len(sites) == 0:
            sites.append("NONE")

        final_expr = " || ".join(exprs)
        desired_sites = ", ".join(sites)
        ranks = " + ".join(ranks)
        return final_expr, desired_sites, ranks

    def get_requirements(self, rses):
        # Determine the job requirements based on the data locations
        sites_expression, desired_sites, ranks = self._determine_target_sites(rses)
        requirements = self.requirements_base
        if sites_expression:
            if sites_expression.startswith("(") or "||" not in sites_expression:
                requirements += f" && {sites_expression}"
            else:
                requirements += f" && ({sites_expression})"
        # Add excluded nodes
        if self._exclude_sites:
            requirements += f" && {self._exclude_sites}"

        return desired_sites, requirements, ranks
