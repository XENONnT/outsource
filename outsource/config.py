import time
from itertools import chain
from utilix import DB, uconfig, xent_collection
import admix

from outsource.meta import PER_CHUNK_DATA_TYPES, DETECTOR_DATA_TYPES, LED_MODES
from outsource.utils import (
    get_possible_dependencies,
    get_to_save_data_types,
    per_chunk_storage_root_data_type,
)


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

    chunks_per_job = uconfig.getint("Outsource", "chunks_per_job", fallback=10)

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
        self._led_mode(include_data_types)

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
                    ret[detector][label] = dict()
                    ret[detector]["submitted"] = []
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
                ret[detector][label] = DataTypes()
                for data_type in data_types_group:
                    ret[detector][label][data_type] = {"missing": [], "processed": [], "rses": []}
                    # Expand the data_type to data_types that need to be saved
                    _data_types = get_to_save_data_types(self.context, data_type, rm_lower=group)
                    # Get Rucio Storage Elements of data_type that needed to be processed.
                    input_data_type = self.depends_on(data_type)
                    hash = self.key_for(input_data_type).lineage_hash
                    ret[detector][label][data_type]["rses"] = list(
                        set(db.get_rses(self.run_id, input_data_type, hash))
                    )
                    if self.ignore_processed:
                        ret[detector][label][data_type]["missing"] += list(_data_types)
                        continue
                    for _data_type in _data_types:
                        hash = self.key_for(_data_type).lineage_hash
                        rses = db.get_rses(self.run_id, _data_type, hash)
                        # If this data is not on any rse, reprocess it, or we are asking for a rerun
                        if rses:
                            ret[detector][label][data_type]["missing"].append(_data_type)
                        else:
                            ret[detector][label][data_type]["processed"].append(_data_type)

                ret[detector][label] = DataTypes(
                    sorted(
                        ret[detector][label].items(),
                        key=lambda item: self.context.tree_levels[item[0]]["order"],
                    )
                )
            # Summarize the submitted data_types for a detector
            ret["submitted"] += list(
                set().union(*[v.not_processed for v in ret[detector].values()])
            )
            if len(ret["submitted"]) != len(set(ret["submitted"])):
                raise ValueError("Why are there duplicated data_types in different detectors?")
        return ret

    def dependency_exists(self, data_type="raw_records"):
        """Returns a boolean for whether the dependency exists in rucio and is accessible.

        It is a simplified version of DB.get_rses, and faster to just go through RunDB.

        """

        for data in self.run_data:
            if (
                data["type"] == data_type
                and data["host"] == "rucio-catalogue"
                and data["status"] == "transferred"
                and data["location"] != "LNGS_USERDISK"
                and "TAPE" not in data["location"]
            ):
                return True
        return False

    def nchunks(self, data_type):
        # Get the data_type this one depends on
        data_type = self.depends_on(data_type)
        hash = self.key_for(data_type).lineage_hash
        did = f"xnt_{self._run_id}:{data_type}-{hash}"
        files = admix.rucio.list_files(did)
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
