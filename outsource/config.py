import os
import time

from utilix import DB, uconfig, xent_collection
import admix


base_dir = os.path.abspath(os.path.dirname(__file__))


# These are the developer decided dependencies of data_type
DEPENDS_ON = {
    "records": ["raw_records"],
    "peaklets": ["raw_records"],
    "peak_basics": ["peaklets"],
    "peak_basics_he": ["raw_records_he"],
    "event_info_double": ["peaklets"],
    "event_shadow": ["peaklets"],
    "hitlets_nv": ["raw_records_nv"],
    "events_nv": ["hitlets_nv"],
    "ref_mon_nv": ["hitlets_nv"],
    "events_mv": ["raw_records_mv"],
    "afterpulses": ["raw_records"],
    "led_calibration": ["raw_records"],
}

# These are datetypes to look for in RunDB
ACTUALLY_STORED = {
    "event_info_double": [
        "peak_basics",
        "event_info",
        "distinct_channels",
        "event_pattern_fit",
        "event_area_per_channel",
        "event_n_channel",
        "event_top_bottom_params",
        "event_ms_naive",
        "event_ambience",
        "event_shadow",
        "peak_s1_positions_cnn",
    ],
    "event_shadow": ["event_shadow", "event_ambience"],
    "peak_basics_he": ["peak_basics_he"],
    "events_nv": ["ref_mon_nv", "events_nv"],
    "ref_mon_nv": ["ref_mon_nv"],
    "peak_basics": ["merged_s2s", "peak_basics", "peaklet_classification"],
    "peaklets": ["peaklets", "lone_hits"],
    "hitlets_nv": ["hitlets_nv"],
    "events_mv": ["events_mv"],
    "afterpulses": ["afterpulses"],
    "led_calibration": ["led_calibration"],
}

# Do a query to see if these data_types are present
DETECTOR_DATA_TYPES = {
    "tpc": {
        "raw": "raw_records",
        "to_process": [
            "peaklets",
            "event_info",
            "peak_basics",
            "peak_basics_he",
            "event_pattern_fit",
            "event_area_per_channel",
            "event_top_bottom_params",
            "event_ms_naive",
            "event_shadow",
            "event_ambience",
            "peak_s1_positions_cnn",
            "afterpulses",
        ],
        "possible": [
            "records",
            "peaklets",
            "peak_basics",
            "event_info_double",
            "event_shadow",
            "peak_basics_he",
            "afterpulses",
            "led_calibration",
        ],
    },
    "neutron_veto": {
        "raw": "raw_records_nv",
        "to_process": ["hitlets_nv", "events_nv", "ref_mon_nv"],
        "possible": ["hitlets_nv", "events_nv", "ref_mon_nv"],
    },
    "muon_veto": {"raw": "raw_records_mv", "to_process": ["events_mv"], "possible": ["events_mv"]},
}

PER_CHUNK_DATA_TYPES = ["records", "peaklets", "hitlets_nv", "afterpulses", "led_calibration"]
NEED_RAW_DATA_TYPES = [
    "peaklets",
    "peak_basics_he",
    "hitlets_nv",
    "events_mv",
    "afterpulses",
    "led_calibration",
]

# LED calibration modes have particular data_type we care about
LED_MODES = {
    "tpc_pmtap": ["afterpulses"],
    "tpc_commissioning_pmtap": ["afterpulses"],
    "tpc_pmtgain": ["led_calibration"],
}

# LED calibration particular data_type we care about
LED_DATA_TYPES = list(set().union(*LED_MODES.values()))

db = DB()
coll = xent_collection()


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

    chunks_per_job = uconfig.getint("Outsource", "chunks_per_job")

    def __init__(self, context, run_id, force=False, standalone_download=False):
        self.context = context
        self.run_id = run_id
        self.force = force
        self.standalone_download = standalone_download

        # Default job priority - workflows will be given priority
        # in the order they were submitted.
        self.priority = 2250000000 - int(time.time())
        assert self.priority > 0

        self.run_data = db.get_data(self.run_id)
        self.set_requirements_base()

        # Get the detectors and start time of this run
        cursor = coll.find_one(
            {"number": self.run_id}, {"detectors": 1, "start": 1, "_id": 0, "mode": 1}
        )
        self.detectors = cursor["detectors"]
        self.start = cursor["start"]
        self.mode = cursor["mode"]
        assert isinstance(
            self.detectors, list
        ), f"Detectors needs to be a list, not a {type(self.detectors)}"

        # Get the data_type that need to be processed
        self.needs_processed = self.get_needs_processed()

        # Determine which rse the input data is on
        self.dependencies_rses = self.get_dependencies_rses()

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

    def depends_on(self, data_type):
        return DEPENDS_ON[data_type]

    def key_for(self, data_type):
        return self.context.key_for(f"{self.run_id:06d}", data_type)

    def get_needs_processed(self):
        """Returns the list of data_type we need to process."""
        # Do we need to process? read from xenon_config
        include_data_types = uconfig.getlist("Outsource", "include_data_types")

        if self.mode in LED_MODES:
            # If we are using LED data, only process those data_types
            # For this context, see if we have that data yet
            include_data_types = [
                data_type for data_type in include_data_types if data_type in LED_MODES[self.mode]
            ]
        else:
            # If we are not, don't process those data_types
            include_data_types = list(set(include_data_types) - set(LED_DATA_TYPES))

        # Get all possible data_types we can process for this run
        possible_data_types = []
        for detector in self.detectors:
            possible_data_types.extend(DETECTOR_DATA_TYPES[detector]["possible"])

        # Modify include_data_types to only consider the possible ones
        include_data_types = [
            data_type for data_type in include_data_types if data_type in possible_data_types
        ]

        ret = []
        for category in include_data_types:
            data_types_already_processed = []
            for data_type in ACTUALLY_STORED[category]:
                hash = self.context.key_for(f"{self.run_id:06d}", data_type).lineage_hash
                rses = db.get_rses(self.run_id, data_type, hash)
                # If this data is not on any rse, reprocess it, or we are asking for a rerun
                data_types_already_processed.append(len(rses) > 0)
            if not all(data_types_already_processed) or self.force:
                ret.append(category)

        ret.sort(key=lambda x: len(self.context.get_dependencies(x)))

        return ret

    def get_dependencies_rses(self):
        """Get Rucio Storage Elements of data_type."""
        rses = dict()
        for data_type in self.needs_processed:
            input_data_types = self.depends_on(data_type)
            _rses_tmp = []
            for input_data_type in input_data_types:
                hash = self.context.key_for(f"{self.run_id:06d}", input_data_type).lineage_hash
                _rses_tmp.extend(db.get_rses(self.run_id, input_data_type, hash))
            rses[data_type] = list(set(_rses_tmp))
        return rses

    def nchunks(self, data_type):
        # Get the data_type this one depends on
        data_type = self.depends_on(data_type)[0]
        hash = self.context.key_for(f"{self.run_id:06d}", data_type).lineage_hash
        did = f"xnt_{self.run_id:06d}:{data_type}-{hash}"
        files = admix.rucio.list_files(did)
        # Subtract 1 for metadata
        return len(files) - 1

    def _raw_data_exists(self, raw_type="raw_records"):
        """Returns a boolean for whether or not raw data exists in rucio and is
        accessible."""
        # It's faster to just go through RunDB

        for data in self.run_data:
            if (
                data["type"] == raw_type
                and data["host"] == "rucio-catalogue"
                and data["status"] == "transferred"
                and data["location"] != "LNGS_USERDISK"
                and "TAPE" not in data["location"]
            ):
                return True
        return False

    def _determine_target_sites(self, rses):
        """Given a list of RSEs, limit the runs for sites for those
        locations."""

        exprs = []
        sites = []
        for rse in rses:
            if rse in self.rse_site_map:
                if "expr" in self.rse_site_map[rse]:
                    exprs.append(self.rse_site_map[rse]["expr"])
                if "desired_sites" in self.rse_site_map[rse]:
                    sites.append(self.rse_site_map[rse]["desired_sites"])

        # make sure we do not request XENON1T sites we do not need
        if len(sites) == 0:
            sites.append("NONE")

        final_expr = " || ".join(exprs)
        desired_sites = ",".join(sites)
        return final_expr, desired_sites
