import straxen


# These data_types need per-chunk storage, so don't upload to rucio here!
PER_CHUNK_DATA_TYPES = [
    "peaklets",
    "peaklet_classification",
    "peaklet_positions_cnf",
    "peaklet_positions_mlp",
    "hitlets_nv",
    "afterpulses",
    "led_calibration",
]


# Do a query to see if these data_types are present
DETECTOR_DATA_TYPES = {
    "tpc": {
        "raw": "raw_records",
        "per_chunk": True,
        "possible": [
            "peaklets",
            "peaklet_classification",
            "peaklet_positions_cnf",
            "peaklet_positions_mlp",
            "event_ms_naive",
            "event_info_double",
            "event_info",
            "event_position_uncertainty",
            "event_top_bottom_params",
            "event_pattern_fit",
            "veto_proximity",
            "event_ambience",
            "event_shadow",
            "event_se_score",
            "cuts_basic",
            "peak_s1_positions_cnn",
            "peak_basics_he",
            "afterpulses",
            "led_calibration",
        ],
        "rate": {
            "peaklets": 0.02,
            "lone_hits": 0.10,
            "merged_s2s": 0.01,
            "peaks": 0.01,
            "events": 0.001,
            "led_cal": 1.0,
            "afterpulses": None,
        },
        "compression": {
            "peaklets": 0.25,
            "lone_hits": 0.35,
            "merged_s2s": 0.25,
            "peaks": 0.75,
            "events": 0.60,
            "led_cal": 0.30,
            "afterpulses": None,
        },
        "memory": {
            "lower": [3.2, 1.2e3],
            "combine": [0.0, 2.0e3],
            "upper": [0.007, 4.0e3],
        },
        "redundancy": {
            "disk": 1.05,
            "memory": 1.05,
        },
    },
    "neutron_veto": {
        "raw": "raw_records_nv",
        "per_chunk": True,
        "possible": ["hitlets_nv", "events_nv", "event_positions_nv", "event_waveform_nv"],
        "keep_seconds": straxen.nVETORecorder.takes_config["keep_n_seconds_for_monitoring"].default,
        "rate": {
            "lone_raw_record_statistics_nv": [0, 0],
            "raw_records_coin_nv": [1.0, 0.02],
            "hitlets_nv": [1.05, 0.025],
            "events_nv": [0.0025, 0.0025],
        },
        "compression": {
            "lone_raw_record_statistics_nv": 0.8,
            "raw_records_coin_nv": 0.35,
            "hitlets_nv": 0.70,
            "events_nv": 0.15,
        },
        "memory": {
            "lower": [4.0, 0.0e3],
            "combine": [0.0, 2.0e3],
            "upper": [0.0, 4.0e3],
        },
        "redundancy": {
            "disk": 1.05,
            "memory": 1.05,
        },
    },
    "muon_veto": {
        "raw": "raw_records_mv",
        "per_chunk": False,
        "possible": ["events_mv"],
        "rate": {
            "hitlets_mv": 0.30,
            "events_mv": 0.003,
        },
        "compression": {
            "hitlets_mv": 0.70,
            "events_mv": 0.003,
        },
        "memory": {
            "lower": [4.0, 1.0e3],
            "combine": [0.0, 2.0e3],
            "upper": [0.0, 4.0e3],
        },
        "redundancy": {
            "disk": 1.05,
            "memory": 1.05,
        },
    },
}


# LED calibration modes have particular data_type we care about
LED_MODES = {
    "tpc_pmtap": {
        "possible": ["afterpulses"],
        "memory": None,
    },
    "tpc_pmtgain": {
        "possible": ["led_calibration"],
        "memory": [7.0, 0.8e3],
    },
    "tpc_commissioning_pmtap": {
        "possible": ["afterpulses"],
        "memory": None,
    },
}


def get_clean_per_chunk_data_types(context):
    """Remove data_types that are not registered at all."""
    return [dt for dt in PER_CHUNK_DATA_TYPES if dt in context._plugin_class_registry]


def get_clean_detector_data_types(context):
    """Remove data_types that are not registered at all from the list of possible data_types."""

    clean_detector_data_types = {}
    for detector, detector_dict in DETECTOR_DATA_TYPES.items():
        clean_detector_data_types[detector] = detector_dict.copy()
        clean_detector_data_types[detector]["possible"] = [
            dt for dt in detector_dict["possible"] if dt in context._plugin_class_registry
        ]
    return clean_detector_data_types
