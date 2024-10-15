# These data_types need per-chunk storage, so don't upload to rucio here!
PER_CHUNK_DATA_TYPES = [
    "peaklets",
    "hitlets_nv",
    "hitlets_mv",
    "afterpulses",
    "led_calibration",
]


# Do a query to see if these data_types are present
DETECTOR_DATA_TYPES = {
    "tpc": {
        "raw": "raw_records",
        "possible": [
            "peaklets",
            "event_ms_naive",
            "event_info_double",
            "event_position_uncertainty",
            "event_top_bottom_params",
            "event_pattern_fit",
            "veto_proximity",
            "event_ambience",
            "event_shadow",
            "event_se_density",
            "cuts_basic",
            "peak_s1_positions_cnn",
            "peak_basics_he",
            "afterpulses",
            "led_calibration",
        ],
    },
    "neutron_veto": {
        "raw": "raw_records_nv",
        "possible": ["hitlets_nv", "events_nv", "ref_mon_nv"],
    },
    "muon_veto": {
        "raw": "raw_records_mv",
        "possible": ["hitlets_mv", "events_mv"],
    },
}


# LED calibration modes have particular data_type we care about
LED_MODES = {
    "tpc_pmtap": ["afterpulses"],
    "tpc_commissioning_pmtap": ["afterpulses"],
    "tpc_pmtgain": ["led_calibration"],
}
