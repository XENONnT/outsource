from itertools import chain
from copy import deepcopy
from utilix import uconfig
from utilix import xent_collection
from utilix.config import setup_logger
import strax
import straxen
import cutax

from outsource.meta import get_clean_detector_data_types, get_clean_per_chunk_data_types


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
coll = xent_collection()


def get_context(
    context,
    xedocs_version,
    input_path=None,
    output_path=None,
    staging_dir=None,
    ignore_processed=False,
    stage=False,
):
    """Get straxen context for given context name and xedocs_version."""
    st = getattr(cutax.contexts, context)(xedocs_version=xedocs_version)
    st.storage = []
    if input_path:
        st.storage.append(strax.DataDirectory(input_path, readonly=True))
    if output_path:
        st.storage.append(strax.DataDirectory(output_path))
    if staging_dir:
        st.storage.append(
            straxen.storage.RucioRemoteFrontend(
                staging_dir=staging_dir,
                download_heavy=True,
                stage=stage,
                take_only=tuple(st.root_data_types),
                rses_only=uconfig.getlist("Outsource", "raw_records_rses"),
            )
        )
        if not ignore_processed:
            st.storage.append(
                straxen.storage.RucioRemoteFrontend(
                    staging_dir=staging_dir,
                    download_heavy=True,
                    stage=stage,
                    exclude=tuple(st.root_data_types),
                )
            )
    st.purge_unused_configs()
    return st


def get_runlist(
    st,
    detector,
    runlist=None,
    number_from=None,
    number_to=None,
    ignore_processed=False,
):
    """Find data to outsource.

    Check if dependencies are available in RunDB.
    :param st: straxen context
    :param detector: detector to process
    :param number_from: start run_id
    :param number_to: end run_id
    :param runlist: list of run_ids to process
    :return: list of run_ids

    """
    include_modes = uconfig.getlist("Outsource", "include_modes", fallback=[])
    exclude_modes = uconfig.getlist("Outsource", "exclude_modes", fallback=[])
    include_sources = uconfig.getlist("Outsource", "include_sources", fallback=[])
    exclude_sources = uconfig.getlist("Outsource", "exclude_sources", fallback=[])
    include_tags = uconfig.getlist("Outsource", "include_tags", fallback=[])
    exclude_tags = uconfig.getlist("Outsource", "exclude_tags", fallback=[])

    min_run_number = uconfig.getint("Outsource", "min_run_number", fallback=1)
    max_run_number = uconfig.getint("Outsource", "max_run_number", fallback=999999)
    if number_from is not None:
        min_run_number = max(number_from, min_run_number)
    if number_to is not None:
        max_run_number = min(number_to, max_run_number)

    number_query = {"$gte": min_run_number, "$lte": max_run_number}
    if runlist:
        number_query["$in"] = runlist

    include_data_types = uconfig.getlist("Outsource", "include_data_types")

    # Setup queries for different detectors
    basic_queries = []
    basic_queries_has_raw = []
    basic_queries_to_save = []
    detector_data_types = get_clean_detector_data_types(st)
    for det, det_info in detector_data_types.items():
        if detector != "all" and detector != det:
            logger.warning(f"Skipping {det} data")
            continue

        # Check if the data_type is in the list of data_types to outsource
        possible_data_types = list(set(det_info["possible"]) & set(include_data_types))
        to_save_data_types = get_to_save_data_types(st, possible_data_types, rm_lower=False)

        if not to_save_data_types:
            logger.warning(f"Skipping {det} data")
            continue

        # Basic query
        basic_query = {"number": number_query, "detectors": det}
        for key, values in zip(
            ["source", "mode", "tags.name"],
            [
                [include_sources, exclude_sources],
                [include_modes, exclude_modes],
                [include_tags, exclude_tags],
            ],
        ):
            _query = dict()
            if values[0]:
                _query["$in"] = values[0]
            if values[1]:
                _query["$nin"] = values[1]
            if _query:
                basic_query[key] = deepcopy(_query)

        basic_queries.append(basic_query)

        has_raw_data_type_query = {
            "$elemMatch": {
                "type": det_info["raw"],
                "host": "rucio-catalogue",
                "status": "transferred",
                "location": {"$in": uconfig.getlist("Outsource", "raw_records_rses")},
            }
        }
        to_save_data_type_query = [
            {
                "data": {
                    "$not": {
                        "$elemMatch": {
                            "host": "rucio-catalogue",
                            "type": data_type,
                            "status": "transferred",
                            "did": {"$regex": st.key_for("0", data_type).lineage_hash},
                        }
                    }
                }
            }
            for data_type in to_save_data_types
        ]

        # Basic query with raw data
        basic_query_has_raw = deepcopy(basic_query)
        basic_query_has_raw["data"] = has_raw_data_type_query
        basic_queries_has_raw.append(basic_query_has_raw)

        # Basic query without to_save data
        basic_query_to_save = deepcopy(basic_query)
        basic_query_to_save["$or"] = to_save_data_type_query
        basic_queries_to_save.append(basic_query_to_save)

    full_query_basic = {"$or": basic_queries}
    full_query_basic_has_raw = {"$or": basic_queries_has_raw}
    full_query_basic_to_save = {"$or": basic_queries_to_save}

    cursor_basic = coll.find(
        full_query_basic,
        {"number": 1, "mode": 1},
        limit=uconfig.getint("Outsource", "max_daily", fallback=None),
        sort=[("number", -1)],
    )
    cursor_basic_has_raw = coll.find(
        full_query_basic_has_raw,
        {"number": 1, "mode": 1},
        limit=uconfig.getint("Outsource", "max_daily", fallback=None),
        sort=[("number", -1)],
    )
    cursor_basic_to_save = coll.find(
        full_query_basic_to_save,
        {"number": 1, "mode": 1},
        limit=uconfig.getint("Outsource", "max_daily", fallback=None),
        sort=[("number", -1)],
    )

    runlist_basic = [r["number"] for r in cursor_basic]
    if not runlist_basic:
        raise ValueError("Nothing was found in RunDB for even the most basic requirement.")

    runlist_basic_has_raw = sorted(r["number"] for r in cursor_basic_has_raw)
    logger.info(
        f"The following are the {len(runlist_basic_has_raw)} run_ids passing the basic queries and "
        f"have raw data available: {runlist_basic_has_raw}"
    )
    runlist_basic_to_save = sorted(r["number"] for r in cursor_basic_to_save)
    logger.info(
        f"The following are the {len(runlist_basic_to_save)} run_ids passing the basic queries and "
        f"have to be processed: {runlist_basic_to_save}"
    )

    if ignore_processed:
        runlist = sorted(set(runlist_basic_has_raw))
    else:
        runlist = sorted(set(runlist_basic_to_save) & set(runlist_basic_has_raw))

    return runlist


def get_possible_dependencies(st, lower=False):
    """Expand the by-product of PER_CHUNK_DATA_TYPES.

    If lower is True, directly return the root_data_types, which are `raw_records_*`.

    For example, "lone_hits" is not in PER_CHUNK_DATA_TYPES so can not be directly requested via
    include_data_types in XENON_CONFIG. But it is a by-product of "peaklets" which is in
    PER_CHUNK_DATA_TYPES. Sometimes a plugin can only depends on the by-product of
    PER_CHUNK_DATA_TYPES like lone_hits. This function is to get all possible dependencies of
    PER_CHUNK_DATA_TYPES.

    """
    if lower:
        return st.root_data_types

    # Get the data_types in the same plugin of PER_CHUNK_DATA_TYPES
    per_chunk_data_types = get_clean_per_chunk_data_types(st)
    possible_dependencies = chain.from_iterable(
        st._plugin_class_registry[d]().provides for d in per_chunk_data_types
    )
    # Add the root_data_types because PER_CHUNK_DATA_TYPES depends on them
    possible_dependencies = set(possible_dependencies) | st.root_data_types
    return possible_dependencies


def get_to_save_data_types(st, data_types, rm_lower=False):
    """Get the data_types that should be saved, disregarding the storage.

    These are the expected data_types to be saved when running get_array.

    """
    plugins = st._get_plugins(strax.to_str_tuple(data_types), "0")
    possible_data_types = set(
        [k for k, v in plugins.items() if v.save_when[k] == strax.SaveWhen.ALWAYS]
    )
    possible_data_types -= st.root_data_types
    per_chunk_data_types = get_clean_per_chunk_data_types(st)
    if rm_lower:
        # Remove all data_types to be saved when processing PER_CHUNK_DATA_TYPES
        possible_data_types -= get_to_save_data_types(st, per_chunk_data_types, False)
    return possible_data_types


def get_rse(st, data_type):
    """Based on the data_type and the utilix config, where should this data go?"""
    if data_type in st._get_plugins(["records", "records_nv", "records_he"], "0"):
        rse = uconfig.get("Outsource", "records_rse")
    elif data_type in st._get_plugins(["peaks", "hitlets_nv"], "0"):
        rse = uconfig.get("Outsource", "peaklets_rse")
    else:
        rse = uconfig.get("Outsource", "events_rse")
    return rse


def per_chunk_storage_root_data_type(st, run_id, data_type):
    """Return root dependency if the data_type is per-chunk storage.

    A data_type is per-chunk storage if it is in PER_CHUNK_DATA_TYPES. The returned data_type is the
    root data_type of the data_type. For exmaple, it will return "raw_records" for "peaklets".

    """

    # Filter out the data_types that are not in the registered plugins
    per_chunk_data_types = get_clean_per_chunk_data_types(st)

    # First filter on the existing and registered data_types
    per_chunk_data_types = [d for d in per_chunk_data_types if d in st._plugin_class_registry]

    if data_type in st._get_plugins(per_chunk_data_types, "0"):
        # find the root data_type
        root_data_types = list(set(st.get_dependencies(data_type)) & st.root_data_types)
        if len(root_data_types) > 1:
            raise ValueError(
                f"Cannot determine root data type for {data_type} "
                f"because got multiple root data types {root_data_types}."
            )
        return root_data_types[0]
    else:
        return None


def get_processing_order(st, data_types, rm_lower=False):
    """Instruction on which data need to be processed first to avoid duplicated computing.

    This function is different from the above. For example, event_pattern_fit depends on
    event_area_per_channel:
    https://github.com/XENONnT/straxen/blob/764f14cbc16c8633e176ca8c0a93c589293c24e0/straxen/plugins/events/event_pattern_fit.py#L15.
    event_area_per_channel is not always saved, but while processing event_area_per_channel,
    event_n_channel will be saved. In this case, even though event_n_channel is lower than
    event_pattern_fit, we should not directly process event_n_channel. Instead, we should
    process event_area_per_channel first.

    """
    # Only consider the parents but not uncle and aunt
    _data_types = set().union(*[st.get_dependencies(d) for d in strax.to_str_tuple(data_types)])
    # add back the directly called data_types
    _data_types |= set(strax.to_str_tuple(data_types))
    # Now we want to know which data_types are to be saved
    _data_types &= get_to_save_data_types(st, tuple(_data_types), rm_lower=rm_lower)
    if rm_lower:
        # Remove all data_types to be saved when processing PER_CHUNK_DATA_TYPES
        per_chunk_data_types = get_clean_per_chunk_data_types(st)
        _data_types -= set(get_processing_order(st, per_chunk_data_types, False))
    _data_types = sorted(_data_types, key=lambda x: st.tree_levels[x]["order"])
    return _data_types
