import itertools
from copy import deepcopy
from utilix import uconfig
from utilix import xent_collection
from utilix.config import setup_logger
import strax
import straxen
import cutax

from outsource.meta import DETECTOR_DATA_TYPES, PER_CHUNK_DATA_TYPES


coll = xent_collection()
logger = setup_logger("outsource")


def get_context(
    context,
    xedocs_version,
    input_path=None,
    output_path=None,
    staging_dir=None,
    ignore_processed=False,
):
    """Get straxen context."""
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
                take_only=tuple(st.root_data_types),
                rses_only=uconfig.getlist("Outsource", "raw_records_rse"),
            )
        )
        if not ignore_processed:
            st.storage.append(
                straxen.storage.RucioRemoteFrontend(
                    staging_dir=staging_dir,
                    download_heavy=True,
                    exclude=tuple(st.root_data_types),
                )
            )
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
    :param number_from: start run number
    :param number_to: end run number
    :param runlist: list of run numbers to process
    :return: list of run numbers

    """
    include_modes = uconfig.getlist("Outsource", "include_modes", fallback=[])
    exclude_modes = uconfig.getlist("Outsource", "exclude_modes", fallback=[])
    include_sources = uconfig.getlist("Outsource", "include_sources", fallback=[])
    exclude_sources = uconfig.getlist("Outsource", "exclude_sources", fallback=[])
    include_tags = uconfig.getlist("Outsource", "include_tags", fallback=[])
    exclude_tags = uconfig.getlist("Outsource", "exclude_tags", fallback=[])

    min_run_number = uconfig.getint("Outsource", "min_run_number", fallback=1)
    max_run_number = uconfig.getint("Outsource", "min_run_number", fallback=999999)
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
    basic_queries_to_process = []

    for det, det_info in DETECTOR_DATA_TYPES.items():
        if detector != "all" and detector != det:
            logger.warning(f"Skipping {det} data")
            continue

        # Check if the data_type is in the list of data_types to outsource
        to_process_data_types = list(set(det_info["to_process"]) & set(include_data_types))

        if not to_process_data_types:
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
                "location": {"$in": uconfig.getlist("Outsource", "raw_records_rse")},
            }
        }
        to_process_data_type_query = [
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
            for data_type in to_process_data_types
        ]

        # Basic query with raw data
        basic_query_has_raw = deepcopy(basic_query)
        basic_query_has_raw["data"] = has_raw_data_type_query
        basic_queries_has_raw.append(basic_query_has_raw)

        # Basic query without to_process data
        basic_query_to_process = deepcopy(basic_query)
        basic_query_to_process["$or"] = to_process_data_type_query
        basic_queries_to_process.append(basic_query_to_process)

    full_query_basic = {"$or": basic_queries}
    full_query_basic_has_raw = {"$or": basic_queries_has_raw}
    full_query_basic_to_process = {"$or": basic_queries_to_process}

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
    cursor_basic_to_process = coll.find(
        full_query_basic_to_process,
        {"number": 1, "mode": 1},
        limit=uconfig.getint("Outsource", "max_daily", fallback=None),
        sort=[("number", -1)],
    )

    runlist_basic = [r["number"] for r in cursor_basic]
    if not runlist_basic:
        raise ValueError("Nothing was found in RunDB for even the most basic requirement.")

    runlist_basic_has_raw = [r["number"] for r in cursor_basic_has_raw]
    logger.warning(
        "The following are the run numbers passing the basic queries and "
        f"have raw data available: {runlist_basic_has_raw}"
    )
    runlist_basic_to_process = [r["number"] for r in cursor_basic_to_process]
    logger.warning(
        "The following are the run numbers passing the basic queries and "
        f"have to be processed: {runlist_basic_to_process}"
    )

    if ignore_processed:
        runlist = list(set(runlist_basic_has_raw))
    else:
        runlist = list(set(runlist_basic_to_process) & set(runlist_basic_has_raw))

    return runlist


def get_possible_dependencies(st):
    # Get the data_types in the same plugin of PER_CHUNK_DATA_TYPES
    possible_dependencies = itertools.chain.from_iterable(
        st._plugin_class_registry[d]().provides for d in PER_CHUNK_DATA_TYPES
    )
    # Add the root_data_types because PER_CHUNK_DATA_TYPES depends on them
    possible_dependencies = set(possible_dependencies) | st.root_data_types
    return possible_dependencies


def get_to_save_data_types(st, data_types):
    plugins = st._get_plugins(strax.to_str_tuple(data_types), "0")
    to_process_data_types = set(
        [k for k, v in plugins.items() if v.save_when[k] == strax.SaveWhen.ALWAYS]
    )
    return to_process_data_types


def get_rse(st, data_type):
    # Based on the data_type and the utilix config, where should this data go?
    if data_type in st._get_plugins(["records", "records_nv", "records_he"], "0"):
        rse = uconfig.get("Outsource", "records_rse")
    elif data_type in st._get_plugins(["peaks", "hitlets_nv"], "0"):
        rse = uconfig.get("Outsource", "peaklets_rse")
    else:
        rse = uconfig.get("Outsource", "events_rse")
    return rse


def per_chunk_storage_root_data_type(st, run_id, data_type):
    """Return True if the data_type is per-chunk storage."""
    if data_type in st._get_plugins(PER_CHUNK_DATA_TYPES, "0"):
        # find the root data_type
        root_data_types = set(st._get_plugins((data_type,), run_id)) & set(st.root_data_types)
        if len(root_data_types) != 1:
            raise ValueError(
                f"Cannot determine root data type for {data_type} "
                f"because got multiple root data types {root_data_types}."
            )
        return root_data_types[0]
    else:
        return None
