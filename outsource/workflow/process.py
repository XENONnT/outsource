#!/usr/bin/env python3
import argparse
import os
import sys
import time
import shutil
import gc
import admix
import strax
import straxen
import cutax

from .upload import get_bottom_data_types, upload_to_rucio

straxen.Events.save_when = strax.SaveWhen.TARGET

# These data_types we need to rechunk, so don't upload to rucio here!
RECHUNK_DATA_TYPES = [
    "pulse_counts",
    "veto_regions",
    "records",
    "peaklets",
    "lone_hits",
    "hitlets_nv",
    "afterpulses",
    "led_calibration",
]

# These data_types will not be uploaded to rucio, and will be removed after processing
IGNORE_DATA_TYPES = [
    "records",
    "records_nv",
    "lone_raw_records_nv",
    "raw_records_coin_nv",
    "lone_raw_record_statistics_nv",
    "records_he",
    "records_mv",
    "peaks",
    "peaklets",  # added to avoid duplicating upload/staging
    "lone_hites",  # added to avoid duplicating upload/staging
]

# These data_types should always be made at the same time:
BUDDY_DATA_TYPES = [
    ("veto_regions_nv", "event_positions_nv"),
    (
        "event_info_double",
        "event_pattern_fit",
        "event_area_per_channel",
        "event_top_bottom_params",
        "event_ms_naive",
        "peak_s1_positions_cnn",
        "event_ambience",
        "event_shadow",
        "cuts_basic",
    ),
    ("event_shadow", "event_ambience"),
    ("events_nv", "ref_mon_nv"),
]

# These are the data_types we want to make first if any of them is in to-process list
PRIORITY_RANK = [
    "peaklet_classification",
    "merged_s2s",
    "peaks",
    "peak_basics",
    "peak_positions_mlp",
    "peak_positions_gcn",
    "peak_positions_cnn",
    "peak_positions",
    "peak_proximity",
    "events",
    "event_basics",
]


def process(run_id, out_data_type, st, chunks):
    run_id_str = f"{run_id:06d}"
    t0 = time.time()

    if chunks:
        assert out_data_type in RECHUNK_DATA_TYPES
        bottoms = get_bottom_data_types(out_data_type)
        assert len(bottoms) == 1
        st.make(
            run_id_str,
            out_data_type,
            chunk_number={bottoms[0]: chunks},
            processor="single_thread",
        )
    else:
        assert out_data_type not in RECHUNK_DATA_TYPES
        st.make(
            run_id_str,
            out_data_type,
            processor="single_thread",
        )

    process_time = time.time() - t0
    print(f"=== Processing time for {out_data_type}: {process_time / 60:0.2f} minutes === ")


def main():
    parser = argparse.ArgumentParser(description="(Re)Processing With Outsource")
    parser.add_argument("run_id", help="Run number", type=int)
    parser.add_argument("--context", help="name of context")
    parser.add_argument("--xedocs_version", help="xedocs global version")
    parser.add_argument("--data_type", help="desired strax data_type")
    parser.add_argument("--chunks", nargs="*", help="chunk numbers to download", type=int)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--download_only", action="store_true", dest="download_only")
    parser.add_argument("--no_download", action="store_true", dest="no_download")

    args = parser.parse_args()

    # Directory where we will be putting everything
    data_dir = "./data"

    # Make sure this is empty
    # if os.path.exists(data_dir):
    #     shutil.rmtree(data_dir)

    # Get context
    st = getattr(cutax.contexts, args.context)(xedocs_version=args.xedocs_version)
    # st.storage = [
    #     strax.DataDirectory(data_dir),
    #     straxen.rucio.RucioFrontend(
    #         include_remote=True, download_heavy=True, staging_dir=os.path.join(data_dir, "rucio")
    #     ),
    # ]
    st.storage = [
        strax.DataDirectory(data_dir),
        straxen.storage.RucioRemoteFrontend(download_heavy=True),
    ]

    # Add local frontend if we can
    # This is a temporary hack
    try:
        st.storage.append(straxen.storage.RucioLocalFrontend())
    except KeyError:
        print("No local RSE found")

    print("Context is set up!")

    run_id = args.run_id
    run_id_str = f"{run_id:06d}"
    out_data_type = args.data_type  # eg. typically for tpc: peaklets/event_info

    # Initialize plugin needed for processing this output type
    plugin = st._plugin_class_registry[out_data_type]()

    # Figure out what plugins we need to process/initialize
    to_process = [args.data_type]
    for buddies in BUDDY_DATA_TYPES:
        if args.data_type in buddies:
            to_process = list(buddies)
    # Remove duplicates
    to_process = list(set(to_process))

    # Keep track of the data we can download now -- will be important for the upload step later
    available_data_types = st.available_for_run(run_id_str)
    available_data_types = available_data_types[
        available_data_types.is_stored
    ].target.values.tolist()

    missing = set(plugin.depends_on) - set(available_data_types)
    intermediates = missing.copy()
    to_process = list(intermediates) + to_process

    # Now we need to figure out what intermediate data we need to make
    while len(intermediates) > 0:
        new_intermediates = []
        for _data_type in intermediates:
            _plugin = st._get_plugins((_data_type,), run_id_str)[_data_type]
            # Adding missing dependencies to to-process list
            for dependency in _plugin.depends_on:
                if dependency not in available_data_types:
                    if dependency not in to_process:
                        to_process = [dependency] + to_process
                    new_intermediates.append(dependency)
        intermediates = new_intermediates

    # Remove any raw data
    to_process = [data_type for data_type in to_process if data_type not in admix.utils.RAW_DTYPES]

    missing = [d for d in to_process if d != args.data_type]
    print(f"Need to create intermediate data: {', '.join(missing)}")

    print("-- Available data --")
    for dd in available_data_types:
        print(dd)
    print("-------------------\n")

    if args.download_only:
        sys.exit(0)

    # If to-process has anything in PRIORITY_RANK, we process them first
    if len(set(PRIORITY_RANK) & set(to_process)) > 0:
        # Remove any prioritized data_types that are not in to_process
        filtered_priority_rank = [
            data_type for data_type in PRIORITY_RANK if data_type in to_process
        ]
        # Remove the PRIORITY_RANK data_types from to_process,
        # as low priority data_type which we don't rigorously care their order
        to_process_low_priority = [dt for dt in to_process if dt not in filtered_priority_rank]
        # Sort the priority by their dependencies
        to_process = filtered_priority_rank + to_process_low_priority

    print(f"To process: {', '.join(to_process)}")
    for data_type in to_process:
        process(run_id, data_type, st, args.chunks)
        gc.collect()

    print("Done processing. Now check if we should upload to rucio")

    # Remove rucio directory
    shutil.rmtree(st.storage[1]._get_backend("RucioRemoteBackend").staging_dir)

    # Now loop over data_type we just made and upload the data
    processed_data = [d for d in os.listdir(data_dir)]
    print("---- Processed data ----")
    for d in processed_data:
        print(d)
    print("------------------------\n")

    if args.chunks:
        print(f"Skipping upload since we used per-chunk storage")
        processed_data = []

    for dirname in processed_data:
        path = os.path.join(data_dir, dirname)

        # Get rucio dataset
        this_run, this_data_type, this_hash = dirname.split("-")

        # Remove data we do not want to upload
        if this_data_type in IGNORE_DATA_TYPES:
            print(f"Removing {this_data_type} instead of uploading")
            shutil.rmtree(path)
            continue

        if not args.rucio_upload:
            print("Ignoring rucio upload")
            continue

        upload_to_rucio(path, rundb_update=args.rundb_update)

        # Cleanup the files we uploaded
        shutil.rmtree(path)

    print("ALL DONE!")


if __name__ == "__main__":
    main()
