import argparse
import os
import sys
import time
import shutil
import gc
from utilix import uconfig
from utilix.config import setup_logger
import admix
import strax
import straxen
import cutax

from outsource.config import get_bottom_data_types
from outsource.config import RECHUNK_DATA_TYPES, IGNORE_DATA_TYPES, BUDDY_DATA_TYPES, PRIORITY_RANK
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource")
straxen.Events.save_when = strax.SaveWhen.TARGET


def process(st, run_id, data_type, chunks):
    t0 = time.time()

    if chunks:
        assert data_type in RECHUNK_DATA_TYPES
        bottoms = get_bottom_data_types(data_type)
        assert len(bottoms) == 1
        st.make(
            run_id,
            data_type,
            chunk_number={bottoms[0]: chunks},
            processor="single_thread",
        )
    else:
        assert data_type not in RECHUNK_DATA_TYPES
        st.make(
            run_id,
            data_type,
            processor="single_thread",
        )

    process_time = time.time() - t0
    logger.info(f"Processing time for {data_type}: {process_time / 60:0.2f} minutes")


def main():
    parser = argparse.ArgumentParser(description="(Re)Processing With Outsource")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--context", required=True)
    parser.add_argument("--xedocs_version", required=True)
    parser.add_argument("--data_type", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--ignore_processed", action="store_true", dest="ignore_processed")
    parser.add_argument("--download_only", action="store_true", dest="download_only")
    parser.add_argument("--no_download", action="store_true", dest="no_download")
    parser.add_argument("--chunks", nargs="*", type=int)

    args = parser.parse_args()

    # Directory of input and output
    input_path = args.input_path
    output_path = args.output_path

    # Get context
    st = getattr(cutax.contexts, args.context)(xedocs_version=args.xedocs_version)
    staging_dir = "./strax_data"
    if os.path.abspath(staging_dir) == os.path.abspath(input_path):
        raise ValueError("Input path cannot be the same as staging directory")
    if os.path.abspath(staging_dir) == os.path.abspath(output_path):
        raise ValueError("Output path cannot be the same as staging directory")
    st.storage = [
        strax.DataDirectory(input_path, readonly=True),
        strax.DataDirectory(output_path),
        straxen.storage.RucioRemoteFrontend(
            staging_dir=staging_dir,
            download_heavy=True,
            take_only=tuple(st.root_data_types),
            rses_only=uconfig.getlist("Outsource", "raw_records_rse"),
        ),
    ]
    if not args.ignore_processed:
        st.storage + [
            straxen.storage.RucioRemoteFrontend(
                staging_dir=staging_dir,
                download_heavy=True,
                exclude=tuple(st.root_data_types),
            ),
        ]

    # Add local frontend if we can
    # This is a temporary hack
    try:
        st.storage.append(straxen.storage.RucioLocalFrontend())
    except KeyError:
        logger.info("No local RSE found")

    logger.info("Context is set up!")

    run_id = f"{args.run_id:06d}"
    data_type = args.data_type  # eg. typically for tpc: peaklets/event_info

    # Initialize plugin needed for processing this output type
    plugin = st._plugin_class_registry[data_type]()

    # Figure out what plugins we need to process/initialize
    to_process = [data_type]
    for buddies in BUDDY_DATA_TYPES:
        if data_type in buddies:
            to_process = list(buddies)
    # Remove duplicates
    to_process = list(set(to_process))

    # Keep track of the data we can download now -- will be important for the upload step later
    available_data_types = st.available_for_run(run_id)
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
            _plugin = st._get_plugins((_data_type,), run_id)[_data_type]
            # Adding missing dependencies to to-process list
            for dependency in _plugin.depends_on:
                if dependency not in available_data_types:
                    if dependency not in to_process:
                        to_process = [dependency] + to_process
                    new_intermediates.append(dependency)
        intermediates = new_intermediates

    # Remove any raw data
    to_process = [data_type for data_type in to_process if data_type not in admix.utils.RAW_DTYPES]

    missing = [d for d in to_process if d != data_type]
    (f"Need to create intermediate data: {', '.join(missing)}")

    logger.info(f"Available data: {available_data_types}")

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

    logger.info(f"To process: {to_process}")
    for data_type in to_process:
        process(st, run_id, data_type, args.chunks)
        gc.collect()

    logger.info("Done processing. Now check if we should upload to rucio")

    # Remove rucio directory
    shutil.rmtree(staging_dir)

    # Now loop over data_type we just made and upload the data
    processed_data = os.listdir(output_path)
    logger.info(f"Processed data: {processed_data}")

    for dirname in processed_data:
        path = os.path.join(output_path, dirname)

        # Get rucio dataset
        this_run, this_data_type, this_hash = dirname.split("-")

        # Remove data we do not want to upload
        if this_data_type in IGNORE_DATA_TYPES:
            logger.warning(f"Removing {this_data_type} instead of uploading")
            shutil.rmtree(path)
            continue

        if args.chunks:
            logger.warning(f"Skipping upload since we used per-chunk storage")
            continue

        if not args.rucio_upload:
            logger.warning("Ignoring rucio upload")
            continue

        upload_to_rucio(path, rundb_update=args.rundb_update)

        # Cleanup the files we uploaded
        shutil.rmtree(path)

    logger.info("ALL DONE!")


if __name__ == "__main__":
    main()
