import os
import argparse
import time
import shutil
import gc
from utilix import uconfig
from utilix.config import setup_logger
import admix
import straxen

from outsource.utils import get_context, get_processing_order, per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio

from rframe.interfaces.mongo import MongoAggregation
from rframe.interfaces.mongo import MultiMongoAggregation

# Allow disk use for mongo aggregation
MongoAggregation.allow_disk_use = True
MultiMongoAggregation.allow_disk_use = True

logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))

if not straxen.HAVE_ADMIX:
    raise ImportError("straxen must be installed with admix to use this script")
if not admix.manager.HAVE_GFAL2:
    raise ImportError("admix must be installed with gfal2 to use this script")


def get_chunk_number(st, run_id, data_type, chunks):
    """Get chunk_number for per-chunk storage."""
    root_data_type = per_chunk_storage_root_data_type(st, run_id, data_type)
    if chunks:
        assert root_data_type is not None
        chunk_number = {root_data_type: chunks}
    else:
        assert root_data_type is None
        chunk_number = None
    return chunk_number


def process(st, run_id, data_type, chunks):
    t0 = time.time()

    st.make(
        run_id,
        data_type,
        save=data_type,
        chunk_number=get_chunk_number(st, run_id, data_type, chunks),
        progress_bar=True,
    )
    gc.collect()

    process_time = time.time() - t0
    logger.info(f"Processing time for {data_type}: {process_time / 60:0.2f} minutes")


def main():
    parser = argparse.ArgumentParser(description="(Re)Processing With Outsource")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--context", required=True)
    parser.add_argument("--xedocs_version", required=True)
    parser.add_argument("--chunks_start", type=int, required=True)
    parser.add_argument("--chunks_end", type=int, required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--data_types", nargs="*", required=True)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--ignore_processed", action="store_true", dest="ignore_processed")
    parser.add_argument("--stage", action="store_true", dest="stage")
    parser.add_argument("--remove_heavy", action="store_true", dest="remove_heavy")

    args = parser.parse_args()

    # Directory of input and output
    input_path = args.input_path
    output_path = args.output_path

    # Get context
    staging_dir = "./strax_data"
    if os.path.abspath(staging_dir) == os.path.abspath(input_path):
        raise ValueError("Input path cannot be the same as staging directory")
    if os.path.abspath(staging_dir) == os.path.abspath(output_path):
        raise ValueError("Output path cannot be the same as staging directory")
    st = get_context(
        args.context,
        args.xedocs_version,
        input_path,
        output_path,
        staging_dir,
        ignore_processed=args.ignore_processed,
        remove_heavy=args.remove_heavy,
        stage=args.stage,
    )

    logger.info("Context is set up!")

    run_id = f"{args.run_id:06d}"
    data_types = args.data_types

    if args.chunks_start == args.chunks_end:
        chunks = None
    else:
        chunks = list(range(args.chunks_start, args.chunks_end))

    # Get the order of data_types in processing
    data_types = get_processing_order(st, data_types, rm_lower=chunks is None)

    if not data_types:
        raise ValueError("No data types to process, something is wrong")

    logger.info(f"To process: {data_types}")
    for data_type in data_types:
        logger.info(f"Processing: {data_type}")
        process(st, run_id, data_type, chunks)

    logger.info("Done processing. Now check if we should upload to rucio")

    # Now loop over data_type we just made and upload the data
    processed_data = os.listdir(output_path)
    logger.info(f"Processed data: {processed_data}")

    if chunks:
        logger.warning("Skipping upload since we used per-chunk storage")
    if not args.rucio_upload:
        logger.warning("Ignoring rucio upload")
    else:
        for dirname in processed_data:
            path = os.path.join(output_path, dirname)

            # Upload to rucio
            if os.path.isdir(path):
                upload_to_rucio(st, path, args.rundb_update)

            # Cleanup the files we uploaded
            shutil.rmtree(path)

    logger.info("ALL DONE!")


if __name__ == "__main__":
    main()
