import argparse
import os
import time
import shutil
import gc
from utilix.config import setup_logger

from outsource.meta import PER_CHUNK_DATA_TYPES
from outsource.utils import get_context, get_to_save_data_types, per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource")


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
        chunk_number=get_chunk_number(st, run_id, data_type, chunks),
        processor="single_thread",
    )

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
    parser.add_argument("--download_only", action="store_true", dest="download_only")
    parser.add_argument("--no_download", action="store_true", dest="no_download")

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
    )

    logger.info("Context is set up!")

    run_id = f"{args.run_id:06d}"
    data_types = args.data_types
    # Sanity check
    if args.chunks_start == args.chunks_end or args.download_only:
        if len(data_types) != 1:
            raise ValueError(
                "Cannot process multiple data types with per-chunk storage. "
                f"'--data_types' should be a single data type, got {data_types}."
            )
    if args.chunks_start == args.chunks_end:
        chunks = None
        chunk_number = None
    else:
        chunks = list(range(args.chunks_start, args.chunks_end))
        chunk_number = {data_types[0]: chunks}

    if args.download_only:
        st.get_array(run_id, data_types, chunk_number=chunk_number)
        return

    data_types = get_to_save_data_types(st, data_types)
    if chunks:
        # Remove all data_types to be saved when processing the dependencies of PER_CHUNK_DATA_TYPES
        per_chunk_dependencies = list(
            set().union(*[st.get_dependencies(d) for d in PER_CHUNK_DATA_TYPES])
        )
    else:
        # Remove all data_types to be saved when processing PER_CHUNK_DATA_TYPES
        per_chunk_dependencies = PER_CHUNK_DATA_TYPES
    data_types -= get_to_save_data_types(st, per_chunk_dependencies)
    data_types = sorted(data_types, key=lambda x: st.tree_levels[x]["order"])

    logger.info(f"To process: {data_types}")
    for data_type in data_types:
        process(st, run_id, data_type, chunks)
        gc.collect()

    logger.info("Done processing. Now check if we should upload to rucio")

    # Remove rucio directory
    shutil.rmtree(staging_dir)

    # Now loop over data_type we just made and upload the data
    processed_data = os.listdir(output_path)
    logger.info(f"Processed data: {processed_data}")

    if chunks:
        logger.warning(f"Skipping upload since we used per-chunk storage")
    if not args.rucio_upload:
        logger.warning("Ignoring rucio upload")
    else:
        for dirname in processed_data:
            path = os.path.join(output_path, dirname)

            # Upload to rucio
            upload_to_rucio(st, path, rundb_update=args.rundb_update)

            # Cleanup the files we uploaded
            shutil.rmtree(path)

    logger.info("ALL DONE!")


if __name__ == "__main__":
    main()
