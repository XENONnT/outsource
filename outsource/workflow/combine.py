import argparse
import os
import shutil
import admix
from utilix import uconfig
from utilix.config import setup_logger

from outsource.utils import get_context, per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
admix.clients._init_clients()


def merge(st, run_id, data_type, chunk_number_group):
    """Merge per-chunk storage for a given data_type.

    :param st: straxen context
    :param run_id: run_id padded with 0s
    :param data_type: data_type to be merged
    :param chunk_number_group: list of chunk number to merge.

    """
    root_data_type = per_chunk_storage_root_data_type(st, run_id, data_type)
    assert root_data_type is not None
    st.merge_per_chunk_storage(
        run_id,
        data_type,
        root_data_type,
        chunk_number_group=chunk_number_group,
        check_is_stored=False,
    )
    if not st.is_stored(run_id, data_type):
        raise ValueError(f"Data type {data_type} not stored after merging the per-chunk storage.")


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--context", required=True)
    parser.add_argument("--xedocs_version", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--stage", action="store_true", dest="stage")
    parser.add_argument("--keep_raw_records", action="store_true", dest="keep_raw_records")
    parser.add_argument("--chunks", required=True, nargs="*", type=int)

    args = parser.parse_args()

    run_id = f"{args.run_id:06d}"
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
        ignore_processed=True,
        stage=args.stage,
    )

    # Check what data is in the output folder
    data_types = sorted(
        set(
            [
                d.split("-")[1]
                for d in os.listdir(input_path)
                if os.path.isdir(os.path.join(input_path, d))
            ]
        )
    )

    _chunks = [0] + args.chunks
    chunk_number_group = [list(range(_chunks[i], _chunks[i + 1])) for i in range(len(args.chunks))]

    # Merge
    for data_type in data_types:
        logger.info(f"Merging {data_type}")
        merge(st, run_id, data_type, chunk_number_group)

    # Remove rucio directory
    if not args.keep_raw_records:
        shutil.rmtree(staging_dir)

    if not args.rucio_upload:
        logger.warning("Ignoring rucio upload")
        return

    # Now loop over data_type we just made and upload the data
    processed_data = os.listdir(output_path)
    logger.info(f"Combined data: {processed_data}")

    for dirname in processed_data:
        path = os.path.join(output_path, dirname)
        if os.path.isdir(path):
            upload_to_rucio(st, path, args.rundb_update)


if __name__ == "__main__":
    main()
