import argparse
import os
from utilix import uconfig
from utilix.config import setup_logger
import admix
import straxen

from outsource.utils import get_context, per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
admix.clients._init_clients()

if not straxen.HAVE_ADMIX:
    raise ImportError("straxen must be installed with admix to use this script")
if not admix.manager.HAVE_GFAL2:
    raise ImportError("admix must be installed with gfal2 to use this script")


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
    parser.add_argument("--staging_dir", required=True)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--stage", action="store_true", dest="stage")
    parser.add_argument("--remove_heavy", action="store_true", dest="remove_heavy")
    parser.add_argument("--chunks", required=True, nargs="*", type=int)

    args = parser.parse_args()

    run_id = f"{args.run_id:06d}"
    input_path = args.input_path
    output_path = args.output_path
    staging_dir = args.staging_dir

    # Get context
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
        remove_heavy=args.remove_heavy,
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
        if st.is_stored(run_id, data_type):
            # Logic kept for slurm jobs
            logger.info(f"Data type {data_type} already stored.")
            continue
        merge(st, run_id, data_type, chunk_number_group)

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
