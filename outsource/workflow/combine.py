import argparse
import os
import shutil
import admix
from utilix import uconfig
from utilix.config import setup_logger
import strax
import straxen
import cutax

from outsource.utils import per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource")
straxen.Events.save_when = strax.SaveWhen.TARGET
admix.clients._init_clients()


def merge(st, run_id, data_type, path, chunk_number_group):
    """Merge per-chunk storage for a given data_type.

    :param st: straxen context
    :param run_id: run number padded with 0s
    :param data_type: data_type to be merged
    :param chunk_number_group: list of chunk number to merge.

    """
    root_data_type = per_chunk_storage_root_data_type(st, run_id, data_type)
    assert root_data_type
    st.merge_per_chunk_storage(
        run_id,
        data_type,
        root_data_type,
        chunk_number_group=chunk_number_group,
        check_is_stored=False,
    )
    rucio_remote_frontend = st.storage.pop(2)
    assert st.is_stored(run_id, data_type)
    st.storage.append(rucio_remote_frontend)


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--context", required=True)
    parser.add_argument("--xedocs_version", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--chunks", required=True, nargs="*", type=int)

    args = parser.parse_args()

    run_id = f"{args.run_id:06d}"
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
        strax.DataDirectory(output_path),  # where we are copying data to
        straxen.storage.RucioRemoteFrontend(
            staging_dir=staging_dir,
            download_heavy=True,
            take_only=tuple(st.root_data_types),
            rses_only=uconfig.getlist("Outsource", "raw_records_rse"),
        ),
        straxen.storage.RucioRemoteFrontend(
            staging_dir=staging_dir,
            download_heavy=True,
            exclude=tuple(st.root_data_types),
        ),
    ]

    # Check what data is in the output folder
    data_types = [d.split("-")[1] for d in os.listdir(input_path)]

    _chunks = [0] + args.chunks
    chunk_number_group = [list(range(_chunks[i], _chunks[i + 1])) for i in range(len(args.chunks))]

    # Merge
    for data_type in data_types:
        logger.info(f"Merging {data_type} level")
        merge(st, run_id, data_type, chunk_number_group)

    # Remove rucio directory
    shutil.rmtree(staging_dir)

    if not args.rucio_upload:
        logger.warning("Ignoring rucio upload")
        return

    # Now loop over data_type we just made and upload the data
    processed_data = os.listdir(output_path)
    logger.info(f"Combined data: {processed_data}")

    for dirname in processed_data:
        upload_to_rucio(os.path.join(output_path, dirname), args.rundb_update)


if __name__ == "__main__":
    main()
