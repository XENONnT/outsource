import argparse
import os
import time
import shutil
import gc
from utilix import uconfig
from utilix.config import setup_logger
import strax
import straxen
import cutax

from outsource.utils import per_chunk_storage_root_data_type
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource")
straxen.Events.save_when = strax.SaveWhen.TARGET


def process(st, run_id, data_type, chunks):
    t0 = time.time()

    if chunks:
        # find the root data_type
        root_data_type = per_chunk_storage_root_data_type(st, run_id, data_type)
        assert root_data_type
        st.make(
            run_id,
            data_type,
            chunk_number={root_data_type: chunks},
            processor="single_thread",
        )
    else:
        assert not per_chunk_storage_root_data_type(st, run_id, data_type)
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

    logger.info("Context is set up!")

    run_id = f"{args.run_id:06d}"
    data_type = args.data_type  # eg. typically for tpc: peaklets/event_info

    if args.download_only:
        st.get_array(run_id, data_type, chunk_number={data_type: args.chunks})
        return

    tree_levels = st.tree_levels
    to_process = sorted(
        st.get_components(run_id, data_type).savers.keys(),
        key=lambda x: tree_levels[x],
        reverse=True,
    )

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
