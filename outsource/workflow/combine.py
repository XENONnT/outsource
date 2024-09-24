import argparse
import os
import admix
from utilix.config import setup_logger
import strax
import straxen
import cutax

from outsource.config import get_bottom_data_types
from outsource.upload import upload_to_rucio


logger = setup_logger("outsource")
straxen.Events.save_when = strax.SaveWhen.TARGET
admix.clients._init_clients()


def merge(st, run_id, data_type, path, chunk_number_group):
    """Merge per-chunk storage for a given data_type.

    :param st: straxen context
    :param run_id: run number padded with 0s
    :param data_type: data_type 'level' e.g. records, peaklets
    :param path: path where the data is stored
    :param chunk_number_group: list of chunk number to merge.

    """
    # Initialize plugin needed for processing
    plugin = st._plugin_class_registry[data_type]()

    to_merge = set(d.split("-")[1] for d in os.listdir(path))

    # Rechunk the data if we can
    for _data_type in plugin.provides:
        if _data_type not in to_merge:
            continue
        bottoms = get_bottom_data_types(data_type)
        assert len(bottoms) == 1
        st.merge_per_chunk_storage(
            run_id,
            _data_type,
            bottoms[0],
            chunk_number_group=chunk_number_group,
        )


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--context")
    parser.add_argument("--xedocs_version")
    parser.add_argument("--input_path")
    parser.add_argument("--output_path")
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--chunks", nargs="*", type=str)

    args = parser.parse_args()

    run_id = f"{args.run_id:06d}"
    input_path = args.input_path
    output_path = args.output_path

    # Get context
    st = getattr(cutax.contexts, args.context)(xedocs_version=args.xedocs_version)
    st.storage = [
        strax.DataDirectory(input_path, readonly=True),
        strax.DataDirectory(output_path),  # where we are copying data to
    ]

    # Check what data is in the output folder
    data_types = [d.split("-")[1] for d in os.listdir(input_path)]

    if any([d in data_types for d in ["lone_hits", "pulse_counts", "veto_regions"]]):
        plugin_levels = ["records", "peaklets"]
    elif "hitlets_nv" in data_types:
        plugin_levels = ["hitlets_nv"]
    elif "afterpulses" in data_types:
        plugin_levels = ["afterpulses"]
    elif "led_calibration" in data_types:
        plugin_levels = ["led_calibration"]
    else:
        plugin_levels = ["peaklets"]

    chunk_number_group = [[int(i) for i in c.split()] for c in args.chunks]

    # Merge
    for data_type in plugin_levels:
        logger.info(f"Merging {data_type} level")
        merge(st, run_id, data_type, input_path, chunk_number_group)

    # Now upload the merged metadata
    # Setup the rucio client(s)
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
