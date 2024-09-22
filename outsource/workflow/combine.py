#!/usr/bin/env python
import argparse
import os
import admix
import strax
import straxen
import cutax

from .upload import get_bottom_data_types, upload_to_rucio

straxen.Events.save_when = strax.SaveWhen.TARGET
print("We have forced events to save always.")

admix.clients._init_clients()


def merge(
    run_id_str,  # run number padded with 0s
    data_type,  # data_type 'level' e.g. records, peaklets
    st,  # strax context
    path,  # path where the data is stored
    chunk_number_group,  # list of chunk number to merge
):
    # Initialize plugin needed for processing
    plugin = st._plugin_class_registry[data_type]()

    to_merge = [d.split("-")[1] for d in os.listdir(path)]

    # Rechunk the data if we can
    for data_type in plugin.provides:
        if data_type not in to_merge:
            continue
        bottoms = get_bottom_data_types(data_type)
        assert len(bottoms) == 1
        st.merge_per_chunk_storage(
            run_id_str,
            data_type,
            bottoms[0],
            chunk_number_group=chunk_number_group,
        )


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument("run_id", help="Run number", type=int)
    parser.add_argument("--context", help="name of context")
    parser.add_argument("--xedocs_version", help="xedocs global version")
    parser.add_argument("--path", help="path where the temp directory is")
    parser.add_argument("--rucio_upload", action="store_true", dest="rucio_upload")
    parser.add_argument("--rundb_update", action="store_true", dest="rundb_update")
    parser.add_argument("--chunks", nargs="*", help="chunk numbers to combine", type=str)

    args = parser.parse_args()

    run_id = args.run_id
    run_id_str = f"{run_id:06d}"
    path = args.path

    data_dir = "finished_data"

    # Get context
    st = getattr(cutax.contexts, args.context)(xedocs_version=args.xedocs_version)
    st.storage = [
        strax.DataDirectory("./data", readonly=True),
        strax.DataDirectory(data_dir),  # where we are copying data to
    ]

    # Check what data is in the output folder
    data_types = [d.split("-")[1] for d in os.listdir(path)]

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
        print(f"Merging {data_type} level")
        merge(run_id_str, data_type, st, path, chunk_number_group)

    # Now upload the merged metadata
    # Setup the rucio client(s)
    if not args.rucio_upload:
        print("Ignoring rucio upload")
        return

    # Now loop over data_type we just made and upload the data
    processed_data = [d for d in os.listdir(data_dir)]
    print("---- Processed data ----")
    for d in processed_data:
        print(d)
    print("------------------------\n")

    for dirname in processed_data:
        path = os.path.join(data_dir, dirname)

        upload_to_rucio(path, args.rundb_update)


if __name__ == "__main__":
    main()
