#!/usr/bin/env python
import argparse
import os
import admix
import strax
import straxen
import cutax

from .upload import get_bottom_dtypes, upload_to_rucio

straxen.Events.save_when = strax.SaveWhen.TARGET
print("We have forced events to save always.")

admix.clients._init_clients()


def merge(
    run_id_str,  # run number padded with 0s
    dtype,  # data type 'level' e.g. records, peaklets
    st,  # strax context
    path,  # path where the data is stored
    chunk_number_group,  # list of chunk number to merge
):
    # Initialize plugin needed for processing
    plugin = st._plugin_class_registry[dtype]()

    to_merge = [d.split("-")[1] for d in os.listdir(path)]

    # Rechunk the data if we can
    for dtype in plugin.provides:
        if dtype not in to_merge:
            continue
        bottoms = get_bottom_dtypes(dtype)
        assert len(bottoms) == 1
        st.merge_per_chunk_storage(
            run_id_str,
            dtype,
            bottoms[0],
            chunk_number_group=chunk_number_group,
        )


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument("run_id", help="Run number", type=int)
    parser.add_argument("--context", help="name of context")
    parser.add_argument("--xedocs_version", help="xedocs global version")
    parser.add_argument("--path", help="path where the temp directory is")
    parser.add_argument("--upload-to-rucio", action="store_true", dest="upload_to_rucio")
    parser.add_argument("--update-db", action="store_true", dest="update_db")
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
    dtypes = [d.split("-")[1] for d in os.listdir(path)]

    if any([d in dtypes for d in ["lone_hits", "pulse_counts", "veto_regions"]]):
        plugin_levels = ["records", "peaklets"]
    elif "hitlets_nv" in dtypes:
        plugin_levels = ["hitlets_nv"]
    elif "afterpulses" in dtypes:
        plugin_levels = ["afterpulses"]
    elif "led_calibration" in dtypes:
        plugin_levels = ["led_calibration"]
    else:
        plugin_levels = ["peaklets"]

    chunk_number_group = [[int(i) for i in c.split()] for c in args.chunks]

    # Merge
    for dtype in plugin_levels:
        print(f"Merging {dtype} level")
        merge(run_id_str, dtype, st, path, chunk_number_group)

    # Now upload the merged metadata
    # Setup the rucio client(s)
    if not args.upload_to_rucio:
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

        upload_to_rucio(path, args.update_db)


if __name__ == "__main__":
    main()
