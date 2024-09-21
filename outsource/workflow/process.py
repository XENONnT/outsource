#!/usr/bin/env python3
import argparse
import os
import shutil
import sys
import tempfile
import glob
import gc
import json
import time
from shutil import rmtree, copyfile
from ast import literal_eval
import datetime

from utilix import DB, uconfig
import admix
import rucio
import numpy as np
import strax
import straxen
import cutax

straxen.Events.save_when = strax.SaveWhen.TARGET

admix.clients._init_clients()
db = DB()

# These dtypes we need to rechunk, so don't upload to rucio here!
RECHUNK_DTYPES = [
    "pulse_counts",
    "veto_regions",
    "peaklets",
    "lone_hits",
    "hitlets_nv",
    "afterpulses",
    "led_calibration",
]

# These dtypes will not be uploaded to rucio, and will be removed after processing
IGNORE_DTYPES = [
    "records",
    "records_nv",
    "lone_raw_records_nv",
    "raw_records_coin_nv",
    "lone_raw_record_statistics_nv",
    "records_he",
    "records_mv",
    "peaks",
    "peaklets",  # added to avoid duplicating upload/staging
    "lone_hites",  # added to avoid duplicating upload/staging
]

# These dtypes should always be made at the same time:
BUDDY_DTYPES = [
    ("veto_regions_nv", "event_positions_nv"),
    (
        "event_info_double",
        "event_pattern_fit",
        "event_area_per_channel",
        "event_top_bottom_params",
        "event_ms_naive",
        "peak_s1_positions_cnn",
        "event_ambience",
        "event_shadow",
        "cuts_basic",
    ),
    ("event_shadow", "event_ambience"),
    ("events_nv", "ref_mon_nv"),
]

# These are the dtypes we want to make first if any of them is in to-process list
PRIORITY_RANK = [
    "peaklet_classification",
    "merged_s2s",
    "peaks",
    "peak_basics",
    "peak_positions_mlp",
    "peak_positions_gcn",
    "peak_positions_cnn",
    "peak_positions",
    "peak_proximity",
    "events",
    "event_basics",
]


def get_bottom_dtypes(dtype):
    """Get the lowest level dependencies for a given dtype."""
    if dtype in ["hitlets_nv", "events_nv", "veto_regions_nv", "ref_mon_nv"]:
        return ("raw_records_nv",)
    elif dtype in ["peak_basics_he"]:
        return ("raw_records_he",)
    elif dtype in ["records", "peaklets"]:
        return ("raw_records",)
    elif dtype == "veto_regions_mv":
        return ("raw_records_mv",)
    else:
        return ("peaklets", "lone_hits")


def find_data_to_download(st, run_id, target):
    run_id_str = f"{run_id:06d}"
    bottoms = get_bottom_dtypes(target)
    bottom_hashes = tuple([st.key_for(run_id_str, b).lineage_hash for b in bottoms])

    to_download = []

    # All data entries from the RunDB for certain run_id
    data = db.get_data(run_id, host="rucio-catalogue")

    def find_data(_target):
        """Recursively find all the data needed to make the target dtype.

        This function will consult RunDB to know where the data you want
        to download is. Returns a list of tuples (dtype, hash) that need
        to be downloaded.
        """
        # Check if we have the data already
        if all([(d, h) in to_download for d, h in zip(bottoms, bottom_hashes)]):
            return

        # Initialize plugin needed for processing
        _plugin = st._get_plugins((_target,), run_id_str)[_target]
        st._set_plugin_config(_plugin, run_id_str, tolerant=False)

        # Download all the required data_type to produce this output file
        for in_dtype in _plugin.depends_on:
            # Get hash for this dtype
            hash = st.key_for(run_id_str, in_dtype).lineage_hash
            rses = [d["location"] for d in data if (d["type"] == in_dtype and hash in d["did"])]
            # For checking if local path exists
            # Here st.storage[0] is the local storage like ./data
            local_path = os.path.join(st.storage[0].path, f"{run_id:06d}-{in_dtype}-{hash}")

            if len(rses) == 0 and not os.path.exists(local_path):
                # Need to download data to make ths one
                find_data(in_dtype)
            else:
                info = (in_dtype, hash)
                if info not in to_download:
                    to_download.append(info)

    # Fills the to_download list
    find_data(target)
    return to_download


def process(run_id, out_dtype, st, chunks, close_savers=False, tmp_path=".tmp_for_strax"):
    run_id_str = f"{run_id:06d}"
    t0 = time.time()

    # st.make(
    #     run_id_str,
    #     out_dtype,
    #     chunk_number={"raw_records": chunks},
    #     processor="single_thread",
    # )

    # Initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), run_id_str)[out_dtype]
    st._set_plugin_config(plugin, run_id_str, tolerant=False)
    plugin.setup()

    # Now move on to processing
    # If we didn't pass any chunks, we process the whole thing --
    # otherwise just do the chunks we listed
    if chunks is None:
        # Check if we need to save anything， if not, skip this plugin
        if plugin.save_when[out_dtype] == strax.SaveWhen.NEVER:
            print("This plugin is not saving anything. Skipping.")
            return

        print("Chunks is none -- processing whole thing!")
        # Then we just process the whole thing
        for keystring in plugin.provides:
            print(f"Making {keystring}")
            # We want to be more tolerant on cuts_basic, because sometimes it is ill-defined
            if keystring == "cuts_basic":
                try:
                    st.make(run_id_str, keystring, save=keystring, processor="single_thread")
                except Exception as e:
                    print(
                        f"Failed to make {keystring}, but it might be "
                        "due to that the cuts are not ready yet. Skipping"
                    )
                    print("Below is the error:")
                    print(e)
            else:
                st.make(run_id_str, keystring, save=keystring, processor="single_thread")
            print(f"DONE processing {keystring}")

            # Test if the data is complete
            try:
                print(
                    "Try loading data in %s to see if it is complete."
                    % (run_id_str + "-" + keystring)
                )
                st.get_array(run_id_str, keystring, keep_columns="time", progress_bar=False)
                print("Successfully loaded %s! It is complete." % (run_id_str + "-" + keystring))
            except Exception as e:
                print(f"Data is not complete for {run_id_str + '-' + keystring}. Skipping")
                print("Below is the error message we get when trying to load the data:")
                print(e)
            print("--------------------------")

    # Process chunk-by-chunk
    else:
        # Setup savers
        savers = dict()
        for keystring in plugin.provides:
            print(f"Making {keystring}")
            key = strax.DataKey(run_id_str, keystring, plugin.lineage)
            saver = st.storage[0].saver(key, plugin.metadata(run_id, keystring))
            saver.is_forked = True
            savers[keystring] = saver

        # Setup a few more variables
        in_dtype = plugin.depends_on[0]
        input_metadata = st.get_metadata(run_id_str, in_dtype)
        input_key = strax.DataKey(run_id_str, in_dtype, input_metadata["lineage"])
        backend = None
        backend_key = None

        for sf in st.storage:
            try:
                backend_name, backend_key = sf.find(input_key)
                backend = sf._get_backend(backend_name)
            except strax.DataNotAvailable:
                pass

        if backend is None:
            raise strax.DataNotAvailable("We could not find data for ", input_key)

        dtype = literal_eval(input_metadata["dtype"])
        chunk_kwargs = dict(
            data_type=input_metadata["data_type"],
            data_kind=input_metadata["data_kind"],
            dtype=dtype,
        )

        for chunk in chunks:
            # Read in the input data for this chunk
            chunk_info = None
            for chunk_md in input_metadata["chunks"]:
                if chunk_md["chunk_i"] == int(chunk):
                    chunk_info = chunk_md
                    break
            assert chunk_info is not None, f"Could not find chunk_id: {chunk}"
            in_data = backend._read_and_format_chunk(
                backend_key=backend_key,
                metadata=input_metadata,
                chunk_info=chunk_info,
                dtype=dtype,
                time_range=None,
                chunk_construction_kwargs=chunk_kwargs,
            )
            # Process this chunk
            output_data = plugin.do_compute(chunk_i=chunk, **{in_dtype: in_data})

            if isinstance(output_data, dict):
                # Save the output -- you have to loop because there could be > 1 output dtypes
                for keystring, strax_chunk in output_data.items():
                    savers[keystring].save(strax_chunk, chunk_i=int(chunk))
            elif isinstance(output_data, strax.Chunk):
                # Save the output -- you have to loop because there could be > 1 output dtypes
                savers[keystring].save(output_data, chunk_i=int(chunk))
            else:
                raise TypeError("Unknown datatype %s for output" % (type(output_data)))

        if close_savers:
            for dtype, saver in savers.items():
                # Copy the metadata to a tmp directory
                tmpdir = os.path.join(tmp_path, os.path.split(saver.tempdirname)[1])
                os.makedirs(tmpdir, exist_ok=True)
                for file in os.listdir(saver.tempdirname):
                    if file.endswith("json"):
                        src = os.path.join(saver.tempdirname, file)
                        dest = os.path.join(tmpdir, file)
                        copyfile(src, dest)
                saver.close()
    process_time = time.time() - t0
    print(f"=== Processing time for {out_dtype}: {process_time / 60:0.2f} minutes === ")


def check_chunk_n(directory):
    """Check that the chunk length and number of chunk is agreed with promise
    in metadata."""
    if directory[-1] != "/":
        directory += "/"
    files = sorted(glob.glob(directory + "*"))
    n_chunks = len(files) - 1
    metadata = json.loads(open(files[-1], "r").read())

    if n_chunks != 0:
        n_metadata_chunks = len(metadata["chunks"])
        # Check that the number of chunks in storage
        # is less than or equal to the number of chunks in metadata
        assert n_chunks == n_metadata_chunks or n_chunks == n_metadata_chunks - 1, (
            "For directory %s, \
            there are %s chunks in storage, \
            but metadata says %s. Chunks in storage must be \
            less than chunks in metadata!"
            % (directory, n_chunks, n_metadata_chunks)
        )

        compressor = metadata["compressor"]
        dtype = eval(metadata["dtype"])

        # Check that the chunk length is agreed with promise in metadata
        for i in range(n_chunks):
            chunk = strax.load_file(files[i], compressor=compressor, dtype=dtype)
            if metadata["chunks"][i]["n"] != len(chunk):
                raise strax.DataCorrupted(
                    f"Chunk {files[i]} of {metadata['run_id']} has {len(chunk)} items, "
                    f"but metadata says {metadata['chunks'][i]['n']}"
                )

        # Check that the last chunk is empty
        if n_chunks == n_metadata_chunks - 1:
            assert (
                metadata["chunks"][n_chunks]["n"] == 0
            ), "Empty chunk has non-zero length in metadata!"

    else:
        # Check that the number of chunks in metadata is 1
        assert (
            len(metadata["chunks"]) == 1
        ), "There are %s chunks in storage, but metadata says %s" % (
            n_chunks,
            len(metadata["chunks"]),
        )
        assert metadata["chunks"][0]["n"] == 0, "Empty chunk has non-zero length in metadata!"


def main():
    parser = argparse.ArgumentParser(description="(Re)Processing With Outsource")
    parser.add_argument("dataset", help="Run number", type=int)
    parser.add_argument("--context", help="name of context")
    parser.add_argument("--xedocs_version", help="xedocs global version")
    parser.add_argument("--output", help="desired strax(en) output")
    parser.add_argument("--chunks", nargs="*", help="chunk ids to download", type=int)
    parser.add_argument("--upload-to-rucio", action="store_true", dest="upload_to_rucio")
    parser.add_argument("--update-db", action="store_true", dest="update_db")
    parser.add_argument("--download-only", action="store_true", dest="download_only")
    parser.add_argument("--no-download", action="store_true", dest="no_download")

    args = parser.parse_args()

    # Directory where we will be putting everything
    data_dir = "./data"

    # Make sure this is empty
    # if os.path.exists(data_dir):
    #     rmtree(data_dir)

    # Get context
    st = getattr(cutax.contexts, args.context)(xedocs_version=args.xedocs_version)
    # st.storage = [
    #     strax.DataDirectory(data_dir),
    #     straxen.rucio.RucioFrontend(
    #         include_remote=True, download_heavy=True, staging_dir=os.path.join(data_dir, "rucio")
    #     ),
    # ]
    st.storage = [
        strax.DataDirectory(data_dir),
        straxen.storage.RucioRemoteFrontend(download_heavy=True),
    ]

    # Add local frontend if we can
    # This is a temporary hack
    try:
        st.storage.append(straxen.storage.RucioLocalFrontend())
    except KeyError:
        print("No local RSE found")

    print("Context is set up!")

    run_id = args.dataset
    run_id_str = f"{run_id:06d}"
    out_dtype = args.output  # eg. ypically for tpc: peaklets/event_info

    print("Getting to-download list...")
    to_download = find_data_to_download(st, run_id, out_dtype)
    print("Got to-download list!")

    # See if we have rucio local frontend
    # If we do, it's probably more efficient to download data through the rucio frontend

    for buddies in BUDDY_DTYPES:
        if out_dtype in buddies:
            for other_dtype in buddies:
                if other_dtype == out_dtype:
                    continue
                to_download.extend(find_data_to_download(st, run_id, other_dtype))
    # Remove duplicates
    to_download = list(set(to_download))

    # Initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), run_id_str)[out_dtype]
    st._set_plugin_config(plugin, run_id_str, tolerant=False)
    plugin.setup()

    # Figure out what plugins we need to process/initialize
    to_process = [args.output]
    for buddies in BUDDY_DTYPES:
        if args.output in buddies:
            to_process = list(buddies)
    # Remove duplicates
    to_process = list(set(to_process))

    # Keep track of the data we can download now -- will be important for the upload step later
    available_dtypes = st.available_for_run(run_id_str)
    available_dtypes = available_dtypes[available_dtypes.is_stored].target.values.tolist()

    missing = set(plugin.depends_on) - set(available_dtypes)
    intermediates = missing.copy()
    to_process = list(intermediates) + to_process

    # Now we need to figure out what intermediate data we need to make
    while len(intermediates) > 0:
        new_intermediates = []
        for _dtype in intermediates:
            _plugin = st._get_plugins((_dtype,), run_id_str)[_dtype]
            # Adding missing dependencies to to-process list
            for dependency in _plugin.depends_on:
                if dependency not in available_dtypes:
                    if dependency not in to_process:
                        to_process = [dependency] + to_process
                    new_intermediates.append(dependency)
        intermediates = new_intermediates

    # Remove any raw data
    to_process = [dtype for dtype in to_process if dtype not in admix.utils.RAW_DTYPES]

    missing = [d for d in to_process if d != args.output]
    missing_str = ", ".join(missing)
    print(f"Need to create intermediate data: {missing_str}")

    print("--Available data--")
    for dd in available_dtypes:
        print(dd)
    print("-------------------\n")

    if args.download_only:
        sys.exit(0)

    # If to-process has anything in PRIORITY_RANK, we process them first
    if len(set(PRIORITY_RANK) & set(to_process)) > 0:
        # Remove any prioritized dtypes that are not in to_process
        filtered_priority_rank = [dtype for dtype in PRIORITY_RANK if dtype in to_process]
        # Remove the PRIORITY_RANK dtypes from to_process, as low priority data_type which we don't
        # rigorously care their order
        to_process_low_priority = [dt for dt in to_process if dt not in filtered_priority_rank]
        # Sort the priority by their dependencies
        to_process = filtered_priority_rank + to_process_low_priority

    print(f"To process: {', '.join(to_process)}")
    _tmp_path = tempfile.mkdtemp()
    for dtype in to_process:
        close_savers = dtype != args.output
        process(run_id, dtype, st, args.chunks, close_savers=close_savers, tmp_path=_tmp_path)
        gc.collect()

    print("Done processing. Now check if we should upload to rucio")

    # Now we move the tmpfiles back to main directory, if needed
    # this is for cases where we went from raw_records-->records-->peaklets in one go
    if os.path.exists(_tmp_path):
        for dtype_path_thing in os.listdir(_tmp_path):
            tmp_path = os.path.join(_tmp_path, dtype_path_thing)
            merged_dir = os.path.join(data_dir, dtype_path_thing.split("_temp")[0])

            for file in os.listdir(tmp_path):
                copyfile(os.path.join(tmp_path, file), os.path.join(merged_dir, file))

            os.rename(merged_dir, os.path.join(data_dir, dtype_path_thing))

    # If we processed the entire run, we upload everything including metadata
    # Otherwise, we just upload the chunks
    upload_meta = args.chunks is None

    # Remove rucio directory
    rmtree(st.storage[1]._get_backend("RucioRemoteBackend").staging_dir)

    # Now loop over data_type we just made and upload the data
    processed_data = [d for d in os.listdir(data_dir) if "_temp" not in d]
    print("---- Processed data ----")
    for d in processed_data:
        print(d)
    print("------------------------\n")

    for dirname in processed_data:
        # Get rucio dataset
        this_run, this_dtype, this_hash = dirname.split("-")

        # Remove data we do not want to upload
        if this_dtype in IGNORE_DTYPES:
            print(f"Removing {this_dtype} instead of uploading")
            shutil.rmtree(os.path.join(data_dir, dirname))
            continue

        if not args.upload_to_rucio:
            print("Ignoring rucio upload")
            continue

        # Based on the dtype and the utilix config, where should this data go?
        if this_dtype in ["records", "pulse_counts", "veto_regions", "records_nv", "records_he"]:
            rse = uconfig.get("Outsource", "records_rse")
        elif this_dtype in ["peaklets", "lone_hits", "merged_s2s", "hitlets_nv"]:
            rse = uconfig.get("Outsource", "peaklets_rse")
        else:
            rse = uconfig.get("Outsource", "events_rse")

        if this_dtype in RECHUNK_DTYPES:
            print(f"Skipping upload of {this_dtype} since we need to rechunk it")
            continue

        # Remove the _temp if we are processing chunks in parallel
        if args.chunks is not None:
            this_hash = this_hash.replace("_temp", "")
        dataset_did = admix.utils.make_did(int(this_run), this_dtype, this_hash)

        scope, dset_name = dataset_did.split(":")

        files = [f for f in os.listdir(os.path.join(data_dir, dirname))]

        if not upload_meta:
            files = [f for f in files if not f.endswith(".json")]

            # Check that the output number of files is what we expect
            if len(files) != len(args.chunks):
                processed_chunks = set([int(f.split("-")[-1]) for f in files])
                expected_chunks = set(args.chunks)
                missing_chunks = expected_chunks - processed_chunks
                missing_chunks = " ".join(missing_chunks)
                raise RuntimeError(
                    "File mismatch! We are missing output data for the following chunks: "
                    f"{missing_chunks}"
                )

        # If there are no files, we can't upload them
        if len(files) == 0:
            print(f"No files to upload in {dirname}. Skipping.")
            continue

        # Get list of files that have already been uploaded
        # this is to allow us re-run workflow for some chunks
        try:
            existing_files = [
                f for f in admix.clients.rucio_client.list_dids(scope, {"type": "file"})
            ]
            existing_files = [f for f in existing_files if dset_name in f]

            existing_files_in_dataset = admix.rucio.list_files(dataset_did)

            # For some reason files get uploaded but not attached correctly
            need_attached = list(set(existing_files) - set(existing_files_in_dataset))

            # Only consider the chunks here
            if args.chunks:
                need_attached = [
                    f for f in need_attached if str(int(f.split("-")[-1])) in args.chunks
                ]

            if len(need_attached) > 0:
                dids_to_attach = [scope + ":" + name for name in need_attached]
                print("---------")
                print("Need to attach the following in rucio:")
                print(dids_to_attach)
                print("---------")

                admix.rucio.attach(dataset_did, dids_to_attach)

        except rucio.common.exception.DataIdentifierNotFound:
            pass

        path = os.path.join(data_dir, dirname)

        print("--------------------------")
        print(f"Checking if chunk length is agreed with promise in metadata for {path}")
        check_chunk_n(path)
        print("The chunk length is agreed with promise in metadata.")

        succeded_rucio_upload = False
        try:
            print("--------------------------")
            print(f"Pre-uploading {path} to rucio!")
            t0 = time.time()
            admix.preupload(path, rse=rse, did=dataset_did)
            preupload_time = time.time() - t0
            print(
                f"=== Preuploading time for {this_dtype}: {preupload_time / 60:0.2f} minutes === "
            )

            print("--------------------------")
            print(f"Uploading {path} to rucio!")
            t0 = time.time()
            admix.upload(path, rse=rse, did=dataset_did)
            upload_time = time.time() - t0
            succeded_rucio_upload = True
            print(f"=== Uploading time for {this_dtype}: {upload_time / 60:0.2f} minutes === ")
        except Exception:
            print(f"Upload of {dset_name} failed for some reason")
            raise

        # TODO: check rucio that the files are there?
        print(f"Upload of {len(files)} files in {dirname} finished successfully")

        # If we processed the whole thing, add a rule at DALI update the RunDB here
        if args.chunks is None:
            # Skip if update_db flag is false, or if the rucio upload failed
            if args.update_db and succeded_rucio_upload:
                md = st.get_meta(run_id_str, this_dtype)
                chunk_mb = [chunk["nbytes"] / (1e6) for chunk in md["chunks"]]
                data_size_mb = np.sum(chunk_mb)

                # Update RunDB
                new_data_dict = dict()
                new_data_dict["location"] = rse
                new_data_dict["did"] = dataset_did
                new_data_dict["status"] = "transferred"
                new_data_dict["host"] = "rucio-catalogue"
                new_data_dict["type"] = this_dtype
                new_data_dict["protocol"] = "rucio"
                new_data_dict["creation_time"] = datetime.datetime.utcnow().isoformat()
                new_data_dict["creation_place"] = "OSG"
                new_data_dict["meta"] = dict(
                    lineage_hash=md.get("lineage_hash"),
                    file_count=len(files),
                    size_mb=data_size_mb,
                )

                db.update_data(run_id, new_data_dict)
                print(f"Database updated for {this_dtype} at {rse}")

        # Cleanup the files we uploaded
        # This is likely only done for records data because we will rechunk the others
        for f in files:
            print(f"Removing {f}")
            os.remove(os.path.join(data_dir, dirname, f))

    print("ALL DONE!")


if __name__ == "__main__":
    main()
