import os
import time
import datetime
import numpy as np
import rucio
from utilix import DB, uconfig
import admix


admix.clients._init_clients()
db = DB()


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


def get_rse(this_dtype):
    # Based on the dtype and the utilix config, where should this data go?
    if this_dtype in ["records", "pulse_counts", "veto_regions", "records_nv", "records_he"]:
        rse = uconfig.get("Outsource", "records_rse")
    elif this_dtype in ["peaklets", "lone_hits", "merged_s2s", "hitlets_nv"]:
        rse = uconfig.get("Outsource", "peaklets_rse")
    else:
        rse = uconfig.get("Outsource", "events_rse")
    return rse


def attach_rucio(path):
    # Get rucio dataset
    files = os.listdir(path)
    dirname = os.path.basename(path)

    # If there are no files, we can't upload them
    if len(files) == 0:
        print(f"No files to upload in {dirname}. Skipping.")
        return

    this_run, this_dtype, this_hash = dirname.split("-")
    dataset_did = admix.utils.make_did(int(this_run), this_dtype, this_hash)

    scope, dset_name = dataset_did.split(":")

    # Get list of files that have already been uploaded
    # this is to allow us re-run workflow for some chunks
    try:
        existing_files = [f for f in admix.clients.rucio_client.list_dids(scope, {"type": "file"})]
        existing_files = [f for f in existing_files if dset_name in f]

        existing_files_in_dataset = admix.rucio.list_files(dataset_did)

        # For some reason files get uploaded but not attached correctly
        need_attached = list(set(existing_files) - set(existing_files_in_dataset))

        if len(need_attached) > 0:
            dids_to_attach = [scope + ":" + name for name in need_attached]
            print("---------")
            print("Need to attach the following in rucio:")
            print(dids_to_attach)
            print("---------")

            admix.rucio.attach(dataset_did, dids_to_attach)
    except rucio.common.exception.DataIdentifierNotFound:
        pass


def upload_to_rucio(path, update_db=False):
    succeded_rucio_upload = False

    # Get rucio dataset
    files = os.listdir(path)
    dirname = os.path.basename(path)

    # If there are no files, we can't upload them
    if len(files) == 0:
        print(f"No files to upload in {dirname}. Skipping.")
        return succeded_rucio_upload

    this_run, this_dtype, this_hash = dirname.split("-")
    rse = get_rse(this_dtype)
    dataset_did = admix.utils.make_did(int(this_run), this_dtype, this_hash)

    scope, dset_name = dataset_did.split(":")

    try:
        print("--------------------------")
        print(f"Pre-uploading {path} to rucio!")
        t0 = time.time()
        admix.preupload(path, rse=rse, did=dataset_did)
        preupload_time = time.time() - t0
        print(f"=== Preuploading time for {this_dtype}: {preupload_time / 60:0.2f} minutes === ")

        print("--------------------------")
        print(f"Uploading {path} to rucio!")
        t0 = time.time()
        admix.upload(path, rse=rse, did=dataset_did, update_db=update_db)
        upload_time = time.time() - t0
        succeded_rucio_upload = True
        print(f"=== Uploading time for {this_dtype}: {upload_time / 60:0.2f} minutes === ")
    except Exception:
        print(f"Upload of {dset_name} failed for some reason")
        raise

    # TODO: check rucio that the files are there?
    print(f"Upload of {len(files)} files in {dirname} finished successfully")
    return succeded_rucio_upload


def update_db(st, path):
    # Get rucio dataset
    files = os.listdir(path)

    dirname = os.path.basename(path)
    this_run, this_dtype, this_hash = dirname.split("-")
    rse = get_rse(this_dtype)
    dataset_did = admix.utils.make_did(int(this_run), this_dtype, this_hash)

    md = st.get_metadata(this_run, this_dtype)
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

    db.update_data(int(this_run), new_data_dict)
    print(f"Database updated for {this_dtype} at {rse}")
