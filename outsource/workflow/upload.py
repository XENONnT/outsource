import os
import time
from utilix import DB, uconfig
import admix


admix.clients._init_clients()
db = DB()


def get_bottom_data_types(data_type):
    """Get the lowest level dependencies for a given data_type."""
    if data_type in ["hitlets_nv", "events_nv", "veto_regions_nv", "ref_mon_nv"]:
        return ("raw_records_nv",)
    elif data_type in ["peak_basics_he"]:
        return ("raw_records_he",)
    elif data_type in ["records", "peaklets"]:
        return ("raw_records",)
    elif data_type == "veto_regions_mv":
        return ("raw_records_mv",)
    else:
        return ("peaklets", "lone_hits")


def get_rse(this_data_type):
    # Based on the data_type and the utilix config, where should this data go?
    if this_data_type in ["records", "pulse_counts", "veto_regions", "records_nv", "records_he"]:
        rse = uconfig.get("Outsource", "records_rse")
    elif this_data_type in ["peaklets", "lone_hits", "merged_s2s", "hitlets_nv"]:
        rse = uconfig.get("Outsource", "peaklets_rse")
    else:
        rse = uconfig.get("Outsource", "events_rse")
    return rse


def upload_to_rucio(path, update_db=False):
    # Get rucio dataset
    files = os.listdir(path)
    dirname = os.path.basename(path)

    # If there are no files, we can't upload them
    if len(files) == 0:
        print(f"No files to upload in {dirname}. Skipping.")

    this_run, this_data_type, this_hash = dirname.split("-")
    rse = get_rse(this_data_type)
    dataset_did = admix.utils.make_did(int(this_run), this_data_type, this_hash)

    scope, dset_name = dataset_did.split(":")

    try:
        print("--------------------------")
        print(f"Pre-uploading {path} to rucio!")
        t0 = time.time()
        admix.preupload(path, rse=rse, did=dataset_did)
        preupload_time = time.time() - t0
        print(
            f"=== Preuploading time for {this_data_type}: {preupload_time / 60:0.2f} minutes === "
        )

        print("--------------------------")
        print(f"Uploading {path} to rucio!")
        t0 = time.time()
        admix.upload(path, rse=rse, did=dataset_did, update_db=update_db)
        upload_time = time.time() - t0
        print(f"=== Uploading time for {this_data_type}: {upload_time / 60:0.2f} minutes === ")
    except Exception:
        print(f"Upload of {dset_name} failed for some reason")
        raise

    # TODO: check rucio that the files are there
    print(f"Upload of {len(files)} files in {dirname} finished successfully")
