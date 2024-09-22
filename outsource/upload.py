import os
import time
from utilix import DB
from utilix.config import setup_logger
import admix

from outsource.config import get_rse


logger = setup_logger("outsource")
admix.clients._init_clients()
db = DB()


def upload_to_rucio(path, update_db=False):
    # Get rucio dataset
    files = os.listdir(path)
    dirname = os.path.basename(path)

    # If there are no files, we can't upload them
    if len(files) == 0:
        logger.warning(f"No files to upload in {dirname}. Skipping.")

    this_run, this_data_type, this_hash = dirname.split("-")
    rse = get_rse(this_data_type)
    dataset_did = admix.utils.make_did(int(this_run), this_data_type, this_hash)

    scope, dset_name = dataset_did.split(":")

    try:
        logger.info(f"Pre-uploading {path} to rucio!")
        t0 = time.time()
        admix.preupload(path, rse=rse, did=dataset_did)
        preupload_time = time.time() - t0
        logger.warning(
            f"Preuploading time for {this_data_type}: {preupload_time / 60:0.2f} minutes"
        )

        logger.info(f"Uploading {path} to rucio!")
        t0 = time.time()
        admix.upload(path, rse=rse, did=dataset_did, update_db=update_db)
        upload_time = time.time() - t0
        logger.warning(f"Uploading time for {this_data_type}: {upload_time / 60:0.2f} minutes")
    except Exception:
        logger.warning(f"Upload of {dset_name} failed for some reason")
        raise

    # TODO: check rucio that the files are there
    logger.info(f"Upload of {len(files)} files in {dirname} finished successfully")
