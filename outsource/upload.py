import os
import time
from datetime import datetime
from utilix import DB, uconfig
from utilix.config import setup_logger
import admix

from outsource.utils import get_rse


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
admix.clients._init_clients()
db = DB()


def upload_to_rucio(st, path, update_db=False):
    # Get rucio dataset
    files = os.listdir(path)
    dirname = os.path.basename(path)

    # If there are no files, we can't upload them
    if len(files) == 0:
        logger.warning(f"No files to upload in {dirname}. Skipping.")

    this_run, this_data_type, this_hash = dirname.split("-")
    rse = get_rse(st, this_data_type)
    dataset_did = admix.utils.make_did(int(this_run), this_data_type, this_hash)

    scope, dset_name = dataset_did.split(":")

    logger.info(f"Pre-uploading {path} to rucio!")
    t0 = time.time()
    admix.preupload(path, rse=rse, did=dataset_did)
    preupload_time = time.time() - t0
    logger.warning(f"Preuploading time for {this_data_type}: {preupload_time / 60:0.2f} minutes")

    logger.info(f"Uploading {path} to rucio!")
    t0 = time.time()
    miscellaneous = {
        "creation_time": datetime.utcnow().isoformat(),
        "creation_place": "OSG",
    }
    admix.upload(path, rse=rse, did=dataset_did, update_db=update_db, miscellaneous=miscellaneous)
    upload_time = time.time() - t0
    logger.warning(f"Uploading time for {this_data_type}: {upload_time / 60:0.2f} minutes")

    # TODO: check rucio that the files are there
    logger.info(f"Upload of {len(files)} files in {dirname} finished successfully")
