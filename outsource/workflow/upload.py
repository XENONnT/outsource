#!/usr/bin/env python

import sys
import os
import argparse
import datetime
import tempfile
# make sure we don't use any custom paths from e.g. pth files
import sys
for p in list(sys.path):
    if os.environ.get('HOME', ' 0123456789 ') in p:
        sys.path.remove(p)

import strax
import straxen
from utilix import db
import numpy as np
import time

from admix.interfaces.rucio_summoner import RucioSummoner
from admix.utils.naming import make_did


def main():
    parser = argparse.ArgumentParser(description="Upload combined output to rucio")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to upload')
    parser.add_argument('rse', help='Target RSE')
    parser.add_argument('--context', help='Strax context')
    parser.add_argument('--ignore-db', help='flag to not update runsDB', dest='ignore_rundb',
                        action='store_true')

    args = parser.parse_args()

    runid = args.dataset
    runid_str = "%06d" % runid
    dtype = args.dtype
    rse = args.rse

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory('data')]

    plugin = st._get_plugins((dtype,), runid_str)[dtype]

    rc = RucioSummoner()

    for keystring in plugin.provides:
        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        hash = key.lineage_hash
        # TODO check with utilix DB call that the hashes match?

        dirname = f"{runid_str}-{keystring}-{hash}"
        upload_path = os.path.join('data', dirname)

        nfiles = len(os.listdir(upload_path))
        print(f"Uploading {dirname}, which has {nfiles} files")

        # make a rucio DID
        did = make_did(runid, keystring, hash)

        # check if a rule already exists for this DID
        rucio_rule = rc.GetRule(upload_structure=did)

        file_count = len(os.listdir(upload_path))
        # TODO check number of files is consistent with what we expect

        md = st.get_meta(runid_str, keystring)

        chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
        data_size_mb = int(np.sum(chunk_mb))
        avg_data_size_mb = int(np.average(chunk_mb))
        lineage_hash = md['lineage_hash']

        new_data_dict = dict()
        new_data_dict['location'] = rse
        new_data_dict['did'] = did
        new_data_dict['status'] = "uploading"
        new_data_dict['host'] = "rucio-catalogue"
        new_data_dict['type'] = keystring
        new_data_dict['protocol'] = 'rucio'
        new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
        new_data_dict['creation_place'] = "OSG"
        new_data_dict['file_count'] = file_count
        new_data_dict['meta'] = dict(lineage=lineage_hash,
                                     avg_chunk_mb=avg_data_size_mb,
                                     file_count=file_count,
                                     size_mb=data_size_mb,
                                     strax_version=strax.__version__,
                                     straxen_version=straxen.__version__
                                     )

        # if not in rucio already and no rule exists, upload into rucio
        if not rucio_rule['exists']:
            t0 = time.time()
            if not args.ignore_rundb:
                db.update_data(runid, new_data_dict)

            result = rc.Upload(did,
                               upload_path,
                               rse,
                               lifetime=None)

            tf = time.time()
            upload_time = (tf - t0)/60
            print(f"=== Upload of {did} took {upload_time} minutes")
            # check that upload was successful
            new_rule = rc.GetRule(upload_structure=did, rse=rse)

            if new_rule['state'] == 'OK' and not args.ignore_rundb:
                new_data_dict['status'] = 'transferred'
                db.update_data(runid, new_data_dict)
        else:
            print(f"Rucio rule already exists for {did}")
            sys.exit(1)


if __name__ == "__main__":
    main()
