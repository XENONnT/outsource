#!/usr/bin/env python

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

from admix.interfaces.rucio_summoner import RucioSummoner
from admix.utils.naming import make_did


def main():
    parser = argparse.ArgumentParser(description="Upload combined output to rucio")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to upload')
    parser.add_argument('rse', help='Target RSE')
    parser.add_argument('--context', help='Strax context')

    args = parser.parse_args()

    tmp_path = tempfile.mkdtemp()


    runid = args.dataset
    runid_str = "%06d" % runid
    dtype = args.dtype
    rse = args.rse

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory(tmp_path)]

    plugin = st._get_plugins((dtype,), runid_str)[dtype]

    rc = RucioSummoner()

    for keystring in plugin.provides:
        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        hash = key.lineage_hash
        # TODO check with utilix DB call that the hashes match?

        dirname = f"{runid_str}-{keystring}-{hash}"
        upload_path = os.path.join('combined', dirname)


        print(f"Uploading {dirname}")
        os.listdir(upload_path)

        # make a rucio DID
        did = make_did(runid, keystring, hash)

        # check if a rule already exists for this DID
        rucio_rule = rc.GetRule(upload_structure=did)

        # if not in rucio already and no rule exists, upload into rucio
        if not rucio_rule['exists']:
            result = rc.Upload(did,
                               upload_path,
                               rse,
                               lifetime=None)

            # check that upload was successful
            new_rule = rc.GetRule(upload_structure=did, rse=rse)

            # TODO check number of files

            new_data_dict={}
            new_data_dict['location'] = rse
            new_data_dict['did'] = did
            new_data_dict['status'] = "transferred"
            new_data_dict['host'] = "rucio-catalogue"
            new_data_dict['type'] = keystring
            new_data_dict['lifetime'] = new_rule['expires'],
            new_data_dict['protocol'] = 'rucio'
            new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
            new_data_dict['checksum'] = 'shit'
            db.update_data(runid, new_data_dict)
        else:
            print(f"Rucio rule already exists for {did}")


if __name__ == "__main__":
    main()
