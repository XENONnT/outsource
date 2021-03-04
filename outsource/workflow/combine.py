#!/usr/bin/env python
# import logging
# logging.basicConfig(level='DEBUG')
import argparse
import os
import numpy as np
import strax
import straxen
import datetime
from admix.utils.naming import make_did
from admix.interfaces.rucio_summoner import RucioSummoner
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from utilix import db
from pprint import pprint


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to combine')
    parser.add_argument('--context', help='Strax context')
    parser.add_argument('--input', help='path where the temp directory is')
    parser.add_argument('--rse', help='RSE to upload to')
    parser.add_argument('--ignore-db', help='flag to not update runsDB', dest='ignore_rundb',
                        action='store_true')
    parser.add_argument('--ignore-rucio', help='flag to not upload to rucio', dest='ignore_rucio',
                        action='store_true')

    args = parser.parse_args()

    runid = args.dataset
    runid_str = "%06d" % runid
    dtype = args.dtype
    path = args.input

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory('./')]

    # initialize plugin needed for processing
    plugin = st._get_plugins((dtype,), runid_str)[dtype]
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    for keystring in plugin.provides:
        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        saver = st.storage[0].saver(key, plugin.metadata(runid_str, keystring))
        # monkey patch the saver
        tmpname = os.path.split(saver.tempdirname)[1]
        dirname = os.path.split(saver.dirname)[1]
        saver.tempdirname = os.path.join(path, tmpname)
        saver.dirname = os.path.join(path, dirname)
        saver.is_forked = True
        # merge the jsons
        saver.close()

    # now upload the merged metadata
    # setup the rucio client(s)
    if args.ignore_rucio:
        print("Ignoring rucio upload. Exiting")
        return

    # need to patch the storage again
    st.storage = [strax.DataDirectory('data')]

    updonkey = UploadClient()
    donkey = Client()

    hash = strax.DataKey(runid_str, dtype, plugin.lineage).lineage_hash

    for this_dir in os.listdir(path):
        # prepare list of dicts to be uploaded
        keystring = this_dir.split('-')[1]
        dataset_did = make_did(runid, keystring, hash)
        scope, dset_name = dataset_did.split(':')

        files = os.listdir(os.path.join(path, this_dir))
        to_upload = []
        for f in files:
            this_path = os.path.join(path, this_dir, f)
            #print(f"Uploading {os.path.basename(path)}")
            d = dict(path=this_path,
                     did_scope=scope,
                     did_name=f,
                     dataset_scope=scope,
                     dataset_name=dset_name,
                     rse=args.rse,
                     register_after_upload=True
                     )
            to_upload.append(d)

        print(to_upload)
        # now do the upload!
        try:
            updonkey.upload(to_upload)
        except:
            print(f'Upload of {keystring} failed')
        print(f"Upload of {len(files)} files in {this_dir} finished successfully")
        for f in files:
            print(f"{scope}:{f}")

        print()


        # now check the rucio data matche what we expect
        rucio_files = [f for f in donkey.list_files(scope, dset_name)]

        # how many chunks?
        md = st.get_meta(runid_str, keystring)

        expected_chunks = len([c for c in md['chunks'] if c['n']>0])


        # we should have n+1 files in rucio (counting metadata)
        if len(rucio_files) != expected_chunks + 1:
            raise RuntimeError(f"File mismatch! There are {len(rucio_files)} but the metadata thinks there "
                               f"should be {expected_chunks} chunks + 1 metadata")


        chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
        data_size_mb = int(np.sum(chunk_mb))
        avg_data_size_mb = int(np.average(chunk_mb))

        # let's do one last check of the rule
        rc = RucioSummoner()
        rule = rc.GetRule(dataset_did, args.rse)
        if rule['state'] == 'OK':
            status = 'transferred'
        elif rule['state'] == 'REPLICATING':
            status = 'transferring'
        else:
            status='error'

        if not args.ignore_rundb:
            # update runDB
            new_data_dict = dict()
            new_data_dict['location'] = args.rse
            new_data_dict['did'] = dataset_did
            new_data_dict['status'] = status
            new_data_dict['host'] = "rucio-catalogue"
            new_data_dict['type'] = keystring
            new_data_dict['protocol'] = 'rucio'
            new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
            new_data_dict['creation_place'] = "OSG"
            #new_data_dict['file_count'] = file_count
            new_data_dict['meta'] = dict(#lineage=plugin.lineage_hash,
                                         avg_chunk_mb=avg_data_size_mb,
                                         file_count=len(rucio_files),
                                         size_mb=data_size_mb,
                                         strax_version=strax.__version__,
                                         straxen_version=straxen.__version__
                                         )

            pprint(new_data_dict)
            #db.update_data(runid, new_data_dict)
            #print(f"Database updated for {keystring}")
        else:
            print("Skipping database update.")


        # if everything is good, let's close the dataset
        # this will make it so no more data can be added to this dataset
        if status == 'transferred':
            donkey.close(scope, dset_name)


if __name__ == "__main__":
    main()
