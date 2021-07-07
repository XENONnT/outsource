#!/usr/bin/env python
# import logging
# logging.basicConfig(level='DEBUG')
import argparse
import os
import shutil
import numpy as np
import strax
import straxen
import datetime
from admix.utils.naming import make_did
from admix.interfaces.rucio_summoner import RucioSummoner
import rucio
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from utilix import DB, uconfig
from immutabledict import immutabledict

db = DB()

def get_hashes(st):
    return {dt: item['hash'] for dt, item in st.provided_dtypes().items()}

def merge(runid_str, # run number padded with 0s
          dtype,     # data type 'level' e.g. records, peaklets
          st,        # strax context
          path       # path where the data is stored
          ):

    # get the storage path, since will need to reset later
    _storage_paths = [storage.path for storage in st.storage]

    # initialize plugin needed for processing
    plugin = st._get_plugins((dtype,), runid_str)[dtype]
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    plugin.chunk_target_size_mb = 1000

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

    # change the storage frontend to use the merged data
    st.storage[0] = strax.DataDirectory(path)

    # rechunk the data if we can
    for keystring in plugin.provides:
        rechunk = True
        if isinstance(plugin.rechunk_on_save, immutabledict):
            if not plugin.rechunk_on_save[keystring]:
                rechunk = False
        else:
            if not plugin.rechunk_on_save:
                rechunk = False

        if rechunk:
            print(f"Rechunking {keystring}")
            st.copy_to_frontend(runid_str, keystring, 1, rechunk=True)
        else:
            print(f"Not rechunking {keystring}. Just copy to the staging directory.")
            key = st.key_for(runid_str, keystring)
            src = os.path.join(st.storage[0].path, str(key))
            dest = os.path.join(st.storage[1].path, str(key))
            shutil.copytree(src, dest)

    # reset in case we need to merge more data
    st.storage = [strax.DataDirectory(path) for path in _storage_paths]


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to combine')
    parser.add_argument('--context', help='Strax context')
    parser.add_argument('--input', help='path where the temp directory is')
    parser.add_argument('--cmt', help='CMT global version')
    parser.add_argument('--update-db', help='flag to update runsDB', dest='update_db',
                        action='store_true')
    parser.add_argument('--upload-to-rucio', help='flag to upload to rucio', dest='upload_to_rucio',
                        action='store_true')

    args = parser.parse_args()

    runid = args.dataset
    runid_str = "%06d" % runid
    path = args.input

    final_path = 'finished_data'

    # get context
    st = getattr(straxen.contexts, args.context)(args.cmt)
    st.storage = [strax.DataDirectory('./'),
                  strax.DataDirectory(final_path) # where we are copying data to
                  ]

    # check what data is in the output folder
    dtypes = [d.split('-')[1] for d in os.listdir(path)]

    if 'records' in dtypes:
        plugin_levels = ['records', 'peaklets']
    else:
        plugin_levels = ['peaklets']

    # merge
    for dtype in plugin_levels:
        print(f"Merging {dtype} level")
        merge(runid_str, dtype, st, path)


    print(f"Current contents of {final_path}:")
    print(os.listdir(final_path))

    # now upload the merged metadata
    # setup the rucio client(s)
    if not args.upload_to_rucio:
        print("Ignoring rucio upload. Exiting")
        return

    # need to patch the storage one last time
    st.storage = [strax.DataDirectory(final_path)]

    updonkey = UploadClient()
    donkey = Client()

    for this_dir in os.listdir(final_path):
        # prepare list of dicts to be uploaded
        _run, keystring, straxhash = this_dir.split('-')
        dataset_did = make_did(runid, keystring, straxhash)
        scope, dset_name = dataset_did.split(':')

        # based on the dtype and the utilix config, where should this data go?
        if keystring in ['records', 'pulse_counts', 'veto_regions']:
            rse = uconfig.get('Outsource', 'records_rse')
        elif keystring in ['peaklets', 'lone_hits']:
            rse = uconfig.get('Outsource', 'peaklets_rse')
        else:
            rse = uconfig.get('Outsource', 'events_rse')

        files = os.listdir(os.path.join(final_path, this_dir))
        to_upload = []

        existing_files = [f for f in donkey.list_dids(scope, {'type': 'file'}, type='file')]
        existing_files = [f for f in existing_files if dset_name in f]

        try:
            existing_files_in_dataset = [f['name'] for f in donkey.list_files(scope, dset_name)]
        except rucio.common.exception.DataIdentifierNotFound:
            existing_files_in_dataset = []


        # for some reason files get uploaded but not attached correctly
        need_attached = list(set(existing_files) - set(existing_files_in_dataset))

        if len(need_attached) > 0:
            dids_to_attach = [dict(scope=scope, name=name) for name in need_attached]

            donkey.attach_dids(scope, dset_name, dids_to_attach)

        for f in files:
            if f in existing_files:
                print(f"Skipping {f} since it is already uploaded")
                continue

            this_path = os.path.join(final_path, this_dir, f)
            d = dict(path=this_path,
                     did_scope=scope,
                     did_name=f,
                     dataset_scope=scope,
                     dataset_name=dset_name,
                     rse=rse
                     )
            to_upload.append(d)

        # now do the upload!
        if len(to_upload) == 0:
            print(f"No files to upload for {this_dir}")
            continue

        # now do the upload!
        try:
            updonkey.upload(to_upload)
        except:
            print(f'Upload of {keystring} failed')
            raise
        print(f"Upload of {len(files)} files in {this_dir} finished successfully")
        for f in files:
            print(f"{scope}:{f}")

        # now check the rucio data matche what we expect
        rucio_files = [f for f in donkey.list_files(scope, dset_name)]

        # how many chunks?
        md = st.get_meta(runid_str, keystring)

        expected_chunks = len([c for c in md['chunks'] if c['n']>0])

        # we should have n+1 files in rucio (counting metadata)
        if len(rucio_files) != expected_chunks + 1:
            # we're missing some data, uh oh
            successful_chunks = set([int(f['name'].split('-')[-1]) for f in rucio_files])
            expected_chunks = set(np.arange(expected_chunks))

            missing_chunks = expected_chunks - successful_chunks

            missing_chunk_str = '/n'.join(missing_chunks)
            raise RuntimeError(f"File mismatch! There are {len(rucio_files)} but the metadata thinks there "
                               f"should be {expected_chunks} chunks + 1 metadata. "
                               f"The missing chunks are:\n{missing_chunk_str}")

        chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
        data_size_mb = np.sum(chunk_mb)
        avg_data_size_mb = np.mean(chunk_mb)

        # let's do one last check of the rule
        rc = RucioSummoner()

        rses = [rse]
        # if (keystring not in ['records', 'veto_regions', 'pulse_counts']
        #         and "UC_DALI_USERDISK" not in rses):
        #     rses.append('UC_DALI_USERDISK')


        for rse in rses:
            rule = rc.GetRule(dataset_did, rse)
            if rule['state'] == 'OK':
                status = 'transferred'
            elif rule['state'] == 'REPLICATING':
                status = 'transferring'
            else:
                status = 'error'

            if args.update_db:
                # update runDB
                new_data_dict = dict()
                new_data_dict['location'] = rse
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

                db.update_data(runid, new_data_dict)


                print(f"Database updated for {keystring} at {rse}")
            else:
                print("Skipping database update.")


        # if everything is good, let's close the dataset
        # this will make it so no more data can be added to this dataset
        if status == 'transferred':
            try:
                donkey.close(scope, dset_name)
            except:
                print(f"Closing {scope}:{dset_name} failed")


if __name__ == "__main__":
    main()
