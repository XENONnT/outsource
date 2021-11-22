#!/usr/bin/env python
import argparse
import os
import shutil
import numpy as np
import strax
import straxen
import datetime
import admix
import rucio
from utilix import DB, uconfig
from immutabledict import immutabledict
import cutax

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

    plugin.chunk_target_size_mb = 500

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
    st = getattr(cutax.contexts, args.context)(_include_rucio_remote=True)
    st.storage = [strax.DataDirectory('./'),
                  strax.DataDirectory(final_path) # where we are copying data to
                  ]

    # check what data is in the output folder
    dtypes = [d.split('-')[1] for d in os.listdir(path)]

    if 'records' in dtypes:
        plugin_levels = ['records', 'peaklets']
    elif 'hitlets_nv' in dtypes:
        plugin_levels = ['hitlets_nv']
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

    for this_dir in os.listdir(final_path):
        # prepare list of dicts to be uploaded
        _run, keystring, straxhash = this_dir.split('-')
        dataset_did = admix.utils.make_did(runid, keystring, straxhash)
        scope, dset_name = dataset_did.split(':')

        # based on the dtype and the utilix config, where should this data go?
        if keystring in ['records', 'pulse_counts', 'veto_regions']:
            rse = uconfig.get('Outsource', 'records_rse')
        elif keystring in ['peaklets', 'lone_hits']:
            rse = uconfig.get('Outsource', 'peaklets_rse')
        else:
            rse = uconfig.get('Outsource', 'events_rse')


        existing_files = [f for f in admix.rucio.rucio_client.list_dids(scope, {'type': 'file'}, type='file')]
        existing_files = [f for f in existing_files if dset_name in f]

        try:
            existing_files_in_dataset = admix.rucio.list_files(dataset_did)
        except rucio.common.exception.DataIdentifierNotFound:
            existing_files_in_dataset = []

        # for some reason files get uploaded but not attached correctly
        need_attached = list(set(existing_files) - set(existing_files_in_dataset))

        if len(need_attached) > 0:
            dids_to_attach = [f"{scope}:{name}" for name in need_attached]
            admix.rucio.attach(dataset_did, dids_to_attach)

        this_path = os.path.join(final_path, this_dir)
        admix.upload(this_path, rse=rse, did=dataset_did)

        # now check the rucio data matche what we expect
        rucio_files = admix.rucio.list_files(dataset_did)

        # make sure the rule status is okay
        rules = admix.rucio.list_rules(dataset_did, state='OK', rse_expression=rse)
        assert len(rules) > 0, f"Error uploading {dataset_did}"

        # how many chunks?
        md = st.get_meta(runid_str, keystring)

        expected_chunks = len([c for c in md['chunks'] if c['n']>0])

        # we should have n+1 files in rucio (counting metadata)
        if len(rucio_files) != expected_chunks + 1:
            # we're missing some data, uh oh
            successful_chunks = set([int(f.split('-')[-1]) for f in rucio_files])
            expected_chunks = set(np.arange(expected_chunks))

            missing_chunks = expected_chunks - successful_chunks

            missing_chunk_str = '/n'.join(missing_chunks)
            raise RuntimeError(f"File mismatch in {this_dir}! "
                               f"There are {len(rucio_files)} but the metadata thinks there "
                               f"should be {expected_chunks} chunks + 1 metadata. "
                               f"The missing chunks are:\n{missing_chunk_str}")

        chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
        data_size_mb = np.sum(chunk_mb)
        avg_data_size_mb = np.mean(chunk_mb)

        if args.update_db:
            # update runDB
            new_data_dict = dict()
            new_data_dict['location'] = rse
            new_data_dict['did'] = dataset_did
            new_data_dict['status'] = 'transferred'
            new_data_dict['host'] = "rucio-catalogue"
            new_data_dict['type'] = keystring
            new_data_dict['protocol'] = 'rucio'
            new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
            new_data_dict['creation_place'] = "OSG"
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
        try:
            admix.rucio.rucio_client.close(scope, dset_name)
        except:
            print(f"Closing {scope}:{dset_name} failed")


if __name__ == "__main__":
    main()
