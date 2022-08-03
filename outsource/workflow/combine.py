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

from admix.clients import rucio_client

admix.clients._init_clients()

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

    plugin.default_chunk_size_mb = 500

    to_merge = [d.split('-')[1] for d in os.listdir(path)]

    for keystring in plugin.provides:
        if keystring not in to_merge:
            continue
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
        if keystring not in to_merge:
            continue
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
    st = getattr(cutax.contexts, args.context)()
    st.storage = [strax.DataDirectory('./'),
                  strax.DataDirectory(final_path) # where we are copying data to
                  ]

    # check what data is in the output folder
    dtypes = [d.split('-')[1] for d in os.listdir(path)]

    if any([d in dtypes for d in ['lone_hits', 'pulse_counts', 'veto_regions']]):
        plugin_levels = ['records', 'peaklets']
    elif 'hitlets_nv' in dtypes:
        plugin_levels = ['hitlets_nv']
    elif 'afterpulses' in dtypes:
        plugin_levels = ['afterpulses']
    elif 'led_calibration' in dtypes:
        plugin_levels = ['led_calibration']
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

        this_path = os.path.join(final_path, this_dir)
        admix.upload(this_path, rse=rse, did=dataset_did, update_db=args.update_db)


if __name__ == "__main__":
    main()
