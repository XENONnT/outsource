#!/usr/bin/env python3
import argparse
import os
import sys
import numpy as np
import strax
import straxen
import time
from pprint import pprint
from shutil import rmtree
from ast import literal_eval
from utilix import db
import admix
from admix.utils.naming import make_did
import rucio
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
import datetime
from pprint import pprint

# these dtypes we need to rechunk, so don't upload to rucio here!
rechunk_dtypes = ['pulse_counts',
                  'veto_regions',
                  'peaklets',
                  'lone_hits'
                  ]

def apply_global_version(context, cmt_version):
    context.set_config(dict(gain_model=('CMT_model', ("to_pe_model", cmt_version))))
    context.set_config(dict(s2_xy_correction_map=("CMT_model", ('s2_xy_map', cmt_version), True)))
    context.set_config(dict(elife_conf=("elife", cmt_version, True)))
    context.set_config(dict(mlp_model=("CMT_model", ("mlp_model", cmt_version), True)))
    context.set_config(dict(gcn_model=("CMT_model", ("gcn_model", cmt_version), True)))
    context.set_config(dict(cnn_model=("CMT_model", ("cnn_model", cmt_version), True)))


def get_hashes(st):
    hashes = set([(d, st.key_for('0', d).lineage_hash)
                  for p in st._plugin_class_registry.values()
                  for d in p.provides
                  ]
                 )
    return {dtype: h for dtype, h in hashes}


def find_data_to_download(runid, target, st):
    runid_str = str(runid)

    hashes = get_hashes(st)
    bottom = "peaklets"

    if bottom not in hashes:
        raise ValueError(f"The dtype {bottom} is not in this context!")

    bottom_hash = hashes[bottom]
    print(bottom, bottom_hash)

    to_download = []

    def find_data(_target):
        if any([d[0] == bottom and d[1] == bottom_hash for d in to_download]):
            return

        # initialize plugin needed for processing
        _plugin = st._get_plugins((_target,), runid_str)[_target]
        st._set_plugin_config(_plugin, runid_str, tolerant=False)
        _plugin.setup()

        # download all the required datatypes to produce this output file
        for in_dtype in _plugin.depends_on:
            # get hash for this dtype
            hash = hashes.get(in_dtype)
            print(_target, in_dtype, hash, bottom, bottom_hash)
            rses = db.get_rses(int(runid_str), in_dtype, hash)
            print(rses)
            # print(in_dtype, rses)
            if len(rses) == 0:
                # need to download data to make ths one
                find_data(in_dtype)
            else:
                info = (in_dtype, hash)
                if info not in to_download:
                    to_download.append(info)
            if in_dtype == bottom and hash == bottom_hash:
                print(f"Reached {bottom}. Returning.")
                return

    # fills the to_download list
    find_data(target)
    return to_download


def main():

    parser = argparse.ArgumentParser(description="Strax Processing With Outsource")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('--output', help='desired strax(en) output')
    parser.add_argument('--context', help='name of context')
    parser.add_argument('--chunks', nargs='*', help='chunk ids to download')
    parser.add_argument('--rse', type=str, default="UC_OSG_USERDISK")
    parser.add_argument('--cmt', type=str, default='ONLINE')

    args = parser.parse_args()

    # directory where we will be putting everything
    data_dir = './data'

    # make sure this is empty
    # if os.path.exists(data_dir):
    #     rmtree(data_dir)

    # get context
    st = getattr(straxen.contexts, args.context)()
    st.storage = [strax.DataDirectory(data_dir)]

    apply_global_version(st, args.cmt)

    hashes = get_hashes(st)

    runid = args.dataset
    runid_str = "%06d" % runid
    out_dtype = args.output

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
    
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    t0 = time.time()
    # download all the required datatypes to produce this output file
    if args.chunks:
        for in_dtype in plugin.depends_on:
            # get hash for this dtype
            hash = hashes[in_dtype]
            # download the input data
            if not os.path.exists(os.path.join(data_dir, f"{runid:06d}-{in_dtype}-{hash}")):
                admix.download(runid, in_dtype, hash, chunks=args.chunks, location=data_dir)
    else:
        # download all the data we need
        to_download = find_data_to_download(runid, out_dtype, st)
        for in_dtype, hash in to_download:
            if not os.path.exists(os.path.join(data_dir, f"{runid:06d}-{in_dtype}-{hash}")):
                admix.download(runid, in_dtype, hash, location=data_dir)

    download_time = time.time() - t0 # seconds
    print(f"=== Download time (minutes): {download_time/60}")

    t0 = time.time()

    # keep track of the data we just downloaded -- will be important for the upload step later
    downloaded_data = os.listdir(data_dir)
    print("--Downloaded data--")
    for dd in downloaded_data:
        print(dd)
    print("-------------------\n")
    # now move on to processing
    # if we didn't pass any chunks, we process the whole thing -- otherwise just do the chunks we listed
    if args.chunks is None:
        print("Chunks is none -- processing whole thing!")
        # then we just process the whole thing
        for keystring in plugin.provides:
            print(f"Making {keystring}")
            st.make(runid_str, keystring,
                    max_workers=8,
                    allow_multiple=True,
                    )
            print(f"DONE processing {keystring}")

    # process chunk-by-chunk
    else:
        # setup savers
        savers = dict()
        for keystring in plugin.provides:
            print(f"Making {keystring}")
            key = strax.DataKey(runid_str, keystring, plugin.lineage)
            saver = st.storage[0].saver(key, plugin.metadata(runid, keystring))
            saver.is_forked = True
            savers[keystring] = saver

        # setup a few more variables
        # TODO not sure exactly how this works when an output plugin depends on >1 plugin
        # maybe that doesn't matter?
        in_dtype = plugin.depends_on[0]
        input_metadata = st.get_metadata(runid_str, in_dtype)
        input_key = strax.DataKey(runid_str, in_dtype, input_metadata['lineage'])
        backend = st.storage[0].backends[0]
        dtype = literal_eval(input_metadata['dtype'])
        chunk_kwargs = dict(data_type=input_metadata['data_type'],
                            data_kind=input_metadata['data_kind'],
                            dtype=dtype)

        for chunk in args.chunks:
            # read in the input data for this chunk
            in_data = backend._read_and_format_chunk(backend_key=st.storage[0].find(input_key)[1],
                                                     metadata=input_metadata,
                                                     chunk_info=input_metadata['chunks'][int(chunk)],
                                                     dtype=dtype,
                                                     time_range=None,
                                                     chunk_construction_kwargs=chunk_kwargs
                                                    )
            # process this chunk
            output_data = plugin.do_compute(chunk_i=chunk, **{in_dtype: in_data})

            # save the output -- you have to loop because there could be > 1 output dtypes
            for keystring, strax_chunk in output_data.items():
                savers[keystring].save(strax_chunk, chunk_i=int(chunk))
    process_time = time.time() - t0
    print(f"=== Processing time (minutes): {process_time/60} === ")
    print("Done processing. Now check if we should upload to rucio")

    # initiate the rucio client
    upload_client = UploadClient()
    rucio_client = Client()

    # if we processed the entire run, we upload everything including metadata
    # otherwise, we just upload the chunks
    upload_meta = args.chunks is None

    # now loop over datatypes we just made and upload the data
    processed_data = [d for d in os.listdir(data_dir) if d not in downloaded_data]
    print("---- Processed data ----")
    for d in processed_data:
        print(d)
    print("------------------------\n")

    for dirname in processed_data:
        # get rucio dataset
        this_run, this_dtype, this_hash = dirname.split('-')
        if this_dtype in rechunk_dtypes:
            print(f"Skipping upload of {this_dtype} since we need to rechunk it")
            continue

        # remove the _temp if we are processing chunks in parallel
        if args.chunks is not None:
            this_hash = this_hash.replace('_temp', '')
        dataset = make_did(int(this_run), this_dtype, this_hash)

        scope, dset_name = dataset.split(':')

        files = [f for f in os.listdir(os.path.join(data_dir, dirname))]

        if not upload_meta:
            files = [f for f in files if not f.endswith('.json')]

            # check that the output number of files is what we expect
            if len(files) != len(args.chunks):
                processed_chunks = set([int(f.split('-')[-1]) for f in files])
                expected_chunks = set(args.chunks)
                missing_chunks = expected_chunks - processed_chunks
                missing_chunks = ' '.join(missing_chunks)
                raise RuntimeError("File mismatch! We are missing output data for the following chunks: "
                                   f"{missing_chunks}"
                                   )


        # if there are no files, we can't upload them
        if len(files) == 0:
            print(f"No files to upload in {dirname}. Skipping.")
            continue

        # get list of files that have already been uploaded
        # this is to allow us re-run workflow for some chunks
        try:
            existing_files = [f for f in rucio_client.list_dids(scope,
                                                                       {'type': 'file'},
                                                                        type='file')
                              ]
            existing_files = [f for f in existing_files if dset_name in f]

            existing_files_in_dataset = [f['name'] for f in rucio_client.list_files(scope, dset_name)]

            # for some reason files get uploaded but not attached correctly
            need_attached = list(set(existing_files) - set(existing_files_in_dataset))

            # only consider the chunks here
            need_attached = [f for f in need_attached if str(int(f.split('-')[-1])) in args.chunks]


            if len(need_attached) > 0:
                dids_to_attach = [dict(scope=scope, name=name) for name in need_attached]

                rucio_client.attach_dids(scope, dset_name, dids_to_attach)


        except rucio.common.exception.DataIdentifierNotFound:
            existing_files = []

        # prepare list of dicts to be uploaded
        to_upload = []


        for f in files:
            path = os.path.join(data_dir, dirname, f)
            if f in existing_files:
                print(f"Skipping {f} since it is already uploaded")
                continue

            print(f"Uploading {f}")
            d = dict(path=path,
                     did_scope=scope,
                     did_name=f,
                     dataset_scope=scope,
                     dataset_name=dset_name,
                     rse=args.rse,
                     register_after_upload=True
                     )
            to_upload.append(d)

        # skip upload for now

        # now do the upload!
        if len(to_upload) == 0:
            print(f"No files to upload for {dirname}")
            continue
        try:
            upload_client.upload(to_upload)
        except:
            print(f"Upload of {dset_name} failed for some reason")
            raise

        # TODO check rucio that the files are there?
        print(f"Upload of {len(files)} files in {dirname} finished successfully")

        # if we processed the whole thing, update the runDB here
        if args.chunks is None:
            md = st.get_meta(runid_str, this_dtype)
            chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
            data_size_mb = np.sum(chunk_mb)
            avg_data_size_mb = np.mean(chunk_mb)

            # update runDB
            new_data_dict = dict()
            new_data_dict['location'] = args.rse
            new_data_dict['did'] = dataset
            new_data_dict['status'] = 'transferred'
            new_data_dict['host'] = "rucio-catalogue"
            new_data_dict['type'] = this_dtype
            new_data_dict['protocol'] = 'rucio'
            new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
            new_data_dict['creation_place'] = "OSG"
            new_data_dict['meta'] = dict(lineage=plugin.lineage,
                                         avg_chunk_mb=avg_data_size_mb,
                                         file_count=len(files),
                                         size_mb=data_size_mb,
                                         strax_version=strax.__version__,
                                         straxen_version=straxen.__version__
                                         )

            db.update_data(runid, new_data_dict)
            print(f"Database updated for {this_dtype}")

        # cleanup the files we uploaded
        # this is likely only done for records data because we will rechunk the others
        for f in files:
            print(f"Removing {f}")
            os.remove(os.path.join(data_dir, dirname, f))


    print("ALL DONE!")


if __name__ == "__main__":
    main()
