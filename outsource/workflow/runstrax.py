#!/usr/bin/env python3
import argparse
import os
import sys
import strax
import straxen
import time
from pprint import pprint
from shutil import rmtree
from ast import literal_eval
from utilix import db
import admix
from admix.utils.naming import make_did
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient


def main():
    parser = argparse.ArgumentParser(description="Strax Processing With Outsource")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('--output', help='desired strax(en) output')
    parser.add_argument('--context', help='name of context')
    parser.add_argument('--chunks', nargs='*', help='chunk ids to download')
    parser.add_argument('--rse', type=str, default="UC_OSG_USERDISK")

    args = parser.parse_args()

    # directory where we will be putting everything
    data_dir = './data'

    # make sure this is empty
    if os.path.exists(data_dir):
        rmtree(data_dir)

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory(data_dir)]
    st.context_config['forbid_creation_of'] = straxen.daqreader.DAQReader.provides

    runid = args.dataset
    runid_str = "%06d" % runid
    out_dtype = args.output

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
    
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    # download all the required datatypes to produce this output file
    for in_dtype in plugin.depends_on:
        # get hash for this dtype
        hash = db.get_hash(args.context, in_dtype, straxen.__version__)
        # download the input data
        admix.download(runid, in_dtype, hash, chunks=args.chunks, location=data_dir)

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
            st.make(runid_str, keystring)

        #print("Processing done. Exiting.")

        # now upload data to rucio
        #return

    # process chunk-by-chunk
    else:
        # setup savers
        savers = dict()
        for keystring in plugin.provides:
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

        # process the chunks
        if args.chunks is not None:
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
    print("Done processing. Now we upload to rucio")

    # initiate the rucio client
    # the rucio logo is a donkey
    donkey = Client()
    updonkey = UploadClient()

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

        # remove the _temp if we are processing chunks in parallel
        if args.chunks is not None:
            this_hash = this_hash.replace('_temp', '')

        dataset = make_did(int(this_run), this_dtype, this_hash)

        scope, dset_name = dataset.split(':')

        files = [f for f in os.listdir(os.path.join(data_dir, dirname))]
        if not upload_meta:
            files = [f for f in files if not f.endswith('.json')]

        # if there are no files, we can't upload them
        if len(files) == 0:
            print(f"No files to upload in {dirname}. Skipping.")
            continue

        # prepare list of dicts to be uploaded
        to_upload = []
        for f in files:
            path = os.path.join(data_dir, dirname, f)
            #print(f"Uploading {os.path.basename(path)}")
            d = dict(path=path,
                     did_scope=scope,
                     did_name=f,
                     dataset_scope=scope,
                     dataset_name=dset_name,
                     rse=args.rse,
                     register_after_upload=True
                     )
            to_upload.append(d)

        # now do the upload!
        try:
            updonkey.upload(to_upload)
            print(f"Upload of {len(files)} files in {dirname} finished successfully")
        except:
            print(f"Upload of {dset_name} failed for some reason")
        for f in files:
            print(f"{scope}:{f}")

        # cleanup the files we uploaded
        for f in files:
            print(f"Removing {f}")
            os.remove(os.path.join(data_dir, dirname, f))

    print("ALL DONE!")


if __name__ == "__main__":
    main()
