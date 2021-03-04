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
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
import datetime

def find_data_to_download(runid, target, st, context_name):
    runid_str = str(runid)

    bottom = "peaklets"
    #bottom_hash = db.get_hash(context_name, bottom, straxen.__version__)
    #bottom_dset = f"{bottom}-{bottom_hash}"

    print(bottom)

    to_download = []


    def find_data(_target, st):
        # peaklets is the bottom of the chain -- just exit if we get there
        # this is really tricky since we use recursive function and is just hack
        if any([d[0] == bottom for d in to_download]):
            return

        # initialize plugin needed for processing
        _plugin = st._get_plugins((_target,), runid_str)[_target]
        st._set_plugin_config(_plugin, runid_str, tolerant=False)
        _plugin.setup()

        # download all the required datatypes to produce this output file
        for in_dtype in _plugin.depends_on:
            print(_target, in_dtype)
            print(to_download)
            # get hash for this dtype
            hash = db.get_hash(context_name, in_dtype, straxen.__version__)
            rses = db.get_rses(int(runid_str), in_dtype, hash)
            # print(in_dtype, rses)
            if len(rses) == 0:
                # need to download data to make ths one
                find_data(in_dtype, st)
            else:
                info = (in_dtype, hash) #make_did(runid, in_dtype, hash)
                if info not in to_download:
                    to_download.append(info)
            if in_dtype == bottom:
                return

    # fills the to_download list
    find_data(target, st)

    return to_download


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
    #st.set_config(dict(gain_model=('CMT_model', ("to_pe_model", "v2"))))

    runid = args.dataset
    runid_str = "%06d" % runid
    out_dtype = args.output

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
    
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    print(args.chunks)
    # download all the required datatypes to produce this output file
    if args.chunks:
        for in_dtype in plugin.depends_on:
            # get hash for this dtype
            hash = db.get_hash(args.context, in_dtype, straxen.__version__)
            # download the input data
            admix.download(runid, in_dtype, hash, chunks=args.chunks, location=data_dir)
    else:
        # download all the data we need
        to_download = find_data_to_download(runid, out_dtype, st, args.context)
        for in_dtype, hash in to_download:
            admix.download(runid, in_dtype, hash, location=data_dir)


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
            # make sure we have the expected number of outputs
            if len(files) != len(args.chunks) + 1:
                raise RuntimeError("The number of output files does not match the number of chunks (+1 metadata) !")

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

        # if we processed the whole thing, update the runDB here
        if args.chunks is None:
            md = st.get_meta(runid_str, this_dtype)
            chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
            data_size_mb = int(np.sum(chunk_mb))
            avg_data_size_mb = int(np.average(chunk_mb))


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

    print("ALL DONE!")


def debug_datafind():
    runid = 8107
    target = 'event_info_double'
    st = straxen.contexts.xenonnt_online()
    st.context_config['forbid_creation_of'] = straxen.daqreader.DAQReader.provides

    data = find_data_to_download(runid, target, st, 'xenonnt_online')
    print(data)

if __name__ == "__main__":
    main()
    #debug_datafind()

