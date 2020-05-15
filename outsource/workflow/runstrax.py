#!/usr/bin/env python
import argparse
import os
import sys
import strax
import straxen
import time
from pprint import pprint
from ast import literal_eval
from utilix import db
import admix


def main():
    parser = argparse.ArgumentParser(description="Strax Processing With Outsource")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('--input_dtype', help='strax input')
    parser.add_argument('--output_dtype', help='strax output')
    parser.add_argument('--context', help='name of context')
    parser.add_argument('--chunks', nargs='*', help='chunk ids to download')

    args = parser.parse_args()

    # directory where we will be putting everything
    data_dir = './data'
    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory(data_dir)]

    runid = args.dataset
    in_dtype = args.input_dtype
    out_dtype = args.output_dtype
    hash = db.get_hash(args.context, in_dtype)

    # download the input data
    admix.download(runid, in_dtype, hash, chunks=args.chunks, location=data_dir)

    runid_str = "%06d" % runid
    input_metadata = st.get_metadata(runid_str, in_dtype)
    input_key = strax.DataKey(runid_str, in_dtype, input_metadata['lineage'])
    for chunk in args.chunks:
        in_data = st.storage[0].backends[0]._read_chunk(st.storage[0].find(input_key)[1],
                                                        chunk_info=input_metadata['chunks'][int(chunk)],
                                                        dtype=literal_eval(input_metadata['dtype']),
                                                        compressor=input_metadata['compressor'])

        plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
        st._set_plugin_config(plugin, runid_str, tolerant=False)
        plugin.setup()
        output_key = strax.DataKey(runid_str, out_dtype, plugin.lineage)

        output_data = plugin.do_compute(chunk_i=chunk, **{in_dtype: in_data})
        saver = st.storage[0].saver(output_key, plugin.metadata(runid, out_dtype))
        saver.is_forked = True

        # To save one chunk, do this:
        for key in output_data.keys():
            saver.save(output_data[key], chunk_i=chunk)


if __name__ == "__main__":
    main()
