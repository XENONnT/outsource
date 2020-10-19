#!/usr/bin/env python3
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
    parser.add_argument('--output', help='desired strax(en) output')
    parser.add_argument('--context', help='name of context')
    parser.add_argument('--chunks', nargs='*', help='chunk ids to download')

    args = parser.parse_args()

    # directory where we will be putting everything
    data_dir = './data'

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory(data_dir)]

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

    # setup savers
    savers = dict()
    for keystring in plugin.provides:
        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        saver = st.storage[0].saver(key, plugin.metadata(runid, keystring))
        saver.is_forked = True
        savers[keystring] = saver

    # setup a few more variables
    # TODO not sure exactly how this works when an output plugin depends on >1 plugin
    in_dtype = plugin.depends_on[0]
    input_metadata = st.get_metadata(runid_str, in_dtype)
    input_key = strax.DataKey(runid_str, in_dtype, input_metadata['lineage'])
    backend = st.storage[0].backends[0]
    dtype = literal_eval(input_metadata['dtype'])
    chunk_kwargs = dict(data_type=input_metadata['data_type'],
                        data_kind=input_metadata['data_kind'],
                        dtype=dtype)

    # process the chunks
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


if __name__ == "__main__":
    main()
