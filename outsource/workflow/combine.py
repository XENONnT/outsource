#!/usr/bin/env python

import argparse
import os
import strax
import straxen

def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to combine')
    parser.add_argument('--context', help='Strax context')
    parser.add_argument('--input', help='path where the temp directory is')

    args = parser.parse_args()

    #if os.path.exists(args.output_path):
    #    raise(FileExistsError("Output path %s already exists" % args.output_path))

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

        saver.close()


if __name__ == "__main__":
    main()
