#!/usr/bin/env python

import argparse
import tempfile
import os
from shutil import copytree, rmtree
import strax
import straxen
from utilix import db
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.utils.naming import make_did


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('dtype', help='dtype to combine')
    parser.add_argument('--context', help='Strax context')
    parser.add_argument('--input_path', help='path where the temp directory is')
    parser.add_argument('--output_path', help='final location of combined data')

    args = parser.parse_args()

    if os.path.exists(args.output_path):
        raise(FileExistsError("Output path %s already exists" % args.output_path))

    runid = args.dataset
    runid_str = "%06d" % runid
    dtype = args.dtype
    path = args.input_path
    output_path = args.output_path

    # get context
    st = eval(f'straxen.contexts.{args.context}()')
    st.storage = [strax.DataDirectory(output_path)]

    # initialize plugin needed for processing
    plugin = st._get_plugins((dtype,), runid_str)[dtype]
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    # setup rucio client
    rc = RucioSummoner()


    for keystring in plugin.provides:
        dirname = f"{runid_str}-{keystring}-{hash}"
        upload_path = os.path.join(output_path, dirname)

        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        saver = st.storage[0].saver(key, plugin.metadata(runid_str, keystring))
        saver.is_forked = True

        tmpdir, tmpname = os.path.split(saver.tempdirname)
        rmtree(saver.tempdirname)
        copytree(os.path.join(path, tmpname), saver.tempdirname)
        saver.is_forked = True
        saver.close()


if __name__ == "__main__":
    main()
