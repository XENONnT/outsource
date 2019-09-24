#!/usr/bin/env python

import argparse
import tempfile
import os
from shutil import copytree, rmtree
import strax
import straxen


def main():
    parser = argparse.ArgumentParser(description="Combine strax output")
    parser.add_argument('dataset', help='Run name')
    parser.add_argument('dtype', help='dtype to combine')
    parser.add_argument('--input_path', help='path where the temp directory is')
    parser.add_argument('--output_path', help='final location of combined data')

    args = parser.parse_args()

    if os.path.exists(args.output_path):
        raise(FileExistsError("Output path %s already exists" % args.output_path))

    runid = args.dataset
    dtype = args.dtype
    path = args.input_path
    tmp_path = tempfile.mkdtemp()

    st = strax.Context(storage=[strax.DataDirectory(path=tmp_path)],
                       register=straxen.plugins.pax_interface.RecordsFromPax,
                       **straxen.contexts.common_opts)

    plugin = st._get_plugins((dtype,), runid)[dtype]
    output_key = strax.DataKey(runid, dtype, plugin.lineage)
    saver = st.storage[0].saver(output_key, plugin.metadata(runid, dtype))
    tmpdir, tmpname = os.path.split(saver.tempdirname)
    rmtree(saver.tempdirname)
    copytree(os.path.join(path, tmpname), saver.tempdirname)
    saver.is_forked = True
    saver.close()

    copytree(tmp_path, args.output_path)


if __name__ == "__main__":
    main()
