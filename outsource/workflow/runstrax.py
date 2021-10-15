#!/usr/bin/env python3
import argparse
import os
import shutil
import sys
import tempfile
import numpy as np
import strax
import straxen
import time
from pprint import pprint
from shutil import rmtree, copyfile
from ast import literal_eval
from utilix import DB, uconfig
import admix
from admix.utils.naming import make_did
from admix.interfaces.rucio_summoner import RucioSummoner
import rucio
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
import datetime
from pprint import pprint

db = DB()
rc = RucioSummoner()

# these dtypes we need to rechunk, so don't upload to rucio here!
rechunk_dtypes = ['pulse_counts',
                  'veto_regions',
                  'peaklets',
                  'lone_hits',
                  'hitlets_nv'
                  ]

# these dtypes will not be uploaded to rucio
ignore_dtypes = ['records',
                 'records_nv',
                 'lone_raw_records_nv',
                 'raw_records_coin_nv',
                 'lone_raw_record_statistics_nv',
                 'records_he',
                 'records_mv',
                 ]

# these dtypes should always be made at the same time:
buddy_dtypes = [('veto_regions_nv', 'event_positions_nv'),
                ('event_info_double', 'event_pattern_fit')
                ]


def get_bottom_dtypes(dtype):
    if dtype in ['hitlets_nv', 'event_positions_nv', 'veto_regions_nv']:
        return ('raw_records_nv',)
    elif dtype in ['peak_basics_he']:
        return ('raw_records_he',)
    elif dtype in ['records', 'peaklets']:
        return ('raw_records',)
    elif dtype == 'veto_regions_mv':
        return ('raw_records_mv', )
    else:
        return ('peaklets', 'lone_hits')

def get_hashes(st):
    return {dt: item['hash'] for dt, item in st.provided_dtypes().items()}


def find_data_to_download(runid, target, st):
    runid_str = str(runid)
    hashes = get_hashes(st)
    bottoms = get_bottom_dtypes(target)

    for bottom in bottoms:
        if bottom not in hashes:
            raise ValueError(f"The dtype {bottom} is not in this context!")

    bottom_hashes = tuple([hashes[b] for b in bottoms])

    to_download = []

    data = db.get_data(runid, host='rucio-catalogue')

    def find_data(_target):
        if all([(d, h) in to_download for d, h in zip(bottoms, bottom_hashes)]):
            return

        # initialize plugin needed for processing
        _plugin = st._get_plugins((_target,), runid_str)[_target]
        st._set_plugin_config(_plugin, runid_str, tolerant=False)

        # download all the required datatypes to produce this output file
        for in_dtype in _plugin.depends_on:
            # get hash for this dtype
            hash = hashes.get(in_dtype)
            rses = [d['location'] for d in data if (d['type'] == in_dtype and
                                               hash in d['did']
                                               )
                    ]
            if len(rses) == 0:
                # need to download data to make ths one
                find_data(in_dtype)
            else:
                info = (in_dtype, hash)
                if info not in to_download:
                    to_download.append(info)

    # fills the to_download list
    find_data(target)
    return to_download


def process(runid,
            out_dtype,
            st,
            chunks,
            close_savers=False,
            tmp_path='.tmp_for_strax'
            ):
    runid_str = "%06d" % runid
    t0 = time.time()

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()
    plugin.chunk_target_size_mb = 1000

    # now move on to processing
    # if we didn't pass any chunks, we process the whole thing -- otherwise just do the chunks we listed
    if chunks is None:
        print("Chunks is none -- processing whole thing!")
        # then we just process the whole thing
        for keystring in plugin.provides:
            print(f"Making {keystring}")
            st.make(runid_str, keystring,
                    max_workers=4,
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

        for chunk in chunks:
            # read in the input data for this chunk
            chunk_info = None
            for chunk_md in input_metadata['chunks']:
                if chunk_md['chunk_i'] == int(chunk):
                    chunk_info = chunk_md
                    break
            assert chunk_info is not None, f"Could not find chunk_id: {chunk}"
            in_data = backend._read_and_format_chunk(backend_key=st.storage[0].find(input_key)[1],
                                                     metadata=input_metadata,
                                                     chunk_info=chunk_info,
                                                     dtype=dtype,
                                                     time_range=None,
                                                     chunk_construction_kwargs=chunk_kwargs
                                                    )
            # process this chunk
            output_data = plugin.do_compute(chunk_i=chunk, **{in_dtype: in_data})

            if isinstance(output_data, dict):
                # save the output -- you have to loop because there could be > 1 output dtypes
                for keystring, strax_chunk in output_data.items():
                    savers[keystring].save(strax_chunk, chunk_i=int(chunk))
            elif isinstance(output_data, strax.Chunk):
                # save the output -- you have to loop because there could be > 1 output dtypes
                savers[keystring].save(output_data, chunk_i=int(chunk))

        if close_savers:
            for dtype, saver in savers.items():
                # copy the metadata to a tmp directory
                tmpdir = os.path.join(tmp_path, os.path.split(saver.tempdirname)[1])
                os.makedirs(tmpdir, exist_ok=True)
                for file in os.listdir(saver.tempdirname):
                    if file.endswith('json'):
                        src = os.path.join(saver.tempdirname, file)
                        dest = os.path.join(tmpdir, file)
                        copyfile(src, dest)
                saver.close()
    process_time = time.time() - t0
    print(f"=== Processing time for {out_dtype}: {process_time/60:0.2f} minutes === ")


def main():
    parser = argparse.ArgumentParser(description="Strax Processing With Outsource")
    parser.add_argument('dataset', help='Run number', type=int)
    parser.add_argument('--output', help='desired strax(en) output')
    parser.add_argument('--context', help='name of context')
    parser.add_argument('--chunks', nargs='*', help='chunk ids to download')
    parser.add_argument('--cmt', type=str, default='global_ONLINE')
    parser.add_argument('--upload-to-rucio', action='store_true', dest='upload_to_rucio')
    parser.add_argument('--update-db', action='store_true', dest='update_db')
    parser.add_argument('--download-only', action='store_true', dest='download_only')
    parser.add_argument('--no-download', action='store_true', dest='no_download')

    args = parser.parse_args()

    # directory where we will be putting everything
    data_dir = './data'

    # make sure this is empty
    # if os.path.exists(data_dir):
    #     rmtree(data_dir)

    # get context
    st = getattr(straxen.contexts, args.context)(args.cmt)
    st.storage = [strax.DataDirectory(data_dir)]

    runid = args.dataset
    runid_str = "%06d" % runid
    out_dtype = args.output

    to_download = find_data_to_download(runid, out_dtype, st)
    for buddies in buddy_dtypes:
        if out_dtype in buddies:
            for other_dtype in buddies:
                if other_dtype == out_dtype:
                    continue
                to_download.extend(find_data_to_download(runid, other_dtype, st))

    to_download = list(set(to_download))

    # rse = None
    # # temporary hack to include SDSC
    # if os.environ.get("GLIDEIN_ResourceName", "not_expanse") == "SDSC-Expanse":
    #     print("On Expanse! Will attempt to download from SDSC")
    #     rse = 'SDSC_USERDISK'

    if not args.no_download:
        t0 = time.time()
        # download all the required datatypes to produce this output file
        if args.chunks:
            for in_dtype, hash in to_download:
                # download the input data
                if not os.path.exists(os.path.join(data_dir, f"{runid:06d}-{in_dtype}-{hash}")):
                    admix.download(runid, in_dtype, hash,
                                   chunks=args.chunks, location=data_dir)
        else:
            for in_dtype, hash in to_download:
                if not os.path.exists(os.path.join(data_dir, f"{runid:06d}-{in_dtype}-{hash}")):
                    admix.download(runid, in_dtype, hash, location=data_dir)
    
        download_time = time.time() - t0 # seconds
        print(f"=== Download time (minutes): {download_time/60:0.2f}")

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((out_dtype,), runid_str)[out_dtype]
    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    # figure out what plugins we need to process/initialize
    to_process = [args.output]
    for buddies in buddy_dtypes:
        if args.output in buddies:
            to_process = list(buddies)

    # keep track of the data we just downloaded -- will be important for the upload step later
    downloaded_data = os.listdir(data_dir)
    downloaded = [d.split('-')[1] for d in downloaded_data]

    missing = set(plugin.depends_on) - set(downloaded)
    intermediates = missing.copy()
    to_process = list(intermediates) + to_process

    while len(intermediates) > 0:
        new_intermediates = []
        for _dtype in intermediates:
            _plugin = st._get_plugins((_dtype,), runid_str)[_dtype]
            for dependency in _plugin.depends_on:
                if dependency not in downloaded:
                    if dependency not in to_process:
                        to_process = [dependency] + to_process
                    new_intermediates.append(dependency)
        intermediates = new_intermediates

    missing = [d for d in to_process if d != args.output]
    missing_str = ', '.join(missing)
    print(f"Need to create intermediate data: {missing_str}")

    print("--Downloaded data--")
    for dd in downloaded_data:
        print(dd)
    print("-------------------\n")

    if args.download_only:
        sys.exit(0)

    print(f"To process: {', '.join(to_process)}")
    _tmp_path = tempfile.mkdtemp()
    for dtype in to_process:
        close_savers = dtype != args.output
        process(runid,
                dtype,
                st,
                args.chunks,
                close_savers=close_savers,
                tmp_path=_tmp_path
                )

    print("Done processing. Now check if we should upload to rucio")

    # now we move the tmpfiles back to main directory, if needed
    # this is for cases where we went from raw_records-->records-->peaklets in one go
    if os.path.exists(_tmp_path):
        for dtype_path_thing in os.listdir(_tmp_path):
            tmp_path = os.path.join(_tmp_path, dtype_path_thing)
            merged_dir = os.path.join(data_dir, dtype_path_thing.split('_temp')[0])

            for file in os.listdir(tmp_path):
                copyfile(os.path.join(tmp_path, file), os.path.join(merged_dir, file))

            os.rename(merged_dir, os.path.join(data_dir, dtype_path_thing))


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

    if not args.upload_to_rucio:
        print("Ignoring rucio upload. Exiting. ")
        return

    for dirname in processed_data:
        # get rucio dataset
        this_run, this_dtype, this_hash = dirname.split('-')

        # remove data we do not want to upload
        if this_dtype in ignore_dtypes:
            print(f"Removing {this_dtype} instead of uploading")
            shutil.rmtree(os.path.join(data_dir, dirname))
            continue

        # based on the dtype and the utilix config, where should this data go?
        if this_dtype in ['records', 'pulse_counts', 'veto_regions', 'records_nv',
                          'records_he']:
            rse = uconfig.get('Outsource', 'records_rse')
        elif this_dtype in ['peaklets', 'lone_hits']:
            rse = uconfig.get('Outsource', 'peaklets_rse')
        else:
            rse = uconfig.get('Outsource', 'events_rse')

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
            if args.chunks:
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
                     rse=rse,
                     register_after_upload=True
                     )
            to_upload.append(d)

        # skip upload for now

        # now do the upload!
        if len(to_upload) == 0:
            print(f"No files to upload for {dirname}")
            continue
        try:
            # print("Doing upload")
            upload_client.upload(to_upload)
        except:
            print(f"Upload of {dset_name} failed for some reason")
            raise

        # TODO check rucio that the files are there?
        print(f"Upload of {len(files)} files in {dirname} finished successfully")

        # if we processed the whole thing, add a rule at DALI update the runDB here
        if args.chunks is None:
            # skip if update_db flag is false
            if args.update_db:
                md = st.get_meta(runid_str, this_dtype)
                chunk_mb = [chunk['nbytes'] / (1e6) for chunk in md['chunks']]
                data_size_mb = np.sum(chunk_mb)
                avg_data_size_mb = np.mean(chunk_mb)

                # update runDB
                new_data_dict = dict()
                new_data_dict['location'] = rse
                new_data_dict['did'] = dataset
                new_data_dict['status'] = 'transferred'
                new_data_dict['host'] = "rucio-catalogue"
                new_data_dict['type'] = this_dtype
                new_data_dict['protocol'] = 'rucio'
                new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
                new_data_dict['creation_place'] = "OSG"
                new_data_dict['meta'] = dict(lineage_hash=md.get('lineage_hash'),
                                             file_count=len(files),
                                             size_mb=data_size_mb,
                                             )

                db.update_data(runid, new_data_dict)
                print(f"Database updated for {this_dtype} at {rse}")

        # cleanup the files we uploaded
        # this is likely only done for records data because we will rechunk the others
        for f in files:
            print(f"Removing {f}")
            os.remove(os.path.join(data_dir, dirname, f))

    print("ALL DONE!")


if __name__ == "__main__":
    main()
