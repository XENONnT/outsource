#!/usr/bin/env python
import argparse
import os
import sys
import strax
import straxen
import time
from ast import literal_eval
from utilix import db
from admix.interfaces.rucio_summoner import RucioSummoner

EURO_SITES = ["CCIN2P3_USERDISK", 
              "NIKHEF_USERDISK",
              "WEIZMANN_USERDISK", 
              "CNAF_USERDISK", 
              "SURFSARA_USERDISK"]

US_SITES = ["UC_OSG_USERDISK"]

ALL_SITES = US_SITES + EURO_SITES


def determine_rse(rse_list, glidein_country):
    if glidein_country == "US":
        in_US = False
        for site in US_SITES:
            if site in rse_list:
                return site

        if not in_US:
            print("This run is not in the US so can't be processed here. Exit 255")
            sys.exit(255)

    elif glidein_country == "FR":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "NL":
        for site in reversed(EURO_SITES):
            if site in rse_list:
                return site

    elif glidein_country == "IL":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "IT":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    if US_SITES[0] in rse_list:
        return US_SITES[0]
    else:
        raise AttributeError("cannot download data")


def main():
    parser = argparse.ArgumentParser(description="strax testing")
    parser.add_argument('dataset', help='Run name')
    parser.add_argument('input_dtype', help='strax input')
    parser.add_argument('output_dtype', help='strax output')
    parser.add_argument('chunk', help='chunk id number to download', type=int)

    args = parser.parse_args()

    runid = args.dataset
    in_dtype = args.input_dtype
    out_dtype = args.output_dtype
    did = db.get_did(runid, type=in_dtype)
    key = did.split(':')[1].split('-')[1]
    chunk_string = str(args.chunk).zfill(6)
    
    file1 = did + '-' + chunk_string
    file2 = did + '-metadata.json'
    rucio_dir = os.path.join('data', runid + "-" + in_dtype + "-" + key)

    rc = RucioSummoner("API")
    rc.ConfigHost()

    rucio_config = {'L0': {'type': 'rucio_container', 'did': did},
                    'L1': {'type': 'rucio_container', 'did': did},
                    'L2': {'type': 'rucio_dataset', 'did': did}
                    }

    rules = rc.ListDidRules(rucio_config)

    rses = []
    for r in rules:
        if r['state'] == 'OK':
            rses.append(r['rse_expression'])

    rse = determine_rse(rses, os.environ.get('GLIDEIN_Country', 'US'))
    tries = 3
    _try = 0
    while _try < tries:
        _try += 1
        try:
            ds = rc.DownloadDids([file1, file2], download_path=rucio_dir, rse=rse,
                                 no_subdir=True, transfer_timeout=None)
        except:
            print(f"Download failed. Doing retry #{_try}")
            time.sleep(10)
            continue

    for ik in ds:
        if ik.get('clientState', 'fail') == 'DONE':
            print('Download of {file} completed.'.format(file=ik.get('did')))

    st = strax.Context(storage=[strax.DataDirectory(path='data')],
                       register=straxen.plugins.pax_interface.RecordsFromPax,
                       config=dict(s2_tail_veto=False, filter=None),
                        **straxen.contexts.common_opts)
    
    input_metadata = st.get_metadata(runid, in_dtype)
    input_key = strax.DataKey(runid, in_dtype, input_metadata['lineage'])
    in_data = st.storage[0].backends[0]._read_chunk(st.storage[0].find(input_key)[1], 
                                                 chunk_info=input_metadata['chunks'][args.chunk],
                                                 dtype=literal_eval(input_metadata['dtype']), 
                                                 compressor=input_metadata['compressor'])

    plugin = st._get_plugins((out_dtype,), runid)[out_dtype]
    output_key = strax.DataKey(runid, out_dtype, plugin.lineage)

    output_data = plugin.do_compute(chunk_i=args.chunk, **{in_dtype: in_data})
    saver = st.storage[0].saver(output_key, plugin.metadata(runid))
    saver.is_forked = True

    # To save one chunk, do this:
    saver.save(output_data, chunk_i=args.chunk)

    # To merge results from many chunks, copy the sub-results into the temp directory now
    # (after creating the saver, since otherwise the saver will remove the files)
    # then do:
    # saver.close()

if __name__ == "__main__":
    main()
