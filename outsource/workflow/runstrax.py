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
from admix.interfaces.rucio_summoner import RucioSummoner

EURO_SITES = ["CCIN2P3_USERDISK", 
              "NIKHEF_USERDISK",
              "NIKHEF2_USERDISK",
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
    download_done = False
    tries = 3
    _try = 0
    while not download_done and _try < tries:
        _try += 1
        print('\nAttempting download of {file1} and {file2} from {rse} ...'\
              .format(file1=file1, file2=file2, rse=rse))
        try:
            ds = rc.DownloadDids([file1, file2], download_path=rucio_dir, rse=rse,
                                 no_subdir=True, transfer_timeout=None)
            # sometimes DownloadDids fails with a 'Protocol implementation not found' and
            # a int instead of a list - catch this corner case here
            if isinstance(ds, int):
                print(f"Download try #{_try} failed.")
                time.sleep(10)
                continue
            download_done = True
        except:
            print(f"Download failed. Doing retry #{_try}")
            time.sleep(10)
            continue

    if not download_done:
        print('Unable to download the input data! Exiting...')
        sys.exit(1)

    for ik in ds:
        pprint(ik)
        if ik.get('clientState', 'fail') == 'DONE':
            print('Download of {file} from {site} completed.'.format(file=ik.get('did'),
                                                                     site=ik.get('rse_expression')))

    st = strax.Context(storage=[strax.DataDirectory(path='data')],
                       register=straxen.plugins.pax_interface.RecordsFromPax,
                        **straxen.contexts.common_opts)
    
    input_metadata = st.get_metadata(runid, in_dtype)
    input_key = strax.DataKey(runid, in_dtype, input_metadata['lineage'])
    in_data = st.storage[0].backends[0]._read_chunk(st.storage[0].find(input_key)[1], 
                                                 chunk_info=input_metadata['chunks'][args.chunk],
                                                 dtype=literal_eval(input_metadata['dtype']),
                                                 compressor=input_metadata['compressor'])

    plugin = st._get_plugins((out_dtype,), runid)[out_dtype]
    st._set_plugin_config(plugin, runid, tolerant=False)
    plugin.setup()
    output_key = strax.DataKey(runid, out_dtype, plugin.lineage)

    output_data = plugin.do_compute(chunk_i=args.chunk, **{in_dtype: in_data})
    saver = st.storage[0].saver(output_key, plugin.metadata(runid, out_dtype))
    saver.is_forked = True

    # To save one chunk, do this:
    for key in output_data.keys():
        saver.save(output_data[key], chunk_i=args.chunk)


if __name__ == "__main__":
    main()
