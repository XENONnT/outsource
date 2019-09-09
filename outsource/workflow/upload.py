#!/usr/bin/env python

import os
import argparse
import datetime

import strax
import straxen
from utilix import db

from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword


def main():
    parser = argparse.ArgumentParser(description="Upload combined output to rucio")
    parser.add_argument('dataset', help='Run number/name')
    parser.add_argument('dtype', help='dtype to upload')
    parser.add_argument('rse', help='Target RSE')

    args = parser.parse_args()

    runid = args.dataset
    dtype = args.dtype
    rse = args.rse

    st = strax.Context(storage=[strax.DataDirectory(path="combined")],
                       register=straxen.plugins.pax_interface.RecordsFromPax,
                       **straxen.contexts.common_opts)

    plugin = st._get_plugins((dtype,), runid)[dtype]
    output_key = strax.DataKey(runid, dtype, plugin.lineage)

    hash = output_key.lineage_hash

    dirname = f"{runid}-{dtype}-{hash}"
    upload_path = os.path.join('combined', dirname)

    rc_reader_path = "/home/ershockley/.xenon-rucio-config"
    rc_reader = ConfigRucioDataFormat()
    rc_reader.Config(rc_reader_path)

    rc = RucioSummoner("API")
    rc.SetRucioAccount("production")
    rc.ConfigHost()

    #Init a class to handle keyword strings:
    keyw = Keyword()

    rucio_template = rc_reader.GetPlugin(args.dtype)

    run_name = db.get_name(int(runid))

    _hash = hash
    _type = dtype
    _date, _time = run_name.split('_')
    _det = "tpc"


    keyw.SetTemplate({'hash': _hash, 'plugin': _type, 'date': _date, 'time': _time, 'detector': _det})
    keyw.SetTemplate({'science_run': "001"})

    rucio_template = keyw.CompleteTemplate(rucio_template)

    result = rc.Upload(upload_structure=rucio_template,
                       upload_path=upload_path,
                       rse=rse,
                       rse_lifetime=None)
    #result not yet done... :/

    #ask for rule from Rucio for the template and RSE
    rucio_rule_status = []
    rucio_rule_rse0 = rc.GetRule(upload_structure=rucio_template, rse=rse)
    rucio_rule_status.append("{rse}:{state}:{lifetime}".format(rse=rse,
                                                               state=rucio_rule_rse0['state'],
                                                               lifetime=rucio_rule_rse0['expires']))

    new_data_dict={}
    new_data_dict['location'] = rucio_template['L2']['did']
    new_data_dict['status'] = "transferred"
    new_data_dict['host'] = "rucio-catalogue"
    new_data_dict['type'] = dtype
    new_data_dict['meta'] = {}
    new_data_dict['rse'] = rucio_rule_status
    #new_data_dict['destination'] = ""
    new_data_dict['checksum'] = 'shit'
    new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
    db.update_data(runid, new_data_dict)


if __name__ == "__main__":
    main()
