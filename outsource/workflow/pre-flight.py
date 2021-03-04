#!/usr/bin/env python
from argparse import ArgumentParser
from rucio.client.client import Client
import straxen
import strax
from utilix import db
from admix.utils.naming import make_did


def main():
    parser = ArgumentParser()
    parser.add_argument('runid', type=int, help='Run number')
    parser.add_argument('--dtype', type=str, help='datatype we will be processing', required=True)
    parser.add_argument('--context', help='Context name', required=True)
    parser.add_argument('--rse', help='RSE to create replication rule at')

    args = parser.parse_args()

    # setup rucio client
    C = Client()

    runid = args.runid
    runid_str = "%06d" % runid
    dtype  = args.dtype

    # get context
    st = eval(f'straxen.contexts.{args.context}()')

    # initialize plugin needed for processing this output type
    plugin = st._get_plugins((dtype,), runid_str)[dtype]

    st._set_plugin_config(plugin, runid_str, tolerant=False)
    plugin.setup()

    for keystring in plugin.provides:
        key = strax.DataKey(runid_str, keystring, plugin.lineage)
        hash = key.lineage_hash

        # need to create the dataset we will be uploading data to out on the grid
        dataset = make_did(args.runid, args.dtype, hash)
        scope, name = dataset.split(':')

        # check if this dataset exists
        existing_datasets = [i for i in C.list_dids(scope, filters=dict(type='dataset'))]

        if name not in existing_datasets:
            C.add_dataset(scope, name)
            print(f"Dataset {dataset} created")
        print(f"WARNING: the dataset {dataset} already exists")

        #check if a rule already exists
        existing_rules = [i['rse_expression'] for i in C.list_did_rules(scope, name)]

        if args.rse not in existing_rules:
            # 1 is the number of copies
            C.add_replication_rule([dict(scope=scope, name=name)], 1, args.rse)
            print(f"Replication rule at {args.rse} created")

        # TODO do a step to update the status for this data type?


if __name__ == "__main__":
    main()
