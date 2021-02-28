#!/usr/bin/env python
from argparse import ArgumentParser
from rucio.client.client import Client
from straxen import __version__ as straxen_version
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

    hash = db.get_hash(args.context, args.dtype, straxen_version)

    # need to create the dataset we will be uploading data to out on the grid
    dataset = make_did(args.runid, args.dtype, hash)
    scope, name = dataset.split(':')

    # check if this dataset exists
    existing_datasets = [i for i in C.list_dids(scope, filters=dict(type='dataset'))]

    print(name)
    print(existing_datasets)

    if name not in existing_datasets:
        C.add_dataset(scope, name)
        print(f"Dataset {dataset} created")
    print(f"WARNING: the dataset {dataset} already exists")

    #check if a rule already exists
    existing_rules = [i['rse_expression'] for i in C.list_did_rules(scope, name)]
    print(existing_rules)
    print(args.rse)

    if args.rse not in existing_rules:
        # 1 is the number of copies
        C.add_replication_rule([dict(scope=scope, name=name)], 1, args.rse)
        print(f"Replication rule at {args.rse} created")

    # TODO do a step to update the status for this data type?




if __name__ == "__main__":
    main()
