#!/usr/bin/env python
from argparse import ArgumentParser
from rucio.client.client import Client
import straxen
import strax
from utilix import DB
from admix.utils.naming import make_did
import datetime
from pprint import pprint

db = DB()

def apply_global_version(context, cmt_version):
    context.set_config(dict(gain_model=('CMT_model', ("to_pe_model", cmt_version))))
    context.set_config(dict(s2_xy_correction_map=("CMT_model", ('s2_xy_map', cmt_version), True)))
    context.set_config(dict(elife_file=("elife_model", cmt_version, True)))
    context.set_config(dict(mlp_model=("CMT_model", ("mlp_model", cmt_version), True)))
    context.set_config(dict(gcn_model=("CMT_model", ("gcn_model", cmt_version), True)))
    context.set_config(dict(cnn_model=("CMT_model", ("cnn_model", cmt_version), True)))


def get_hashes(st):
    hashes = set([(d, st.key_for('0', d).lineage_hash)
                  for p in st._plugin_class_registry.values()
                  for d in p.provides if p.save_when != strax.SaveWhen.NEVER])
    return {dtype: h for dtype, h in hashes}


def main():
    parser = ArgumentParser()
    parser.add_argument('runid', type=int, help='Run number')
    parser.add_argument('--dtype', help='dtype', required=True)
    parser.add_argument('--context', help='Context name', required=True)
    parser.add_argument('--rse', help='RSE to create replication rule at')
    parser.add_argument('--cmt', help='Global CMT version', default='ONLINE')
    parser.add_argument('--update-db', help='flag to update runsDB', dest='update_db',
                        action='store_true')
    parser.add_argument('--upload-to-rucio', help='flag to upload to rucio', dest='upload_to_rucio',
                        action='store_true')

    args = parser.parse_args()

    runid = args.runid
    runid_str = "%06d" % runid
    dtype = args.dtype

    dtypes = ['records', 'peaklets']

    # setup rucio client
    C = Client()

    # get context
    st = getattr(straxen.contexts, args.context)()

    # apply global version
    apply_global_version(st, args.cmt)

    for dtype in dtypes:

        # initialize plugin needed for processing this output type
        plugin = st._get_plugins((dtype,), runid_str)[dtype]

        st._set_plugin_config(plugin, runid_str, tolerant=False)
        plugin.setup()

        for _dtype in plugin.provides:
            hash = get_hashes(st)[_dtype]

            # need to create the dataset we will be uploading data to out on the grid
            dataset = make_did(args.runid, _dtype, hash)
            scope, name = dataset.split(':')

            # check if this dataset exists
            existing_datasets = [i for i in C.list_dids(scope, filters=dict(type='dataset'))]

            if name not in existing_datasets:
                C.add_dataset(scope, name)
                print(f"Dataset {dataset} created")
            else:
                print(f"Warning: The dataset {dataset} already exists!")
                #raise ValueError(f"The dataset {dataset} already exists!")

            #check if a rule already exists
            existing_rules = [i['rse_expression'] for i in C.list_did_rules(scope, name)]

            # update runDB
            new_data_dict = dict()
            new_data_dict['location'] = args.rse
            new_data_dict['did'] = dataset
            new_data_dict['status'] = 'processing'
            new_data_dict['host'] = "rucio-catalogue"
            new_data_dict['type'] = _dtype
            new_data_dict['protocol'] = 'rucio'
            new_data_dict['creation_time'] = datetime.datetime.utcnow().isoformat()
            new_data_dict['creation_place'] = "OSG"
            new_data_dict['meta'] = dict(lineage=None,
                                         avg_chunk_mb=None,
                                         file_count=None,
                                         size_mb=None,
                                         strax_version=strax.__version__,
                                         straxen_version=straxen.__version__
                                         )

            if args.rse not in existing_rules:
                # 1 is the number of copies
                if args.upload_to_rucio:
                    C.add_replication_rule([dict(scope=scope, name=name)], 1, args.rse)
                    print(f"Replication rule at {args.rse} created")

                if args.update_db:
                    db.update_data(runid, new_data_dict)

                # send peaklets data to dali
                if dtype == 'peaklets' and args.rse != 'UC_DALI_USERDISK':
                    if args.upload_to_rucio:
                        C.add_replication_rule([dict(scope=scope, name=name)], 1, 'UC_DALI_USERDISK',
                                               source_replica_expression=args.rse,
                                               priority=5)
                    # if args.update_db:
                    #     new_data_dict['location'] = 'UC_DALI_USERDISK'
                    #     db.update_data(runid, new_data_dict)


if __name__ == "__main__":
    main()

