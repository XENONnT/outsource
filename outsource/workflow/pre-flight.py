#!/usr/bin/env python
from argparse import ArgumentParser
from rucio.client.client import Client
import straxen
import strax
from utilix import DB, uconfig
from admix.utils.naming import make_did
import datetime

db = DB()


def main():
    parser = ArgumentParser()
    parser.add_argument('runid', type=int, help='Run number')
    parser.add_argument('--dtype', help='dtype', required=True)
    parser.add_argument('--context', help='Context name', required=True)
    parser.add_argument('--cmt', help='Global CMT version', default='global_ONLINE')
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
    st = getattr(straxen.contexts, args.context)(args.cmt)

    output_rses = {'records': uconfig.get('Outsource', 'records_rse'),
                   'peaklets': uconfig.get('Outsource', 'peaklets_rse')
                   }
    for dtype in dtypes:

        # initialize plugin needed for processing this output type
        plugin = st._get_plugins((dtype,), runid_str)[dtype]

        st._set_plugin_config(plugin, runid_str, tolerant=False)
        plugin.setup()

        rse = output_rses[dtype]


        for _dtype in plugin.provides:
            hash = st.provided_dtypes()[_dtype]['hash']

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

            #check if a rule already exists
            existing_rules = [i['rse_expression'] for i in C.list_did_rules(scope, name)]

            # update runDB
            new_data_dict = dict()
            new_data_dict['location'] = rse
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

            if rse not in existing_rules:
                # 1 is the number of copies
                if args.upload_to_rucio:
                    C.add_replication_rule([dict(scope=scope, name=name)], 1, rse)
                    print(f"Replication rule at {rse} created")

                if args.update_db:
                    db.update_data(runid, new_data_dict)


if __name__ == "__main__":
    main()

