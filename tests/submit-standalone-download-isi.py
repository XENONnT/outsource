#!/usr/bin/env python3

import sys
import argparse

sys.path.insert(0, '.')

from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig


def main():
    parser = argparse.ArgumentParser("Outsource Testing")
    parser.add_argument('--run', type=int, default=8000)
    parser.add_argument('--context', default='xenonnt_online')
    parser.add_argument('--cmt', default='ONLINE')
    parser.add_argument('--xsede', action='store_true')

    args = parser.parse_args()

    configs = [DBConfig(args.run, context_name=args.context, cmt_version=args.cmt,
                        force_rerun=True, upload_to_rucio=False, update_db=False,
                        standalone_download=True, staging_site='staging-isi'
                        )
               ]
    outsource = Outsource(configs, xsede=args.xsede, debug=True)
    outsource.submit_workflow()


if __name__ == '__main__':
    main()