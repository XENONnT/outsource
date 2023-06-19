#!/usr/bin/env python3
from argparse import ArgumentParser
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig
import numpy as np

if __name__ == '__main__':
    parser = ArgumentParser("Outsource Testing")
    parser.add_argument('--context', default='xenonnt_online')
    parser.add_argument('--cmt', default='ONLINE')

    args = parser.parse_args()

    runs = np.arange(8300, 8500)

    configs = [DBConfig(r, context_name=args.context, cmt_version=args.cmt,
                        ignore_rucio=False, ignore_db=False) for r in runs
               ]
    outsource = Outsource(configs, xsede=False)
    outsource.submit_workflow()
