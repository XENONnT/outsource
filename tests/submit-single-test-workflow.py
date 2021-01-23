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

    args = parser.parse_args()

    configs = [DBConfig(args.run, strax_context=args.context, straxen_version='0.14.3',
                        ignore_rucio=False, ignore_db=False)]
    outsource = Outsource(configs)
    outsource.submit_workflow()


if __name__ == '__main__':
    main()
