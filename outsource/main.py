#!/usr/bin/env python3

import argparse
from tqdm import tqdm
from utilix.io import load_runlist

from outsource.Outsource import Outsource, DEFAULT_IMAGE
from outsource.RunConfig import DBConfig


def main():
    parser = argparse.ArgumentParser("Outsource")
    parser.add_argument('--run', type=int)
    parser.add_argument('--runlist', type=str, help='path to runlist')
    parser.add_argument('--context', default='xenonnt')
    parser.add_argument('--cmt', default='global_ONLINE')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--name', help='Custom name of workflow directory. If not passed, inferred from run/runlist')
    parser.add_argument('--image', default=DEFAULT_IMAGE, help='path to singularity image')
    args = parser.parse_args()

    if not (args.run or args.runlist):
        print("Need to pass either --run or --runlist")
        return

    upload_to_rucio = update_db = True

    if args.debug:
        upload_to_rucio = update_db = False

    if args.runlist:
        runlist = load_runlist(args.runlist)

    else:
        runlist = [args.run]

    configs = []
    for run in tqdm(runlist, desc="Building configs for the passed run(s)"):
        configs.append(DBConfig(run, context_name=args.context, cmt_version=args.cmt,
                        force_rerun=args.force, upload_to_rucio=upload_to_rucio, update_db=update_db
                        )
                       )
    outsource = Outsource(configs, debug=args.debug, image=args.image)
    outsource.submit_workflow(force=args.force)

if __name__ == '__main__':
    main()
