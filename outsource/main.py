#!/usr/bin/env python3

import argparse
from utilix.io import load_runlist

from outsource.Outsource import Outsource, DEFAULT_IMAGE


def main():
    parser = argparse.ArgumentParser("Outsource")
    parser.add_argument("--run", type=int)
    parser.add_argument("--runlist", type=str, help="path to runlist")
    parser.add_argument("--context", default="xenonnt")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--name", help="Custom name of workflow directory. If not passed, inferred from run/runlist"
    )
    parser.add_argument("--image", default=DEFAULT_IMAGE, help="path to singularity image")
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

    outsource = Outsource(
        runlist,
        args.context,
        force_rerun=args.force,
        upload_to_rucio=upload_to_rucio,
        update_db=update_db,
        debug=args.debug,
        image=args.image,
        wf_id=args.name,
    )

    outsource.submit_workflow()


if __name__ == "__main__":
    main()
