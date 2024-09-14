#!/usr/bin/env python3

import sys
import argparse

from pprint import pprint

from utilix import rundb


def update_status(run_id, dtype, status):
    db = rundb.DB()
    data = db.get_data(run_id)
    for data_set in data:
        if "type" in data_set and data_set["type"] == dtype:
            if data_set["checksum"] is None:
                data_set["checksum"] = "None"
            data_set["status"] = status
            print("\nUpdating the following data section to:")
            pprint(data_set)
            db.update_data(run_id, data_set)


def main():
    # top-level parser
    parser = argparse.ArgumentParser(prog="update_run_db")
    subparsers = parser.add_subparsers(
        title="subcommands", dest="cmd", description="valid subcommands", help="sub-command help"
    )

    # sub parser for 'update-status'
    parser_a = subparsers.add_parser("update-status", help="Sets the status of a run/data")
    parser_a.add_argument("--run-id", type=int, help="The run id")
    parser_a.add_argument("--dtype", type=str, help="The dtype of the data record")
    parser_a.add_argument("--status", type=str, help="The new status")

    # sub parser for 'add-results'
    parser_b = subparsers.add_parser("add-results", help="b help")
    parser_b.add_argument("--baz", choices="XYZ", help="baz help")

    args = parser.parse_args()

    print(args)

    if args.cmd == "update-status":
        update_status(args.run_id, args.dtype, args.status)
    elif args.cmd == "add-results":
        print("Not implemented yet...")
        sys.exit(1)
    else:
        print("Unknown command")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
