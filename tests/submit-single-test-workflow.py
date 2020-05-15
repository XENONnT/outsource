#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig
import argparse


def main():
    parser = argparse.ArgumentParser("Outsource Testing")
    parser.add_argument('--run', type=int, default=7340)

    args = parser.parse_args()

    configs = [DBConfig(args.run)]
    outsource = Outsource(configs)
    outsource.submit_workflow()


if __name__ == '__main__':
    main()
