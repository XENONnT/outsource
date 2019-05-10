#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig

if __name__ == '__main__':
    # 16854
    configs = [DBConfig(10635)]
    outsource = Outsource(configs)
    outsource.submit_workflow()
