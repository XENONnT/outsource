#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig

if __name__ == '__main__':
    configs = [DBConfig(2023)]
    outsource = Outsource(configs)
    outsource.submit_workflow()
