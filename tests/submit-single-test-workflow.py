#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig

if __name__ == '__main__':
    configs = [DBConfig(16854)]
    outsource = Outsource(configs)
    outsource.submit_workflow()
