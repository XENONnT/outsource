#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig

if __name__ == '__main__':
    config = DBConfig(2023)
    outsource = Outsource(config)
    outsource.submit_workflow()
