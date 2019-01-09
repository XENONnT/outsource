#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.Config import Config, ConfigDB

if __name__ == '__main__':
    config = ConfigDB(49)
    outsource = Outsource(config)
    outsource.submit_workflow()
