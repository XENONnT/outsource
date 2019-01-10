#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.Config import Config, ConfigDB

if __name__ == '__main__':
    config = ConfigDB(49)
    outsource = Outsource(config)
    #outsource = Outsource(detector = 'tcp', name = '181203_0841', force_rerun = True, update_run_db = False)
    outsource.submit_workflow()
