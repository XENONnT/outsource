#!/usr/bin/env python3

from Outsource import Outsource

if __name__ == '__main__':
    outsource = Outsource(detector = 'tcp', name = '181203_0841', force_rerun = True, update_run_db = False)
    outsource.submit_workflow()
    
