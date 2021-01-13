#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig
import numpy as np

if __name__ == '__main__':
    runs = np.arange(10002, 10005)

    configs = [DBConfig(r, straxen_version='0.14.0',
                        strax_context='xenonnt_online') for r in runs]
    outsource = Outsource(configs)
    outsource.submit_workflow()
