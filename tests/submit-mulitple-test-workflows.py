#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig
import numpy as np

if __name__ == '__main__':
    runs = np.arange(19000, 19050)
    runs = runs[runs != 19012]
    configs = [DBConfig(r) for r in runs]
   #configs = [DBConfig(2021),
   #            DBConfig(2022, priority=100),
   #            DBConfig(2023)]
    outsource = Outsource(configs)
    outsource.submit_workflow()
