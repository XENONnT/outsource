#!/usr/bin/env python3
from outsource.Outsource import Outsource
from outsource.RunConfig import DBConfig
import numpy as np

if __name__ == '__main__':
    runs = [#10244, 10245, 10252, 10266, 10270, 10279, 
            10302, 10305, 10312, 10313, 10315, 10322, 
            10330, 10333, 10336, 10337, 10342, 10344, 
            #10345, 10346, 10354, 10355, 10357, 10363, 
            #10365, 10370, 10372, 10377, 10381, 10385, 
            #10391, 10410, 10440, 10443, 10459, 10462,
            #10467, 10472, 10473, 10474, 10476, 10479,
            #10483, 10490, 10734, 10735
            ]

    configs = [DBConfig(r) for r in runs]
    outsource = Outsource(configs)
    outsource.submit_workflow()
