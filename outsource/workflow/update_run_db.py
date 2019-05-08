#!/usr/bin/env python3

import sys

from pprint import pprint

from utilix import *

db = rundb.DB()
data = db.get_data(sys.argv[1])
for data_set in data:
    if 'type' in data_set and data_set['type'] == 'records':
        if data_set['checksum'] is None:
            data_set['checksum'] = 'None' 
        data_set['status'] = 'processing'
        db.update_data(sys.argv[1], data_set)

# trust, but verify...
data = db.get_data(sys.argv[1])
pprint(data)


