#!/opt/anaconda/bin/python

'''exportable_tables.py - convert the HDF5 GDELT data to regular tables'''

import pandas as pd

old = pd.HDFStore('GDELT-compressed.h5')
new = pd.HDFStore('GDELT-minimal.h5', complevel=9, complib='blosc')

for df in old.select('reduced', iterator=True):
    print df.Day.iloc[-1]
    new.append('reduced', df, data_columns=True,
                min_itemsize={'EventCode': 4,
                                'QuadCategory': 1,
                                'Actor1Code': 18,
                                'Actor2Code': 18})
    # Didn't have this on the current run...
    # new.flush()
