#!/opt/anaconda/bin/python

'''max_widths.py - figure out max width for various text fields'''

import pandas as pd

store = pd.HDFStore('GDELT-compressed.h5')

max_max = pd.Series({'Actor1Code': 0, 'Actor2Code': 0, 'EventCode': 0, 'QuadCategory': 0})

for df in store.select('reduced',
                       columns=['Actor1Code', 'Actor2Code', 'EventCode', 'QuadCategory'],
                       iterator=True):
    curr_max = df.applymap(len).max()
    max_max = pd.concat([curr_max, max_max],axis=1).max(axis=1)
    print max_max
