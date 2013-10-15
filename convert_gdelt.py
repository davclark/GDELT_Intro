#!/opt/anaconda/bin/python

'''convert_gdelt.py - convert the reduced GDELT dataset from text to HDF5

These text files are split up by year. I can't find a source for the exact
format of these columns, but this is the full list:

 - Day - yyyymmdd
 - Actor1Code - vaguely described, first 3 chars are most critical, other codes
                may be present
 - Actor2Code - same as Actor1Code
 - EventCode - Numeric code, but leading '0' is relevant, keep as text
 - QuadCategory - Numeric code, but generally small (and not metrical /
                  ordinal). Also keep as text.
 - GoldsteinScale - Floating point
 - Actor1Geo_Lat
 - Actor1Geo_Long
 - Actor2Geo_Lat
 - Actor2Geo_Long
 - ActionGeo_Lat
 - ActionGeo_Long

 Sadly, some files were badly formatted. In particular, files after 2006 have a
 number of repeats of the header column. I removed these manually. Could script
 it for a more public distribution of this approach.
'''


from functools import partial
from datetime import date, MINYEAR
import pandas as pd

PATH = 'GDELT.1979-2012.reduced/'

# We don't need a "robust" date parser, as we know the format, so we pass in
# this custom parser (actually *too* robust, as it will catch *all* exeptions!).
# I verified identical behavior on the 1979 data
# The speedup on my 2009 MacBook Air goes from 32 seconds to under 3 to read the 1979 file
def parse_date(date_string):
    # Just convert to monthly data
    try:
        year = int(date_string[:4])
        month = int(date_string[4:6])
        day = int(date_string[6:8])
        return date(year, month, day)
    except:
        print 'funny date string: ', date_string
        return date(MINYEAR,1,1)

# This is the "standard" way to use generic_parser
gp = pd.io.date_converters.generic_parser
parser = partial(gp, parse_date)

# Given that these files are huge, we are using compression from the get-go
# You can modify compression settings with ptrepack (included with pytables)
store = pd.HDFStore('GDELT.h5', complevel=5, complib='blosc')
# for year in range(1979, 2013):
for year in range(1979, 2013):
    print year
    # We're using textual column labels here, as it's both more robust and more
    # readable
    data = pd.read_csv(PATH + str(year) + ".reduced.txt", sep='\t',
                       dtype={'EventCode': str, 'QuadCategory': str},
                       parse_dates=['Day'], date_parser=parser,
                       min_itemsize={'EventCode': 4, 'QuadCategory': 1,
                                     'Actor1Code': 18, 'Actor2Code': 18} )

    # We aren't actually appending here, but this is how you create data_columns
    store.append('r' + str(year), data, data_columns=True)
