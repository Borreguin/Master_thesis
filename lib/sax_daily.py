# Calculates statistics
# Author: Roberto Sanchez
# Personal library.
# 1. Obtain the tag names
# 2. Create a line time
# 3. Compute the statistics
# 16/11/2016
##
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import math as m
import rs_common_framework_v4 as rs
import pysax

# TODO: change the name of the collection: Source---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'comfort_room'  # 'normalized_time_series', 'comfort_room'
collection_metadata = 'metadata'

# TODO: change the name of the collection: Final collection (result)---------
collection_sax = 'comfort_room_sax' # 'sax_daily', 'comfort_room_sax'

projection = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables
period = '1D'  # '1M'

# TODO: change number of bins for SAX: ---------
# number of divisions) for SAX representation
sax_nbins_size = 4              # 4 is the final value
sax_alphabet = "uc"           # "LbaH" is the final alphabet
sax = pysax.SAXModel(nbins=sax_nbins_size, alphabet=sax_alphabet)

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_sax = connection[MONGODB_DB][collection_sax]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    compute_sax_words(tag_list)
    return "End of this script"


###############################################################

def compute_sax_words(tag_list):

    offset_1h = pd.DateOffset(hours=1)
    time_line = rs.get_timeline(collection_series, time_query, freq=period)
    time_line += [pd.Timestamp(time_line[-1]) + pd.DateOffset(days=1)]
    start_time = time_line[0]

    for end_time in time_line[1:]:
        t_limit = pd.Timestamp(end_time) - offset_1h
        t_init = start_time + " 00:00:00"
        print('-- processing: ', t_init, 'to', t_limit)
        date_query = rs.dict_range_time(t_init, t_limit)
        df_x = rs.get_tag_values(collection_series, date_query, tag_list, series_format='DF_t')
        tag_list = list(df_x.columns)
        for tag in tag_list:

            df = pd.DataFrame()
            df[tag] = pd.to_numeric(df_x[tag], errors='coerce')
            df = df.dropna()
            word = sax.symbolize_whiten_window(df[tag])

            query_filter = {'timestamp': start_time,
                            'epoch': time.mktime(time.strptime(str(start_time), "%Y-%m-%d"))
                            }

            collection_sax.find_one_and_update(
                filter=query_filter,
                update={"$set": {
                    tag: word
                }},
                upsert=True,
            )
        start_time = end_time


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
