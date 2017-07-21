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

import time
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_normalized = 'normalized_daily'
collection_general_statistics = 'statistics_general'

projection = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables
period = '1D'  # '1M'

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_normalized = connection[MONGODB_DB][collection_normalized]
collection_general_statistics = connection[MONGODB_DB][collection_general_statistics]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    compute_normalization(tag_list)
    return "End of this script"


###############################################################

def compute_normalization(tag_list):
    for tag in tag_list:

        print('-- processing: ', tag)

        df = rs.get_tag_values(collection_series, time_query, tag, series_format='DF_t')
        st_projection = {'mean': True, 'std': True}
        std_values = collection_general_statistics.find_one({'tagname': tag}, st_projection)
        u_value = std_values['mean']
        std_value = std_values['std']
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        df[tag].dropna(inplace=True)
        if std_value > 0:
            df[tag] = (df[tag] - u_value) / std_value
        else:
            print("Observe this variable: {0} because his standard deviation is {1}".format(tag, std_value))

        for date in df.index:
            save_normalization(date, df.loc[date], tag)

    return True


def save_normalization(timestamp, value, tag):
    query_filter = {'timestamp': str(timestamp),
                    'epoch': time.mktime(time.strptime(str(timestamp), "%Y-%m-%d %H:%M:%S"))
                    }

    collection_normalized.find_one_and_update(
        filter=query_filter,
        update={'$set': {tag: float(value)}},
        upsert=True
    )
    return "!Done"


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
