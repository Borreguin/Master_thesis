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
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_statistics = 'statistics_daily'
collection_general_statistics = 'statistics_general'

projection = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables
period = '1D'  # '1M'

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_statistics = connection[MONGODB_DB][collection_statistics]
collection_general_statistics = connection[MONGODB_DB][collection_general_statistics]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    compute_statistics(tag_list)
    return "End of this script"


###############################################################

def compute_statistics(tag_list):
    tag_list = ['tre200b0', 'sre000b0', 'V032_outdoor_temp']
    timestamp = rs.get_tag_values(collection_series, time_query, projection, series_format='DF_t')
    offset_1h = pd.DateOffset(hours=1)
    start_time = pd.Timestamp(timestamp.index[0]._date_repr)
    end_time = pd.Timestamp(timestamp.index[-1] + offset_1h)
    time_line = pd.date_range(start_time, end_time, freq=period)

    i = 0
    for end_time in time_line[1:]:

        if i%100 == 0:
            print('-- processing: {0} to {1}. \n\t processing next 100 days'.format(start_time, end_time - offset_1h))
        i += 1
        date_query = rs.dict_range_time(start_time, end_time - offset_1h)
        df_x = rs.get_tag_values(collection_series, date_query, tag_list, series_format='DF_t')
        tag_list = list(df_x.columns)
        for tag in tag_list:

            df = pd.DataFrame()
            df[tag] = pd.to_numeric(df_x[tag], errors='coerce')
            df[tag].dropna()
            st_projection = {'mean': True}
            std_values = collection_general_statistics.find_one({'tagname': tag}, st_projection)
            u_value = std_values['mean']

            statistics = df[tag].describe()
            statistics = validate_dictionary(statistics.T.to_dict())
            statistics['dev_u'] = deviation_mean(np.array(df[tag]), u_value)

            # NOTE: r_factor can be multiplied by the mean value (u_value)
            # to gain more information
            statistics['r_factor_u'] = u_value * r_factor(np.array(df[tag]), u_value)
            statistics['r_factor'] = u_value * r_factor(np.array(df[tag]), df[tag].mean())

            # The following features could create noise
            # since they are dispersed in some cases
            # (used for experiments, but not so much effectively in the cluster processing)
            statistics['(max-min)*std'] = (df[tag].max() - df[tag].min()) * (df[tag].std())

            statistics['r_factor_st'] = r_factor(np.array(df[tag].iloc[0:6]), df[tag].iloc[6])
            statistics['r_factor_ed'] = r_factor(np.array(df[tag].iloc[18:23]), df[tag].iloc[18])
            statistics['max_st'] = df[tag].iloc[0:6].max()
            statistics['max_ed'] = df[tag].iloc[18:23].max()
            statistics['max_me'] = df[tag].iloc[7:18].max()

            statistics['min_st'] = df[tag].iloc[0:6].min()
            statistics['min_ed'] = df[tag].iloc[18:23].min()
            statistics['min_me'] = df[tag].iloc[7:18].min()



            query_filter = {'tagname': tag, 'timestamp': start_time._date_repr,
                            'epoch': time.mktime(time.strptime(str(start_time._date_repr), "%Y-%m-%d"))
                            }
            aux = collection_statistics.find_one(query_filter)
            if aux is None:
                register = query_filter
            else:
                register = aux

            register.update(statistics)
            collection_statistics.find_one_and_replace(
                filter=query_filter,
                replacement=register,
                upsert=True
            )

        start_time = end_time

def validate_dictionary(to_validate):
    if not np.isnan(sum(to_validate.values())):
        return  to_validate
    else:
        for d in to_validate:
            if np.isnan(to_validate[d]):
                to_validate[d] = -1


def deviation_mean(x_array, u_value):
    r = 0
    x_array = x_array[~np.isnan(x_array)]
    if len(x_array) > 0:
        for x in x_array:
            r += (x - u_value) ** 2
        return (r / len(x_array)) ** 0.5
    else:
        return -1


def r_factor(x_array, u_value):
    N = x_array.size
    #print(x_array)
    if N > 0:
        p = 1
        n = 1
        f = 1.0 / N
        for x, i in zip(x_array, range(N)):
            if x > u_value:
                p += 2 ** (i * f)
            else:
                n += 2 ** (i * f)
        return ((p - n) / N) * (np.log(2))
    else:
        return -1





###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
