# Calculates the correlation matrix
# Author: Roberto Sanchez
# Personal library.
# 1. compute the correlation matrix
# 2. Save the factors that are bigger than the correlation factor (i.e. abs(correlation) > fc)
# 16/11/2016
##
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  #'192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_correlation_matrix = 'correlation_matrix'
time_query = {}
day_type = 'working_day'
projection = {'timestamp': 1}
fc = 0.1  # this allows to save only correlations that are bigger tha fc
# -------------------------------------------------


connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_correlation_matrix = connection[MONGODB_DB][collection_correlation_matrix]


##############################################################


def main():
    # TODO: Put here the function to run
    compute_by_subcategory()
    return "End of this script"


###############################################################


def compute_by_subcategory():
    query_dict = {}
    field = 'orientation'
    projection_dict = {field: True, '_id': False}
    subcategory_list = list(collection_metadata.find(query_dict, projection_dict))
    subcategory_list = [x[field] for x in subcategory_list]
    subcategory_list = list(set(subcategory_list))

    for sub_category in subcategory_list:
        tag_query = get_list_subcategory(sub_category, field)
        compute_matrix_correlation(tag_query)


def get_list_subcategory(sub_category, field):
    r_query = {}
    if sub_category == 'NE':
        r_query[field] = {'$in': ['NE', 'N', 'E', 'N/A']}
        return r_query
    if sub_category == 'SW':
        r_query[field] = {'$in': ['SW', 'S', 'W', 'N/A']}
        return r_query

    r_query[field] = {'$in': ['N/A', sub_category]}
    return r_query


def compute_matrix_correlation(tag_query):

    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    id_tags = range(len(tag_list))
    timestamp = rs.get_tag_values(collection_series, time_query, projection, series_format='DF_t')
    offset = pd.DateOffset(days=7)
    offset_1h = pd.DateOffset(hours=1)
    start_time = pd.Timestamp(pd.Timestamp(rs.get_next_weekday(timestamp.index[0], "Monday"))._date_repr)
    end_time = timestamp.index[-1]
    time_line = pd.date_range(start_time, end_time + offset, freq='7D')

    for end_time in time_line[1:]:

        print('-- processing: ', start_time, 'to', end_time - offset_1h)

        date_query = rs.dict_range_time(start_time, end_time - offset_1h)
        date_query['day_type'] = day_type
        df = rs.get_tag_values(collection_series, date_query, tag_list, series_format='DF_t')
        tag_list = list(df.columns)
        for tag in tag_list:
            df[tag] = pd.to_numeric(df[tag], errors='coerce')
        values = df.T.values
        df.dropna(inplace=True)
        print('VALUES:', values.shape)
        matrix = np.corrcoef(values)

        for idx in id_tags:
            query_filter = {'tagname': tag_list[idx], 'timestamp': start_time._date_repr,
                        'epoch': time.mktime(time.strptime(str(start_time._date_repr), "%Y-%m-%d"))
                        }
            aux = collection_correlation_matrix.find_one(query_filter)
            if aux is None:
                register = query_filter
                correlation_list = []
            else:
                register = aux
                correlation_list = register['correlation_list']
            for idy in id_tags:
                if idy != idx:
                    if matrix[idx][idy] != np.nan:
                        if abs(matrix[idx][idy]) > fc:
                            register[tag_list[idy]] = matrix[idx][idy]
                            correlation_list.append(tag_list[idy])

            register['correlation_list'] = list(set(correlation_list))
            collection_correlation_matrix.find_one_and_replace(
                filter=query_filter,
                replacement=register,
                upsert=True
            )

        print('matrix done')
        start_time = end_time


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
