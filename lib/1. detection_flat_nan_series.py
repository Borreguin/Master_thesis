# Add a new feature in each record
# Author: Roberto Sanchez
# Personal library.
# 1. Detect flat days in a series Time (daily detection)
#    based on the flat percentile value, the result is saved in
#    flat detection collection
# 2. Detect non numeric values and NaN values (daily detection)
#    based on NaN values and absence of values
#
# 12/12/2016
#   Ex: with 80 flat percentile value -> at least 80% of the values are flat (equal)
#   Ex: with 100 flat percentile value -> all this day is a flat day
##

from pymongo import MongoClient
import pandas as pd
import rs_common_framework_v4 as rs
import time


# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  #'192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
dictionary_query = {}
# 80%, 90%
flat_percentile_values = [80, 90, 100]
flat_percentile_values = [x/100.0 for x in flat_percentile_values]
collection_flat_detection = 'detection_flat'
collection_NaN_detection = 'detection_nan'

# -------------------------------------------------

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_flat_detection = connection[MONGODB_DB][collection_flat_detection]
collection_NaN_detection = connection[MONGODB_DB][collection_NaN_detection]

##############################################################


def main():
    # TODO: Put here the function to run
    detect_flat_days()
    return "End of this script"
###############################################################


def detect_flat_days():
    """
    This function detects time series that have X flat percentile value.
    :return: Save json documents in the 'flat_detection_collection'
    """
    query_time = {}     # {} implies all the time range
    projection = {'timestamp': True}
    time_range = rs.get_tag_values(collection_series, query_time, projection, series_format='DF_t')
    start_day = time_range.index[0]
    end_day = time_range.index[-1]
    date_range = pd.date_range(start_day, end_day, freq='1D')
    tag_list = rs.get_tag_names(collection_metadata, '{}')
    projection = rs.projection(tag_list, _id=True)
    # initialization of variables:
    detections_nan = {}
    for tag in tag_list:
        detections_nan[tag] = 0

    detections_flat = {}
    # create empty dictionaries for each percentile value
    list_flat_detections = [detections_flat]*len(flat_percentile_values)

    for detections_flat in list_flat_detections:
        for tag in tag_list:
            detections_flat[tag] = 0

    for date in date_range:
        query_time = rs.dictionary_time(date._date_repr)
        df = rs.get_tag_values(collection_series, query_time, projection, del_id=False, series_format='DF_idx')
        print(df.info())
        idx = df[u'_id'][0]

        for tag in tag_list:
            df[tag] = pd.to_numeric(df[tag], errors='coerce')
            original_size = df[tag].size
            df[tag].dropna(inplace=True)
            new_size = df[tag].size
            if original_size != new_size:
                detections = original_size - new_size
                collection_NaN_detection.find_and_modify(query={'_id': idx},
                                                         update={"$set": {
                                                             tag: detections,
                                                             'timestamp': date._date_repr,
                                                             'epoch': time.mktime(time.strptime(
                                                                 str(date._date_repr), "%Y-%m-%d"))
                                                         }
                                                                 },
                                                         upsert=True)
                detections_nan[tag] = detections_nan[tag] + detections

            if not df[tag].empty:
                id_percentil = range(len(flat_percentile_values))
                for id_p, flat_percentile in zip(id_percentil, flat_percentile_values):
                    percentile_value = df[tag].quantile(q=flat_percentile)
                    min_value = df[tag].min()
                    if percentile_value == min_value:
                        collection_flat_detection.find_and_modify(
                            query={'percentil': flat_percentile,
                                   'timestamp': date._date_repr,
                                   'epoch': time.mktime(time.strptime(
                                       str(date._date_repr), "%Y-%m-%d"))
                                   },
                            update={"$set": {tag: percentile_value}},
                            upsert=True)
                        list_flat_detections[id_p][tag] += 1

    detections_nan['document'] = 'statistics'
    collection_NaN_detection.insert_one(detections_nan)

    for p, detections_flat in zip(flat_percentile_values, list_flat_detections):
        detections_flat['document'] = 'statistics'
        detections_flat['percentil'] = p
        detections_flat_aux = detections_flat.copy()
        collection_flat_detection.insert_one(detections_flat_aux)

main()
