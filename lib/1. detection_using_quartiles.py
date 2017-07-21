"""
Author: Roberto Sanchez
This filter is based on inter quartile analysis (i.e. quartiles 0-25th, 75-100th)
"""
import rs_common_framework_v4 as rs
from pymongo import MongoClient
import pandas as pd
import time
import numpy as np

MONGODB_HOST = 'localhost'  #'192.168.6.132'
MONGODB_PORT = 27017

low_quartile = np.linspace(0.01, 0.25, 15)  # lower quartile
up_quartile = np.linspace(0.75, 0.99, 15)   # upper quartile
high_limit = 1.0  # This defines the high limit using the linear regression when x=1.0
low_limit = 0  # This defines the low limit using the linear regression when x=0.0

query_tag = {'breakout_group' : {'$in': ['A', 'B', 'A_1', 'B_1', 'A_2', 'B_2'
                                   'A_3', 'B_3', 'A_4_1', 'A_4_2', 'B_4_1', 'B_4_2',
                                   'A_5_1', 'B_5_1', 'A_6_1', 'A_6_2', 'B_6_1', 'B_6_2']} }

def main():
    # Get default values
    args = rs.__get_parameters('filter')
    options = vars(args)
    db_name = options['db']
    collection_metadata = options['mt']
    collection_series = options['sr']
    collection_register = options['save']
    filter_factor = options['ft']
    # number_interactions = options['it']

    # query_tag = options['q']
    query_time = options['t']
    verbose = options['v']
    # Connect to server, getting: data base and collection:
    connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
    collection_metadata = connection[db_name][collection_metadata]

    # USING METADATA BASE:
    # select tagname of each data series
    tags = rs.get_tag_names(collection_metadata, query_tag)
    # The time query uses regular expressions
    query_time = rs.dictionary_time(query_time)

    # USING DATA SERIES:
    collection_series = connection[db_name][collection_series]
    collection_register = connection[db_name][collection_register]

    # keep counter of number of detections
    detections = {}
    for tag in tags:
        ''' Process data for each Tag
        '''
        detections[tag] = 0
        df = rs.get_tag_values(collection_series, query_time, tag, series_format='DF_idx')
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        df[tag] = df[tag].fillna(df[tag].interpolate())

        start_time = df.index[0]
        end_time = df.index[-1]
        freq = '1d'
        delta_24h = pd.DateOffset(hours=24)
        delta_7d = pd.DateOffset(days=7)
        delta_1h = pd.DateOffset(hours=1)
        periods = pd.date_range(start_time, end_time, freq=freq)

        print("\n--- Processing: \t", tag)
        print("from: ", start_time, " to ", end_time)

        for t in periods:
            t_fin = t + delta_24h

            # inspected day
            df_inspected_day = df.loc[t:t_fin].copy()
            # getting window of days
            df_raw = df_inspected_day.copy()

            t_day = t.dayofweek
            if 0 == t_day:
                delta = pd.DateOffset(days=1)
                df_raw = df_raw.append(df.loc[t_fin:t_fin + delta])

            elif 1 <= t_day <= 3:
                delta = pd.DateOffset(days=1)
                df_raw = df_raw.append(df.loc[t - delta:t])
                df_raw = df_raw.append(df.loc[t_fin + delta_1h: t_fin + delta])

            elif t_day == 4:
                delta = pd.DateOffset(days=1)
                df_raw = df_raw.append(df.loc[t - delta - delta_1h: t])

            else:
                df_raw = df_raw.append(df.loc[t - delta_7d:t_fin - delta_7d])

            # obtain the limits according the quartile analysis
            ucl = rs.get_limit_by_quartile(df_raw[tag], up_quartile, high_limit)
            lcl = rs.get_limit_by_quartile(df_raw[tag], low_quartile, low_limit)

            # Registering the detected values that are outside of the quartile analysis range
            # above UCL, below LCL
            df_ucl_register = df_inspected_day[df_inspected_day[tag] > ucl]
            df_lcl_register = df_inspected_day[df_inspected_day[tag] < lcl]

            n_detections_1, register_1 = save_results(collection_register,
                                                      df_ucl_register, df_inspected_day, tag, 'ucl', ucl)

            n_detections_2, register_2 = save_results(collection_register,
                                                      df_lcl_register, df_inspected_day, tag, 'lcl', lcl)

            detections[tag] += n_detections_1 + n_detections_2
            if verbose:
                print(register_1, register_2)

        print("Number of Detections:\n", detections[tag])

    print("\n", options)
    # save statistic of detection

    detections['document'] = 'statistics'
    collection_register.insert_one(detections)


def save_results(collection_register, df_register, df, tag, label, limit_to_save):

    detections = 0
    records_list = list()
    if not df_register.empty:
        timestamp_list = [str(x) for x in df_register.index.tolist()]
        epoch_list = [rs.epoch(x) for x in timestamp_list]
        df_register['epoch'] = epoch_list
        df_register['timestamp'] = timestamp_list
        df_register.index = timestamp_list
        records = df_register.transpose().to_dict()
        records_list = records.copy()
        for x in records_list.values():
            # Registering the changes in collection_register
            filter_query = {
                'timestamp': x['timestamp'],
                'epoch': x['epoch']
            }
            to_save = filter_query.copy()
            to_save[tag] = {
                label: limit_to_save,
                'mean': df[tag].mean(),
                'std': df[tag].std(),
                'max': df[tag].max(),
                'min': df[tag].min(),
                'tag_value': x[tag]
            }

            collection_register.find_and_modify(
                query=filter_query,
                update={"$set": to_save},
                upsert=True
            )
        detections += 1
            # -----------------------------------------------------

    return detections, records_list








if __name__ == "__main__":
    main()
