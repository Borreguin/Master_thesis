"""
Author: Roberto Sanchez
This filter is based on instrumentation theory and the Central Limit Theorem
The inter quartile analysis is done by using a linear regression
The limits are found when the interpolation is values:
    1.10 -> for upper limit
   -0.10 -> for lower limit
Each one is 15% below or above the expected limits
"""
import rs_common_framework_v4 as rs
from pymongo import MongoClient
import pandas as pd
import numpy as np


MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017

low_quartile = np.linspace(0.01, 0.25, 25)  # lower quartile
up_quartile = np.linspace(0.75, 0.99, 25)   # upper quartile
high_limit = 3.0  # This defines the high limit using the linear regression when x=2.0
low_limit = -2.0  # This defines the low limit using the linear regression when x=-1.0


def main():
    # Get default values
    args = rs.__get_parameters('filter')
    options = vars(args)
    db_name = options['db']
    collection_metadata = options['mt']
    collection_series = options['sr']
    collection_register = options['save']
    collection_filter = options['fi']

    query_tag = options['q']
    query_time = options['t']
    # FILE = options['file'] #for saving the results
    verbose = options['v']
    # Connect to server, getting: data base and collection:
    connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
    collection_metadata = connection[db_name][collection_metadata]

    print("Reading from \n{0} \n-- Saving filtered series in \n{1} \n-- Saving filter registers in \n{2}".format(collection_series, collection_filter, collection_register))

    # USING METADATA BASE:
    # select tagname of each data series
    tags = rs.get_tag_names(collection_metadata, query_tag)
    # The time query uses regular expressions
    query_time = rs.dictionary_time(query_time)

    # USING DATA SERIES:
    collection_series = connection[db_name][collection_series]
    collection_filter = connection[db_name][collection_filter]
    collection_register = connection[db_name][collection_register]

    # keep counter of number of detections
    detections = {}
    up_ucl_values = {}
    low_lcl_values = {}

    for tag in tags:
        ''' Process data for each Tag
        '''
        detections[tag] = 0
        df = rs.get_tag_values(collection_series, query_time, tag, series_format='DF_idx')
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        df_raw = df[tag].fillna(df[tag].interpolate())  # having a copy to work with

        up_ucl = rs.get_limit_by_quartile(df_raw, up_quartile, limit=high_limit)
        low_lcl = rs.get_limit_by_quartile(df_raw, low_quartile, limit=low_limit)

        # saving general statistics about the variable
        p50 = df_raw.quantile(q=0.5)
        up_ucl_values[tag] = up_ucl
        low_lcl_values[tag] = low_lcl

        # above up_UCL, below low_LCL
        mask1 = df_raw > up_ucl
        mask2 = df_raw < low_lcl
        df_up_ucl_register = pd.DataFrame(df_raw.loc[mask1], columns=[tag])
        df_low_lcl_register = pd.DataFrame(df_raw.loc[mask2], columns=[tag])

        # UPPER LIMIT
        # -----------------------------------------------------------------------------
        num_detections, list_records = save_results(collection_filter, collection_register,
                                                df_up_ucl_register, tag, p50, up_ucl)
        detections[tag] += num_detections
        if verbose:
            print(list_records)
        # ------------------------------------------------------------------------------

        # LOWER LIMIT
        # -----------------------------------------------------------------------------
        num_detections, list_records = save_results(collection_filter, collection_register,
                                                df_low_lcl_register, tag, p50, low_lcl)
        detections[tag] += num_detections
        if verbose:
            print(list_records)
        # ------------------------------------------------------------------------------

        print('--' + tag)
        print("Number of Detections:\n", detections[tag])

    detections['document'] = 'description'
    detections['low_LCL_values'] = low_lcl_values
    detections['up_UCL_values'] = up_ucl_values
    collection_register.insert_one(detections)
    print("Reading from \n{0} \n-- Saving filtered series in \n{1} \n-- Saving filter registers in \n{2}".format(
        collection_series, collection_filter, collection_register))


def save_results(collection_filter, collection_register, df_register, tag, p50, limit_to_save):

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
                'new_value': limit_to_save,
                'old_value': x[tag],
                'p50': p50
            }

            collection_register.find_and_modify(
                query=filter_query,
                update={"$set": to_save},
                upsert=True
            )
            detections += 1
            # -----------------------------------------------------

            # Making the changes in the filtered time series
            collection_filter.find_and_modify(
                query=filter_query,
                update={"$set": {tag: limit_to_save}})

    return detections, records_list

if __name__ == "__main__":
    main()
