"""
Author: Roberto Sanchez
This filter is based on instrumentation theory and the Central Limit Theorem
See more information on:
https://www.isixsigma.com/tools-templates/control-charts/a-guide-to-control-charts/
The six sigma approach has  USL (upper specification limit) which is a value that is over UCL (Upper Control Limit)
and the LSL (lower specification limit) which is a value below the LCL (Lower Control Limit)
"""
import rs_common_framework_v4 as rs
from pymongo import MongoClient
import pandas as pd
import time

MONGODB_HOST = 'localhost'  #'192.168.6.132'
MONGODB_PORT = 27017
six_sigma_factor = 3.5


def main():
    # Get default values
    args = rs.__get_parameters('filter')
    options = vars(args)
    db_name = options['db']
    collection_metadata = options['mt']
    collection_series = options['sr']
    collection_register = options['save']
    collection_filter = options['fi']
    filter_factor = options['ft']

    query_tag = options['q']
    query_time = options['t']
    # FILE = options['file'] #for saving the results
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
    collection_filter = connection[db_name][collection_filter]
    collection_register = connection[db_name][collection_register]

    # keep counter of number of detections
    detections = {}

    for tag in tags:
        ''' Process data for each Tag
        '''
        detections[tag] = 0
        df = rs.get_tag_values(collection_series, query_time, tag, series_format='DF_idx')
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        df[tag].dropna(inplace=True)
        global_mean = (df[tag].quantile(q=0.3) + df[tag].quantile(q=0.5) +
                       + df[tag].quantile(q=0.8))/3
        global_std = ((df[tag].quantile(q=0.2)-global_mean)**2 + (df[tag].quantile(q=0.4)-global_mean)**2
                      + (df[tag].quantile(q=0.6) - global_mean) ** 2
                      + (df[tag].quantile(q=0.8)-global_mean)**2)**0.5

        u_mean = global_mean
        u_std = global_std

        # collection.copyTo("Filter_Data")

        start_time = df.index[0]
        end_time = df.index[-1]
        step = end_time - start_time
        step_days = int(step.days/2)
        freq = str(step_days) + 'd'
        delta_step = pd.DateOffset(days=step_days-1)
        delta_24h = pd.DateOffset(hours=24)
        periods = pd.date_range(start_time, end_time, freq=freq)

        print("\n--- Processing: \t", tag)
        print("from: ", start_time, " to ", end_time)

        for t in periods:
            t_fin = t + delta_step + delta_24h
            # inspected day
            df_raw = df.loc[t:t_fin].copy()
            # getting statistics
            u_mean = (u_mean + df_raw[tag].quantile(q=0.2) + df_raw[tag].quantile(q=0.5) +
                           + df_raw[tag].quantile(q=0.8)) / 4
            u_std = ((df_raw[tag].quantile(q=0.2) - u_mean) ** 2 + (df_raw[tag].quantile(q=0.4) - u_mean) ** 2
                          + (df_raw[tag].quantile(q=0.6) - u_mean) ** 2
                          + (df_raw[tag].quantile(q=0.8) - u_mean) ** 2) ** 0.5

            u_std_ini = u_std
            ucl = u_mean + filter_factor * u_std
            lcl = u_mean - filter_factor * u_std
            df_raw = df_raw[(df_raw[tag] > lcl) & (df_raw[tag] < ucl)]
            u_mean = (u_mean + df_raw[tag].mean())/2
            u_std = (u_std + df_raw[tag].std())/2

        u_max = df[tag].max()
        u_min = df[tag].min()

        # based on the Central Limit Theorem
        # UCL is u_mean + 3.0*std
        # LCL is u_mean - 3.0*std


        # Get the new mean
        # exclude values that are not in the Central Limit Stripe
        cls_values = df[(df[tag] > lcl) & (df[tag] < ucl)]
        if not cls_values.empty:
            u_mean = cls_values[tag].mean()
            u_std = cls_values[tag].std()

        up_ucl = u_mean + six_sigma_factor * u_std
        low_lcl = u_mean - six_sigma_factor * u_std

        # Register the changes
        # saving the new values in the register
        # Values that are outside of the Central Limit Stripe
        # We make equal to: u_mean (+/-) f*std
        # print t,t_fin
        # print "True", u_std_ini,u_max-u_min,"=" , 2*u_std_ini / (u_max-u_min)
        # print "other", u_mean, u_max-u_min,"=" , (u_max-u_min) / u_std_ini

        df_cls = df.copy()

        if (u_std_ini > (0.15 / 2) * (u_max - u_min)) & (u_std_ini * u_std > 0.1):

            mask1 = (df_cls[tag] > up_ucl) | (df_cls[tag] < low_lcl)
            df_register = df_cls.loc[mask1].copy()

            # above up_UCL, below low_LCL
            mask1 = df_register[tag] > up_ucl
            mask2 = df_register[tag] < low_lcl
            df_up_ucl_register = df_register.loc[mask1]
            df_low_lcl_register = df_register.loc[mask2]
            df_up_ucl_register['new_value'] = up_ucl
            df_low_lcl_register['new_value'] = low_lcl

            if not df_up_ucl_register.empty:
                list_timestamp = [str(x) for x in df_up_ucl_register.index.tolist()]
                list_epoch = [time.mktime(time.strptime(str(x), "%Y-%m-%d %H:%M:%S")) for x in list_timestamp]
                df_up_ucl_register['timestamp'] = list_timestamp
                df_up_ucl_register['epoch'] = list_epoch
                df_up_ucl_register['tagname'] = tag
                df_up_ucl_register['mean'] = u_mean
                df_up_ucl_register['std'] = u_std
                df_up_ucl_register['up_UCL'] = up_ucl
                records = df_up_ucl_register.transpose().to_dict()
                list_records = records.copy()
                for x in list_records:
                    collection_register.insert(records[x])
                    detections[tag] += 1
                    epoch = records[x]['epoch']
                    collection_filter.find_and_modify(
                        query={'epoch': epoch, 'timestamp': records[x]['timestamp']},
                        update={"$set": {tag: records[x]['new_value']}})

            if not df_up_ucl_register.empty:
                list_timestamp = [str(x) for x in df_up_ucl_register.index.tolist()]
                list_epoch = [time.mktime(time.strptime(str(x), "%Y-%m-%d %H:%M:%S")) for x in list_timestamp]
                df_up_ucl_register['timestamp'] = list_timestamp
                df_up_ucl_register['epoch'] = list_epoch
                df_up_ucl_register['tagname'] = tag
                df_up_ucl_register['mean'] = u_mean
                df_up_ucl_register['std'] = u_std
                df_up_ucl_register['up_UCL'] = up_ucl
                records = df_up_ucl_register.transpose().to_dict()
                list_records = records.copy()
                for x in list_records:
                    collection_register.insert(records[x])
                    detections[tag] += 1
                    epoch = records[x]['epoch']
                    collection_filter.find_and_modify(
                        query={'epoch': epoch, 'timestamp': records[x]['timestamp']},
                        update={"$set": {tag: records[x]['new_value']}})

            if verbose:
                print(df_register.transpose())

        print("Number of Detections:\n", detections[tag])

    detections['document'] = 'statistics'
    collection_register.insert_one(detections)

if __name__ == "__main__":
    main()
