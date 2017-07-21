"""
Author: Roberto Sanchez
This filter is based on instrumentation theory and the Central Limit Theorem
See more information on:
https://www.isixsigma.com/tools-templates/control-charts/a-guide-to-control-charts/
"""
import rs_common_framework_v4 as rs
from pymongo import MongoClient
import pandas as pd
import time

MONGODB_HOST = 'localhost'  #'192.168.6.132'
MONGODB_PORT = 27017


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

    query_tag = options['q']
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
        #df[tag] = pd.to_numeric(df[tag], errors='coerce')
        #df[tag].dropna(inplace=True)

        start_time = df.index[0]
        end_time = df.index[-1]
        freq = '1d'
        delta_24h = pd.DateOffset(hours=24)
        delta_7d = pd.DateOffset(days=7)
        periods = pd.date_range(start_time, end_time, freq=freq)

        print("\n--- Processing: \t", tag)
        print("from: ", start_time, " to ", end_time)

        for t in periods:
            t_fin = t + delta_24h
            # inspected day
            df_raw = df.loc[t:t_fin].copy()
            # getting statistics
            df_past = df.loc[t - delta_7d:t_fin - delta_7d]
            df_future = df.loc[t + delta_7d:t_fin + delta_7d]

            if df_past.size > 5 and df_future.size > 5:
                u_mean = (df_raw[tag].mean() + df_past[tag].mean() + df_future[tag].mean()) / 3
                u_std = (df_raw[tag].std() + df_past[tag].std() + df_future[tag].std()) / 3
            elif df_past.size > 5:
                u_mean = (df_raw[tag].mean() + df_past[tag].mean()) / 2
                u_std = (df_raw[tag].std() + df_past[tag].std()) / 2
            elif df_future.size > 5:
                u_mean = (df_raw[tag].mean() + df_future[tag].mean()) / 2
                u_std = (df_raw[tag].std() + df_future[tag].std()) / 2
            else:
                u_mean = df_raw[tag].mean()
                u_std = df_raw[tag].std()

            u_std_ini = u_std
            u_max = df_raw[tag].max()
            u_min = df_raw[tag].min()
            df_register = pd.DataFrame()
            df_ucl_register = pd.DataFrame()
            df_lcl_register = pd.DataFrame()

                # based on the Central Limit Theorem
                # UCL is u_mean + 3.0*std
                # LCL is u_mean - 3.0*std
            ucl = u_mean + filter_factor * u_std
            lcl = u_mean - filter_factor * u_std

            # Registering the detected values
            # Values that are outside of the Central Limit Stripe
            # print t,t_fin
            # print "True", u_std_ini,u_max-u_min,"=" , 2*u_std_ini / (u_max-u_min)
            # print "other", u_mean, u_max-u_min,"=" , (u_max-u_min) / u_std_ini
            df_cls = df.loc[t:t_fin].copy()

            if (u_std_ini > (0.15 / 2) * (u_max - u_min)) & (u_std_ini * u_std > 0.1):
                # above UCL, below LCL
                mask1 = (df_cls[tag] > ucl) | (df_cls[tag] < lcl)
                df_register = df_cls.loc[mask1].copy()

                # above UCL, below LCL
                mask1 = df_register[tag] > ucl
                mask2 = df_register[tag] < lcl
                df_ucl_register = df_register.loc[mask1]
                df_lcl_register = df_register.loc[mask2]

                # these values correspond to the ranges [ucl,up_ucl]
                # and the range [low_lcl, lcl]
            if not df_ucl_register.empty:
                list_timestamp = [str(x) for x in df_ucl_register.index.tolist()]
                list_epoch = [time.mktime(time.strptime(str(x), "%Y-%m-%d %H:%M:%S")) for x in list_timestamp]
                df_ucl_register['timestamp'] = list_timestamp
                df_ucl_register['epoch'] = list_epoch
                df_ucl_register['tagname'] = tag
                df_ucl_register['mean'] = u_mean
                df_ucl_register['std'] = u_std
                df_ucl_register['UCL'] = ucl
                records = df_ucl_register.transpose().to_dict()
                list_records = records.copy()
                for x in list_records:
                    collection_register.insert(records[x])
                    detections[tag] += 1

            if not df_lcl_register.empty:
                list_timestamp = [str(x) for x in df_lcl_register.index.tolist()]
                list_epoch = [time.mktime(time.strptime(str(x), "%Y-%m-%d %H:%M:%S")) for x in list_timestamp]
                df_lcl_register['timestamp'] = list_timestamp
                df_lcl_register['epoch'] = list_epoch
                df_lcl_register['tagname'] = tag
                df_lcl_register['mean'] = u_mean
                df_lcl_register['std'] = u_std
                df_lcl_register['LCL'] = lcl
                records = df_lcl_register.transpose().to_dict()
                list_records = records.copy()
                for x in list_records:
                    collection_register.insert(records[x])
                    detections[tag] += 1

                if verbose:
                    print(df_register.transpose())

        print("Number of Detections:\n", detections[tag])

    print("\n", options)
    # save statistic of detection

    detections['document'] = 'statistics'
    collection_register.insert_one(detections)

if __name__ == "__main__":
    main()
