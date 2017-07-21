# Author: Roberto Sanchez
from __future__ import print_function
import rs_common_framework_v4 as rs
from sklearn.externals import joblib
import pandas as pd
# import collections
import numpy as np
import math
import warnings

from hmmlearn.hmm import GaussianHMM
from pymongo import MongoClient

# import sys
# sys.path.append('../../lib')

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

print(__doc__)

# DB parameters
# Connecting to the DB Mongo
db_name = "project_db"
# Select category to test
selected_category = 'C'
selected_vector = 'S_vector'
label_name = 'seasonal_label'

# filter for type of day
day_type = ['weekend', 'working_day', 'holiday']  # 'holiday' was not significant
# vector collection, is the collection where the vector R for each day is in
collection_source = "C_1-C_2-C_4"
collection_to_label = 'statistics_daily'

# HMM model Parameters
hmm_path_model = '../HMM_models/Final_models/'
file_names = ['named_hmm_S_vector_C_C_1-C_2-C_4_5.pkl',
              'named_hmm_S_vector_C_C_1-C_2-C_4_10.pkl',
              'named_hmm_S_vector_C_C_1-C_2-C_4_15.pkl']

connection = MongoClient('localhost')  # "192.168.6.132"
collection_metadata = connection[db_name]["metadata"]
collection_series = connection[db_name]["filtered_time_series"]
collection_source = connection[db_name][collection_source]
collection_to_label = connection[db_name][collection_to_label]


# Read the HMM model
def read_HMM_model(path_model, name):
    try:
        model = joblib.load(path_model + name)['model']
        name_states = joblib.load(path_model + name)['hidden_state_name']
        print("\nThe Model {0} has {1} hidden states".format(name, model.n_components))

    except FileNotFoundError:
        model = None
        name_states = None
        print('The model was not found')
    return model, name_states


def save_results(df_result, hidden_state_labels):
    df_to_save = pd.DataFrame(df_result['timestamp'], columns=['timestamp'])

    # the most confident Hidden Markov Model in file_names[-1]
    df_to_save['hidden_state_label'] = df_result[file_names[-1]]

    N = len(df_to_save.index)
    counter2 = 0
    total_reg = 0
    for idx in list(df_to_save.index):
        name_state_list = list(df_result.loc[idx, file_names])

        n = len(set(name_state_list))

        # if all the states are equal (n==1):
        if n == 1:
            df_to_save.loc[idx, 'hidden_state_label'] = name_state_list[0]
            counter2 += 1
        else:
            for name in name_state_list:
                max_n = name_state_list.count(name)

                if max_n > n / 2.0:
                    df_to_save.loc[idx, 'hidden_state_label'] = name

        total_reg += save_register(df_to_save.loc[idx, 'timestamp'], df_to_save.loc[idx, 'hidden_state_label'])

    print("\nFinally, {0} registers were updated".format(N))
    print("\nThe models \n{0} \nhave a coincidence of {1}\n".format(file_names, counter2 / N * 100))


def save_register(timestamp, label):
    query_time = {'timestamp': timestamp}
    cursor = list(collection_to_label.find(query_time,{'_id':True}))
    ct = 0
    for c in cursor:
        new_query = {'_id':c['_id']}

        collection_to_label.find_and_modify(
            query=new_query,
            update={"$set": {label_name: label}},
            upsert=True,

        )
        ct += 1
    return ct


def get_dates():
    time_query = {'timestamp': {'$exists': True}}
    projection_time = dict()
    time_df = rs.get_tag_values(collection_series, time_query, projection_time, series_format='DF_t')

    start_time = time_df.index[0]._date_repr
    end_time = time_df.index[-1]

    timeline = pd.date_range(start_time, end_time, freq='1D')
    timeline = [x._date_repr for x in timeline]

    df = pd.DataFrame(timeline, columns=['timestamp'])
    return df


def main():
    # Read the HMM model

    df_result = get_dates()

    for name in file_names:
        model, hidden_state_labels = read_HMM_model(hmm_path_model, name)
        if model is None:
            return False

        timeline = df_result['timestamp']

        # get the samples for the HMM model
        filter_query = {"breakout_group": selected_category,
                        'day_type': {'$in': day_type}}

        df_norm = rs.get_samples_norm_vector(collection_source, timeline, filter_query, selected_vector)
        X = np.array(df_norm)
        dates = df_norm.index

        print("\nAnalysis from {0} to {1}, {2} samples".format(dates[0], dates[-1], len(dates)))
        print('\tThe vector of samples has shape {0}'.format(X.shape))

        # Infering the hidden states from the samples
        try:
            hidden_state_list = model.predict(X)
            print("\t{0} hidden states were infered from the model {1}".format(len(hidden_state_list), name))
        except:
            print("--> The shape of the samples are not compatible with {0}".format(name))
            return False

        # save results in dataframe
        if model.n_components == len(hidden_state_labels):

            ind_id = range(model.n_components)
            df = pd.DataFrame(dates, columns=['timestamp'])
            df['hidden_state'] = hidden_state_list
            for idx, hs in zip(ind_id, hidden_state_labels):
                mask = (df['hidden_state'] == idx)
                dates_indx = df.loc[mask, 'timestamp']
                mask2 = df_result['timestamp'].isin(dates_indx)
                df_result.loc[mask2, name] = hs

        else:
            print("The number of hidden states does not match with the number of labels")
            return False

    save_results(df_result, hidden_state_labels)

    return True


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    print("---->The labeling process was done: {0}".format(main()))
    print('\nEnd of the script')
