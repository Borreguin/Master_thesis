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

parameters = {
    'seasonal_model':
        {
            'selected_category': 'C',
            'collection_source': ['C_1-C_2-C_4', 'C_1-C_4', 'C_1-C_2', 'C_1-C_2-C_3-C_4'],
            'vector_name': 'S_vector'
        },

    'interactional_model_A':
        {
            'selected_category': 'A',
            'collection_source': ['A-A_3-A_4_1-A_4_2-A_6_1-A_6_2',
                                  'A-A_3-A_4_1-A_4_2-A_5_1-A_5_2-A_6_1-A_6_2',
                                  'A-A_1-A_2-A_3-A_4_1-A_4_2-A_5_1-A_5_2-A_6_1-A_6_2',
                                  'A-A_1-A_3-A_4_1-A_4_2-A_5_1-A_5_2-A_6_1-A_6_2-A_6_3',
                                  'A-A_1-A_3-A_4_1-A_4_2-A_5_1-A_5_2-A_6_1-A_6_2',
                                  'A-A_1-A_3-A_4_1-A_4_2-A_5_1-A_6_1-A_6_2',
                                  'A-A_1-A_4_1-A_4_2-A_5_1-A_6_1-A_6_2',
                                  'A-A_4_1-A_4_2-A_5_2-A_6_3',
                                  'A-A_1-A_2-A_4_1-A_4_2-A_5_1-A_6_1-A_6_2',
                                  'A-A_1-A_2-A_4_1-A_4_2-A_5_1-A_6_1-A_6_2',
                                  'A-A_3-A_4_1-A_4_2-A_6_1-A_6_2-C_1-C_2'],
            'vector_name': 'R_vector'
        },
    'interactional_model_B':
        {
            'selected_category': 'B',
            'vector_name': 'R_vector'
        }
}
parameters['interactional_model_B']['collection_source'] = parameters['interactional_model_A']['collection_source']

# Select type of model to train ('interactional_model_A', 'interactional_model_B', 'seasonal_model')
model_pr = 'seasonal_model'
source_to_use = 0

# DB parameters
# Connecting to the DB Mongo
db_name = "project_db"
# Select category to test
selected_category = parameters[model_pr]['selected_category']
# filter for type of day
day_type = ['weekend', 'working_day', 'holiday']  # 'holiday' was not significant
# vector collection, is the collection where the vector R for each day is in
collection_source = parameters[model_pr]['collection_source'][source_to_use]

# Training Parameters
# k-fold cross-validation
k_size = 15
# maximun number of components to check
n_max = 5
# maximun number of components to check
n_min = 4
# vector to analyze
vector_name = parameters[model_pr]['vector_name']


# Path to save the best found model
path_to_save = '../HMM_models/Draft_models/'
file_name = 'hmm_' + vector_name + '_' + selected_category + '_' + collection_source + '_' + str(k_size)+'.pkl'

connection = MongoClient('localhost')  # "192.168.6.132"
collection_metadata = connection[db_name]["metadata"]
collection_source = connection[db_name][collection_source]

# Defining the testing data set and training data set
training_size = 0.6  # the testing dataset size is: 1 - training_size


def main():
    # Getting the time index for the entire data set
    time_query = {}
    timeline = rs.get_timeline(collection_source, time_query, freq='1D')

    div_index = int(training_size * len(timeline))
    start_time = timeline[0]
    div_time = timeline[div_index]
    end_time = timeline[-1]
    training_set = pd.date_range(start_time, div_time, freq='1D')
    training_set = [x._date_repr for x in training_set]
    testing_set = pd.date_range(div_time, end_time, freq='1D')
    testing_set = [x._date_repr for x in testing_set]

    print('Training set from: ', start_time, 'to ', div_time)
    print('Testing set from: ', div_time, 'to ', end_time)

    filter_query = {"breakout_group": selected_category,
                    'day_type': {'$in': day_type}}

    df_x = rs.get_vector(collection_source, timeline, filter_query, vector_name)

    # training the model for n_component: [n_min, n_max]
    best_score = 0
    best_n_comp = 0
    final_model = None
    best_log_prob = -np.inf

    for n_comp in range(n_min, n_max):
        df_training = df_x.loc[training_set]
        df_testing = df_x.loc[testing_set]
        df_training.dropna(inplace=True)
        df_testing.dropna(inplace=True)

        best_model = rs.HMM_trainning(df_training, n_comp, k_size)
        score, log_prob = rs.HMM_testing(df_testing, best_model)
        if score > best_score and log_prob > best_log_prob:
            best_score = score
            best_n_comp = n_comp
            best_log_prob = log_prob
            final_model = best_model


    print('Save the best mmodel in:', path_to_save + file_name)
    try:
        joblib.dump(final_model, filename=path_to_save + file_name, compress=3, protocol=2)

    except FileNotFoundError:
        joblib.dump(final_model, filename=file_name, compress=3, protocol=2)

    print("The best model was found with n_comp = {0} and score = {1} (log_prob = {2})".format(best_n_comp,
                                                                                               best_score, best_log_prob))


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
