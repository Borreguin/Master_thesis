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

# filter for type of day
day_type = ['weekend', 'working_day', 'holiday']  # 'holiday' was not significant


collection_statistics_daily = 'statistics_daily'
# select tag name
tag_query = {'category': {'$in': ['Temperature','Humidity', 'Heating', 'Cooling', 'CO_2']}, 'orientation': 'NE'}
# tag_query = {'category': {'$in': ['CO2']}, 'orientation': 'NE'}

# Training Parameters
# k-fold cross-validation
k_size = 10
# maximum  and minimum number of components to check (i.e. max /min number of hidden states)
n_max = 35
n_min = 25

# Path to save the best found model
path_to_save = '../HMM_models/Draft_models/'
file_name = 'hmm_'

connection = MongoClient('localhost')  # "192.168.6.132"
collection_metadata = connection[db_name]["metadata"]
collection_series = connection[db_name]["filtered_time_series"]
collection_statistics_daily = connection[db_name][collection_statistics_daily]


def main():
    # Getting the time index for the entire data set
    time_query = {}
    timeline = rs.get_timeline(collection_statistics_daily, time_query, freq='1D')

    start_time = timeline[0]
    end_time = timeline[-1]

    training_set = pd.date_range(start_time, end_time, freq='1D')

    training_set = [x._date_repr for x in training_set]

    print('Training set from: ', training_set[0], 'to ', training_set[-1])

    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    #tag_list= ['V074_tabs_warm_NO']

    for tag in tag_list:
        # training the model for n_component: [2,n_max]
        print('\t --processing', tag)
        best_score = 0
        best_n_comp = 0
        best_log_prob = -np.inf
        final_model = None

        df_vect = rs.get_samples(collection_series, start_time, end_time, tag,normalization=False)

        for n_comp in range(n_min, n_max):

            df_training = df_vect.loc[training_set]
            df_training.dropna(inplace=True)

            best_model = rs.HMM_trainning(df_training, n_comp, k_size)
            score, log_prob = rs.HMM_testing(df_training, best_model)
            if score > best_score and log_prob > best_log_prob:
                best_score = score
                best_n_comp = n_comp
                best_log_prob = log_prob
                final_model = best_model
                print("n_comp= {0}, score= {1}".format(n_comp, score))

        file_to_save = path_to_save + file_name + tag + '.pkl'
        print('Save the best model in:', file_to_save)
        try:
            joblib.dump(final_model, filename=file_to_save, compress=3, protocol=2)

        except FileNotFoundError:
            joblib.dump(final_model, filename=file_to_save, compress=3, protocol=2)

        print("The best model was found with n_comp = {0}, and score = {1} (log_prob = {2})".
              format(best_n_comp, best_score, best_log_prob))


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
