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

collection_feature_selection = 'feature_selection'
collection_statistics_daily = 'statistics_daily'
# select tag name
tag_query = {'category': {'$in': ['CO2']}}

# Training Parameters
# k-fold cross-validation
k_size = 10
# maximun  and minimun number of components to check
n_max = 15
n_min = 10

# Selection of Features
# maximun and minimum number of features
f_max = 18
f_min = 6

# Path to save the best found model
path_to_save = '../HMM_models/Draft_models/'
file_name = 'hmm_' + collection_statistics_daily + '_'

connection = MongoClient('localhost')  # "192.168.6.132"
collection_metadata = connection[db_name]["metadata"]
collection_series = connection[db_name]["filtered_time_series"]
collection_statistics_daily = connection[db_name][collection_statistics_daily]
collection_feature_selection = connection[db_name][collection_feature_selection]

# Defining the testing data set and training data set
training_size = 0.6  # the testing dataset size is: 1 - training_size


def score_model(Y_samples, model):
    r = 0
    n = len(Y_samples)

    score_samples = model.predict_proba(Y_samples)

    # to_score = list(zip(hidden_states, Y_labels))
    # hidden_states = np.array(model.predict(Y_samples))
    # count_labels = dict(collections.Counter(to_score))
    # comp = range(0, model.n_components)

    # p = 0
    # for c in comp:
    #    count = {k: v for (k, v) in count_labels.items() if c in k}
    #    values = [i for i in count.values()]
    #    if len(values) > 0:
    #        s = sum(values)
    #        p = p + (max(values) / n)

    for sample in score_samples:
        max_prob = max(sample)
        r += max_prob

    score = (r / n)
    return score


def HMM_trainning(training_set, n_component):
    # Working with the selected category
    # and make the k-fold cross validation
    N = len(training_set.index)
    index_set = range(0, N)
    n_chucks = range(0, k_size)
    chunk_size = math.ceil((N - k_size) / k_size)
    best_score = 0

    for n in n_chucks:
        # validate_index = list(np.random.randint(n * chunk_size,N,chunk_size))
        validate_index = range(n * chunk_size, (n + 1) * chunk_size)
        training_index = [x for x in index_set if x not in validate_index]

        validating = training_set.iloc[validate_index]
        training = training_set.iloc[training_index]

        Y_samples = validating.values
        X_samples = training.values
        # print(X_samples.shape)
        # print(Y_samples.shape)
        # Training the model
        model = GaussianHMM(n_components=n_component, covariance_type="diag").fit(X_samples)

        # Validating the model for each chunk
        score = score_model(Y_samples, model)

        if score > best_score:
            best_score = score
            best_model = model

    return best_model


def HMM_testing(testing_set, model):
    Y_testing = testing_set.values
    # Y_labels = get_labels(testing_set)

    score = score_model(Y_testing, model)
    # print(score)
    return score

    # X_samples = np.array_split(X_samples, k_size)


def main():
    # Getting the time index for the entire data set
    time_query = {}
    timeline = rs.get_timeline(collection_statistics_daily, time_query, freq='1D')

    div_index = int(training_size * len(timeline))
    start_time = timeline[0]
    div_time = timeline[div_index]
    end_time = timeline[-1]
    training_set = pd.date_range(start_time, div_time, freq='1D')
    testing_set = pd.date_range(div_time, end_time, freq='1D')
    print('Training set from: ', start_time, 'to ', div_time)
    print('Testing set from: ', div_time, 'to ', end_time)

    tag_list = rs.get_tag_names(collection_metadata, tag_query)

    for tag in tag_list:
        # training the model for n_component: [2,n_max]
        print('\t --processing', tag)
        best_score = 0
        best_n_comp = 0
        best_n_feat = 0
        final_model = None

        feature_list = rs.select_features(collection_feature_selection, tag, f_max)
        df_norm_feature = rs.get_feature_vector(collection_statistics_daily,
                                                start_time, end_time, tag, feature_list)

        for n_features in range(f_min, f_max):

            features = feature_list[:n_features]

            for n_comp in range(n_min, n_max):

                df_training = df_norm_feature.loc[training_set, features]
                df_testing = df_norm_feature.loc[testing_set, features]
                best_model = HMM_trainning(df_training, n_comp)
                score = HMM_testing(df_testing, best_model)
                if score > best_score:
                    best_score = score
                    best_n_comp = n_comp
                    best_n_feat = n_features
                    final_model = best_model
                    print("n_comp= {0}, n_feat= {1}, score= {2}".format(n_comp, best_n_feat, score))

        file_to_save = path_to_save + file_name + tag + '.pkl'
        print('Save the best model in:', file_to_save)
        try:
            joblib.dump(final_model, filename=file_to_save, compress=3, protocol=2)

        except FileNotFoundError:
            joblib.dump(final_model, filename=file_to_save, compress=3, protocol=2)

        print("The best model was found with n_comp = {0} , n_feat = {1} and score = {2}".
              format(best_n_comp, best_n_feat, best_score))


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
