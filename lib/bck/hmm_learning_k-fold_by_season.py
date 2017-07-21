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
selected_category = 'A'
# filter for type of day
day_type = ['working_day']  # 'holiday' was not significant
# filter for season (was given by the S vector)
season = 'winter'
# vector collection, is the collection where the vector R for each day is in
collection_daily_vector = 'vector_daily'

# Training Parameters
# k-fold cross-validation
k_size = 20
# maximun number of components to check
n_max = 10
# maximun number of components to check
n_min = 2
# vector to analyze
vector_name = 'R_vector'


# Path to save the best found model
path_to_save = '../HMM_models/Draft_models/'
file_name = 'hmm_' + vector_name + '_' + selected_category + '_' \
            + collection_daily_vector + '_' + season + str(k_size)+'.pkl'

connection = MongoClient('localhost')  # "192.168.6.132"
collection_metadata = connection[db_name]["metadata"]
collection_series = connection[db_name]["filtered_time_series"]
collection_daily_vector = connection[db_name][collection_daily_vector]

# Defining the testing data set and training data set
training_size = 0.6  # the testing dataset size is: 1 - training_size


# Get the R vector from the Mongo DB
def get_samples(time_line):
    l = []
    for timestamp in time_line:

        projection = {vector_name: True}
        filter_query = {"breakout_group": selected_category,
                        'timestamp': timestamp._date_repr,
                        'day_type': {'$in': day_type},
                        'seasonal_label': season}
        dictionaries = collection_daily_vector.find_one(filter_query, projection)
        if dictionaries is not None:
            l.append(dictionaries[vector_name])

    r = pd.DataFrame(l)
    r.dropna(inplace=True)

    return np.array(r)


def get_labels(time_line):
    l = []
    for timestamp in time_line:

        projection = {'day_type': True}
        filter_query = {"breakout_group": selected_category,
                        'timestamp': timestamp._date_repr,
                        'day_type': {'$in': day_type}}
        dictionaries = collection_daily_vector.find_one(filter_query, projection)
        if dictionaries is not None:
            l.append(dictionaries['day_type'])

    return np.array(l)


def score_model(Y_labels, Y_samples, model):
    r = 0
    n = len(Y_labels)

    hidden_states = np.array(model.predict(Y_samples))
    score_samples = model.predict_proba(Y_samples)

    to_score = list(zip(hidden_states, Y_labels))

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
    N = len(training_set)
    index_set = range(0, N)
    n_chucks = range(0, k_size)
    chunk_size = math.ceil((N - k_size) / k_size)
    best_score = 0

    for n in n_chucks:
        #validate_index = list(np.random.randint(n * chunk_size,N,chunk_size))
        validate_index = range(n * chunk_size, (n + 1) * chunk_size)
        training_index = [x for x in index_set if x not in validate_index]

        validating = training_set[validate_index]
        training = training_set[training_index]

        Y_samples = get_samples(validating)
        Y_labels = get_labels(validating)
        X_samples = get_samples(training)
        #print(X_samples.shape)
        #print(Y_samples.shape)
        # Training the model

        if len(X_samples) > 1 and len(Y_samples) > 1:
            model = GaussianHMM(n_components=n_component, covariance_type="diag", algorithm='map').fit(X_samples)

            # Validating the model for each chunk
            score = score_model(Y_labels, Y_samples, model)

            if score > best_score:
                best_score = score
                best_model = model

    return best_model


def HMM_testing(testing_set, model):
    Y_testing = get_samples(testing_set)
    Y_labels = get_labels(testing_set)

    score = score_model(Y_labels, Y_testing, model)
    print(score)
    return score

    # X_samples = np.array_split(X_samples, k_size)


def main():
    # Getting the time index for the entire data set
    time_query = dict()
    projection_time = dict()
    time_df = rs.get_tag_values(collection_series, time_query, projection_time, series_format='DF_t')
    div_index = int(training_size * time_df.index.size)

    start_time = time_df.index[0]._date_repr
    div_time = time_df.index[div_index]._date_repr
    end_time = time_df.index[-1]
    training_set = pd.date_range(start_time, div_time, freq='1D')
    testing_set = pd.date_range(div_time, end_time, freq='1D')
    print('Training set from: ', start_time, 'to ', div_time)
    print('Testing set from: ', div_time, 'to ', end_time)

    # training the model for n_component: [2,n_max]
    best_score = 0
    best_n_comp = 0
    final_model = None
    for n_comp in range(n_min, n_max):
        best_model = HMM_trainning(training_set, n_comp)
        score = HMM_testing(testing_set, best_model)
        if score > best_score:
            best_score = score
            best_n_comp = n_comp
            final_model = best_model

    print('Save the best mmodel in:', path_to_save + file_name)
    try:
        joblib.dump(final_model, filename=path_to_save + file_name, compress=3, protocol=2)

    except FileNotFoundError:
        joblib.dump(final_model, filename=file_name, compress=3, protocol=2)

    print("The best model was found with n_comp = {0} and score = {1}".format(best_n_comp, best_score))


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
