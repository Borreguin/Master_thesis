# Calculates statistics
# Author: Roberto Sanchez
# Personal library.
# 1. Obtain the subcategories
# 2. Get the tagnames of a selected category, this category is the reference for the breakout detection
# 3. Create the groups of tags based on the correlation matrix's rules
# 4. Get the n best features from the 'feature selection' algorithm
# 5. Compute by each day the feature vector (sample) in union with the correlation factors
# 6. Train the HMM model by feeding the samples
# 7. Search the best number of states for the HMM
# 8. Obtain the best HMM model
# 01/02/2017
##
from pymongo import MongoClient
import pandas as pd
import numpy as np
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_correlation = 'correlation_matrix_daily'
collection_feature_selection = 'feature_selection'
collection_statistics = 'statistics_daily'
# collection_to_save = 'vector_daily'

projection_field = 'breakout_group'

# Selection of categories to work with
category = 'C'
exclude_category = ['C_3']  # if is needed
selected_category = ['C_1', 'C_2', 'C_3', 'C_4']
selected_category = sorted(set(selected_category) - set(exclude_category))

# Manual selection of features (if is needed)
# 'min_ed', '(max-min)*std', 'dev_u', 'r_factor', 'min_st', 'min_me', 'mean', 'max_me'
# '50%', '25%', 'max_ed', 'max', 'r_factor_ed', '75%', 'min', 'std', 'r_factor_u', 'max_st', 'r_factor_st'
selected_features_m = sorted(['r_factor_u', 'r_factor', 'dev_u', '25%', '75%', 'mean'])
# selected_features_m = sorted(['min_ed', '(max-min)*std', 'dev_u', 'r_factor', 'min_st', 'min_me', 'mean', 'max_me',
#                            '50%', '25%', 'max_ed', 'max', 'r_factor_ed', '75%', 'min', 'std', 'r_factor_u', 'max_st',
#                            'r_factor_st'])
# print(selected_category)

# Note: Even if feature 'r_factor_u' does not gain so much information
# for the seasonal model this feature is important
# looking the results of 'feature_selection'
# the following features are the most interesting: ['r_factor_u', 'r_factor', 'dev_u', '25%', '75%', 'mean']

# Automatic selection of features
number_of_features = 6

# Mode for selecting features: manual/automatic
mode_manual = True

collection_to_save = '-'.join(selected_category)
projection_time = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables

# connecting to the collections
connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_correlation = connection[MONGODB_DB][collection_correlation]
collection_feature_selection = connection[MONGODB_DB][collection_feature_selection]
collection_statistics = connection[MONGODB_DB][collection_statistics]
collection_to_save = connection[MONGODB_DB][collection_to_save]


##############################################################

def main():
    # TODO: Put here the function to run

    tag_list = dict()
    feature_list = dict()
    subcategory_name = dict()

    # Creating structure of the S_vector
    seasonal_category = sorted(set(selected_category) - set(exclude_category))
    for sub_category in seasonal_category :
        tags_sub_cat = rs.get_tag_names(collection_metadata, {projection_field: sub_category})
        tag_list[sub_category] = tags_sub_cat
        feature_list[sub_category] = dict()
        for tag in tags_sub_cat:

            if mode_manual:
                feature_list[sub_category][tag] = selected_features_m
            else:
                feature_list[sub_category][tag] = rs.select_features(
                    collection_feature_selection,tag,n_features= number_of_features
                )
        subcategory_name[sub_category] = rs.get_field_value(
            collection_metadata,
            {projection_field: sub_category},
            'alias_breakout_group'
        )

    time_df = rs.get_tag_values(collection_series, time_query, projection_time, series_format='DF_t')
    start_time = time_df.index[0]._date_repr
    end_time = time_df.index[-1]._date_repr
    time_line = pd.date_range(start_time, end_time, freq='1D')

    # Save the description of the daily vectors: (this only one document that helps to understand the
    # current structure of S vector
    description_dictionary = {
            'S_category_list': seasonal_category,
            'tag_list': tag_list,
            'feature_list': feature_list,
            'subcategory_name': subcategory_name
    }
    save_description(description_dictionary, category)

    for timestamp in time_line:

        print("--process: " + timestamp._date_repr)
        print(seasonal_category)

        S_vector = []  # Seasonal vector according category C "particular_category"

        for sub_category in seasonal_category:
            # Calculating the feature vector based on statistics:
            features = feature_list[sub_category]
            tags_sub_cat = tag_list[sub_category]
            for tag in tags_sub_cat:
                # filter_query = {"tagname": {"$in": tag}, 'timestamp': timestamp._date_repr}
                filter_query = {"tagname": tag, 'timestamp': timestamp._date_repr}

                if mode_manual:
                    # Manual selection:
                    S_vector += get_feature_vector(filter_query, selected_features_m)
                else:
                    # Automatic selection:
                    # Cue to perform automatic selection
                    S_vector += get_feature_vector(filter_query, features[tag])

        filter_query = {"breakout_group": category, 'timestamp': timestamp._date_repr}

        save_vector(S_vector, filter_query)
    return "End of this script"


###############################################################

def get_categories(field):
    dict_list = list(collection_metadata.find({}, {field: True}))
    categories = [m.get(field) for m in dict_list]
    categories = list(set(categories))
    return categories

def get_feature_vector(filter_query, features):
    projection_features = rs.projection(features)
    dictionaries = list(collection_statistics.find(filter_query, projection_features))

    n_ft = len(features)
    F = [0] * n_ft
    if len(dictionaries) > 0:
        for d in dictionaries:
            for ftr, idx in zip(features, range(n_ft)):
                F[idx] += d[ftr]

        for idx in range(n_ft):
            F[idx] = round(F[idx] / len(dictionaries), 2)

    return F


def save_vector(S_vector, filter_query):
    aux = collection_to_save.find_one(filter_query)
    if aux is None:
        register = filter_query
    else:
        register = aux

    register['S_vector'] = S_vector
    collection_to_save.find_one_and_replace(
        filter_query,
        register,
        upsert=True
    )
    return 'Done!'


def save_description(description_dictionary, category):
    filter_query = {'document': 'description'}

    collection_to_save.find_and_modify(
        query=filter_query,
        update={"$set": {
            category: description_dictionary
        }},
        upsert=True,

    )

    return 'Done!'

###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
