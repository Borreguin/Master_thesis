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
collection_daily_vector = 'vector_daily_f'

projection_field = 'breakout_group'
selected_category = ['A']
exclude_category = ['']  # if is needed
particular_category = ['C_1', 'C_2', 'C_3', 'C_4']
#features_particular_category = sorted(['r_factor', '25%', '75%', 'std', 'min', 'mean'])
features_particular_category = sorted(['r_factor_u','dev_u', '25%', '75%', 'r_factor', '50%', 'mean'])
number_features = 6

projection_time = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_correlation = connection[MONGODB_DB][collection_correlation]
collection_feature_selection = connection[MONGODB_DB][collection_feature_selection]
collection_statistics = connection[MONGODB_DB][collection_statistics]
collection_daily_vector = connection[MONGODB_DB][collection_daily_vector]


##############################################################


def main():
    # TODO: Put here the function to run

    categories = get_categories(projection_field)
    categories = list(set(categories) - set(exclude_category))
    categories = sorted(categories)
    description_dictionary = {'document': 'description'}

    for category in selected_category:
        tag_category = rs.get_tag_names(collection_metadata, {projection_field: category})
        f = category + '_'  # applying filter of category: ex: "A_"
        sub_category_list = [k for k in categories if f in k]
        sub_category_list = sorted(sub_category_list)

        for tag in tag_category:

            tag_list = dict()
            feature_list = dict()
            subcategory_name = dict()
            for sub_category in sub_category_list:
                tags_sub_cat = rs.get_tag_names(collection_metadata, {projection_field: sub_category})
                tag_list[sub_category] = tags_sub_cat
                feature_list[sub_category] = sorted(select_features(tags_sub_cat, number_features))
                subcategory_name[sub_category] = rs.get_field_value(
                                                        collection_metadata,
                                                        {projection_field: sub_category},
                                                        'alias_breakout_group'
                                                                    )

            for sub_category in particular_category:
                tags_sub_cat = rs.get_tag_names(collection_metadata, {projection_field: sub_category})
                tag_list[sub_category] = tags_sub_cat
                feature_list[sub_category] = features_particular_category
                subcategory_name[sub_category] = rs.get_field_value(
                    collection_metadata,
                    {projection_field: sub_category},
                    'alias_breakout_group'
                )

            time_df = rs.get_tag_values(collection_series, time_query, projection_time, series_format='DF_t')
            start_time = time_df.index[0]._date_repr
            end_time = time_df.index[-1]._date_repr
            time_line = pd.date_range(start_time, end_time, freq='1D')

            R_category_list = sub_category_list + particular_category
            F_category_list = sub_category_list

            # Save the description of the daily vectors: (this only one document that helps to understand the
            # current structure of the R and F vector
            description_dictionary[category] = {
                    'R_category_list': R_category_list,
                    'F_category_list': F_category_list,
                    'S_category_list': particular_category,
                    'tag_list': tag_list,
                    'feature_list': feature_list,
                    'subcategory_name': subcategory_name
            }
            save_description(description_dictionary)

            for timestamp in time_line:

                print("--process: " + timestamp._date_repr)
                print(sub_category_list, particular_category)
                R_vector = []  # correlation vector according with the subcategory
                F_vector = []  # feature vector for each variable according with the subcategory
                S_vector = []  # Seasonal vector according category C "particular_category"
                """for sub_category in R_category_list:
                    tags_sub_cat = tag_list[sub_category]
                    # Calculating the correlation factor r for all the tags that belong to the subcategory
                    filter_query = {'tagname': tag, 'timestamp': timestamp._date_repr}
                    projection_tags = rs.projection(tags_sub_cat)
                    R_vector.append(get_correlation_value(filter_query, projection_tags))

                for sub_category in F_category_list:
                    # Calculating the feature vector based on statistics:
                    features = feature_list[sub_category]
                    tags_sub_cat = tag_list[sub_category]
                    filter_query = {"tagname": {"$in": tags_sub_cat}, 'timestamp': timestamp._date_repr}
                    F_vector += get_feature_vector(filter_query, features)"""

                for sub_category in particular_category:
                    # Calculating the feature vector based on statistics:
                    features = feature_list[sub_category]
                    tags_sub_cat = tag_list[sub_category]
                    filter_query = {"tagname": {"$in": tags_sub_cat}, 'timestamp': timestamp._date_repr}
                    S_vector += get_feature_vector(filter_query, features)

                filter_query = {"breakout_group": category, 'timestamp': timestamp._date_repr}

                save_vector(R_vector, F_vector, S_vector, filter_query)
    return "End of this script"


###############################################################

def get_categories(field):
    dict_list = list(collection_metadata.find({}, {field: True}))
    categories = [m.get(field) for m in dict_list]
    categories = list(set(categories))
    return categories


def select_features(tag_list, n_features):
    result = dict()
    field = 'summary'
    tag_filter = {'tagname': tag_list[0]}
    projection_filter = {field: True}
    feature_list = collection_feature_selection.find_one(tag_filter, projection_filter)
    feature_list = list(feature_list[field].keys())

    for f in feature_list:
        result[f] = 0

    for tag in tag_list:
        tag_filter = {'tagname': tag}
        to_select = collection_feature_selection.find_one(tag_filter, projection_filter)
        to_select = to_select[field]

        max_value = max([to_select[k]['J_value'] for k in feature_list])
        for f in feature_list:
            result[f] += to_select[f]['J_value'] / max_value

    result = sorted(result, key=result.get, reverse=True)

    return result[:n_features]


def get_correlation_value(filter_query, projection_tags):
    r_value = collection_correlation.find_one(filter_query, projection_tags)
    n = len(r_value)

    if n != 0:
        r_value = np.array(list(r_value.values()))
        r_value = round(r_value.mean(), 2)
        return r_value

    else:
        return 0


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


def save_vector(R_vector, F_vector, S_vector, filter_query):
    aux = collection_daily_vector.find_one(filter_query)
    if aux is None:
        register = filter_query
    else:
        register = aux

    register['R_vector'] = R_vector
    register['F_vector'] = F_vector
    register['S_vector'] = S_vector
    collection_daily_vector.find_one_and_replace(
        filter_query,
        register,
        upsert=True
    )
    return 'Done!'


def save_description(description_dictionary):
    filter_query = {'document': 'description'}
    aux = collection_daily_vector.find_one(filter_query)
    if aux is None:
        register = filter_query
    else:
        register = aux

    register.update(description_dictionary)
    collection_daily_vector.find_one_and_replace(
        filter_query,
        register,
        upsert=True
    )
    return 'Done!'



###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
