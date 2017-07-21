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
import pysax
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
collection_to_save = 'vector_daily'

# 'A_3-A_4_1-A_4_2-A_6_1-A_6_2'
projection_field = 'breakout_group'
selected_category = ['A', 'B']  # ['A', 'B']

exclude_category = {  # if is needed
    'A': ['A_1', 'A_2', 'A_5_1', 'A_5_2', 'A_6_3'],
    'B': ['B_1', 'B_2', 'B_5_1', 'B_5_2', 'B_6_3']
}
particular_category = ['C_1', 'C_2', 'C_3', 'C_4']

R_category_list = {
    'A': ['A', 'A_1', 'A_2', 'A_3', 'A_4_1', 'A_4_2', 'A_5_1', 'A_5_2', 'A_6_1', 'A_6_2', 'A_6_3'],
    'B': ['B', 'B_1', 'B_2', 'B_3', 'B_4_1', 'B_4_2', 'B_5_1', 'B_5_2', 'B_6_1', 'B_6_2', 'B_6_3']
}

R_category_list = {
    'A': sorted(list(set(R_category_list['A']) - set(exclude_category['A']))),
    'B': sorted(list(set(R_category_list['B']) - set(exclude_category['B'])))
}

# number of divisions) for SAX representation
# sax_nbins_size = 3
# sax_alphabet = "abcd"
# sax = pysax.SAXModel(nbins=sax_nbins_size, alphabet=sax_alphabet)

#collection_to_save = '-'.join(R_category_list['A'])

projection_time = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables

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

    categories = get_categories(projection_field)
    categories = list(set(categories) - set(exclude_category))
    categories = sorted(categories)
    description_dictionary = dict()

    for category in selected_category:
        tag_category = rs.get_tag_names(collection_metadata, {projection_field: category})
        # f = category + '_'  # applying filter of category: ex: "A_"
        # sub_category_list = [k for k in categories if f in k]
        # sub_category_list = sorted(sub_category_list)

        for tag in tag_category:

            tag_list = dict()
            subcategory_name = dict()
            category_list = R_category_list[category]
            for sub_category in category_list:
                tags_sub_cat = rs.get_tag_names(collection_metadata, {projection_field: sub_category})
                tag_list[sub_category] = tags_sub_cat
                subcategory_name[sub_category] = rs.get_field_value(
                    collection_metadata,
                    {projection_field: sub_category},
                    'alias_breakout_group'
                )

            time_line = rs.get_timeline(collection_series, time_query, freq='1D')
            mean_general, std_general = rs.get_mean_std_tag(collection_series, time_query, tag)

            # Save the description of the daily vectors: (this only one document that helps to understand the
            # current structure of the R vector
            description_dictionary = {
                'R_category_list': category_list,
                'tag_list': tag_list,
                'subcategory_name': subcategory_name
            }
            save_description(description_dictionary, category)

            category_list = sorted(list(set(category_list) - set(category)))
            # sax_words = dict()
            # idx = 0
            for timestamp in time_line:

                print("--process: " + timestamp)
                print(category_list)
                # query_time = rs.dictionary_time(timestamp)
                # mean_local, std_local = rs.get_mean_std_tag(collection_series,query_time,tag)

                # correlation vector according with the subcategory,
                # his first member is the normalized mean of the variable CO2

                # R_vector = [(mean_local-mean_general)/(std_general)]
                R_vector = []

                for sub_category in category_list:
                    tags_sub_cat = tag_list[sub_category]
                    # Calculating the correlation factor r for all the tags that belong to the subcategory
                    filter_query = {'tagname': tag, 'timestamp': timestamp}
                    correlation_vector = get_correlation_vector(filter_query, tags_sub_cat)

                    R_vector += correlation_vector

                # Save results
                # word = sax.symbolize_whiten_window(R_vector)
                # if not word in sax_words.keys():
                #    sax_words[word] = idx
                #    idx += 1

                # R_vector += [sax_words[word]]
                filter_query = {"breakout_group": category, 'timestamp': timestamp}
                # save_information(R_vector, word, filter_query)
                save_information(R_vector, filter_query)
    return "End of this script"


###############################################################

def get_categories(field):
    projection = {field: True}
    dict_list = list(collection_metadata.find({}, projection))
    categories = [m.get(field) for m in dict_list]
    categories = list(set(categories))
    return categories


def get_correlation_value(filter_query, tags_sub_cat):
    projection_tags = rs.projection(tags_sub_cat)
    r_value = collection_correlation.find_one(filter_query, projection_tags)
    n = len(r_value)
    r_value = sum(r_value.values())
    if n != 0: r_value = round(r_value / n, 2)
    return r_value


def get_correlation_vector(filter_query, tags_sub_cat):
    projection_tags = rs.projection(tags_sub_cat)
    r_value = collection_correlation.find_one(filter_query, projection_tags)
    n = len(tags_sub_cat)
    r = [0] * n
    for tag, idx in zip(tags_sub_cat, range(n)):
        if tag in r_value.keys():
            r[idx] = r_value[tag]

    return r


# def save_information(R_vector, word, filter_query):
def save_information(R_vector, filter_query):
    collection_to_save.find_and_modify(
        query=filter_query,
        update={"$set": {
            'R_vector': R_vector
            # 'R_sax_word': word
        }},
        upsert=True,

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
    print('end of the script \n Vector according to: {0}'.format('-'.join(R_category_list['A'])))
