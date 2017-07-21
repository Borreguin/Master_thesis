##
# Author: Roberto Sanchez
# Personal library.
# 1. Queries to Mongo Data Base
# 2. Use of argparse to make bash scripts
#
# 10/10/2016
##
import argparse
import pandas as pd
import datetime
import math
import collections
from flask import request
import time
import numpy as np

from hmmlearn.hmm import GaussianHMM

def __get_parameters(option='default'):
    # DEFAULT PARAMETERS
    dbs_name = 'project_db'
    collection_metadata = 'metadata'

    collection_filter = 'filter_collection'
    collection_register = 'register_collection'
    # query_tag = "tagname: V032_outdoor_temp"
    # collection_filter = 'filtered_time_series'
    collection_series = 'original_time_series'
    collection_filtered = 'filtered_time_series'
    collection_register = 'collection_register'
    collection_origen = 'collection_origen'
    collection_destine = 'collection_destine'
    query_tag = "{}"  # {} implies all the tagnames
    query_time = '2012|2013|2014|2015'
    # PARSE VALUES
    parser = argparse.ArgumentParser(description='Arguments to insert:')
    parser.add_argument('-file', type=str, default="output.csv", help='Output file name, Ex: file.csv')
    parser.add_argument('-db', type=str, default=dbs_name, help='Mongo Data Base Name')
    parser.add_argument('-mt', type=str, default=collection_metadata, help='MongoDB Collection Name for the metadata')
    parser.add_argument('-sr', type=str, default=collection_series, help='MongoDB Collection Name for the time series')
    parser.add_argument('-or', type=str, default=collection_origen,
                        help='MongoDB Collection Name for the collection origen')
    parser.add_argument('-de', type=str, default=collection_destine,
                        help='MongoDB Collection Name for the collection destine')
    parser.add_argument('-q', type=str, default=query_tag,
                        help='Query for tags Names using the Metadata DB. Ex: "type: Temperature, location : out')
    parser.add_argument('-t', type=str, default=query_time,
                        help='Query for specify  the time.  Ex1: 2013 | 2014  Ex2: 06.2013')
    parser.add_argument('-v', action='store_true', help='Verbose mode')

    parser.add_argument('-save', type=str, default=collection_register,
                        help='Name of MongoDB collection to save the results')

    if option == 'filter':
        number_interactions = 2
        # factor for filtering information
        filter_factor = 3.0
        parser.add_argument('-ft', type=float, default=filter_factor,
                            help='Factor for filtering, Ex: 4.5 (based on the Central Limit Theorem)')
        parser.add_argument('-it', type=int, default=number_interactions, help='Number of interactions, Ex: 2')
        parser.add_argument('-fi', type=str, default=collection_filtered,
                            help='MongoDB Collection Name for saving the filtered values')
        parser.add_argument('-clone', action='store_true', help='clone database mode')

    return parser.parse_args()


def get_sax_profiles(collection_sax, filter_sax_query, field_sax_name, sax_word,
                     collection_series, tag, max_cluster_number):
    sax_word_df = get_tag_values(collection_sax, filter_sax_query, field_sax_name, series_format='DF_t')

    if len(sax_word_df.index) == 0:
        return pd.DataFrame()

    mask_date = sax_word_df[sax_word_df[field_sax_name] == sax_word].index
    mask_date = [x._date_repr for x in mask_date]
    N = len(mask_date)
    cnt_sax = collections.Counter(sax_word_df[field_sax_name])
    T_max = max(cnt_sax.values())

    n_clusters = max(math.ceil(N / T_max * max_cluster_number), 1)
    if n_clusters > N:
        n_clusters = N

    df = get_tag_pivoted_values(collection_series, mask_date, tag)
    df = df.loc[mask_date]
    df = df.fillna(df.interpolate())  # only because is an approximation

    order_index = order_all_index(df)
    chunk_list = chunkIt(order_index, n_clusters)

    register = {
        'days': [sorted(l) for l in chunk_list],
        'data': list()
    }
    for chunk, idx in zip(chunk_list, range(len(chunk_list))):
        df_result = df.loc[chunk].mean(axis=0)

        for col, idy in zip(df_result, range(len(df_result))):
            register['data'].append(
                {'day': idx, 'hour': idy,
                 'value': round(col, 1)})

    return register, mask_date


def get_tag_names(metadata_collection, tag_query):
    """
    Useful to get the tag names for the metadata
    :param metadata_collection: collection for the meatadata
    :param tag_query: Example: type:Temperature
    :return: List of tag variables
    """

    tags = []
    projection_value = {'tagname': True, 'id': True, '_id': False}
    if isinstance(tag_query, str):
        query = dictionary_query(tag_query)
    else:
        query = tag_query
    # getting name of tags
    cursor = metadata_collection.find(query, projection_value)
    df = pd.DataFrame(list(cursor))
    try:
        df.sort_values(['id'], inplace=True)
        tags = list(df['tagname'])
    except KeyError:
        print("The query does not produce any results :\nCorrect format without spaces: f1:value1,f2:value2 \n",
              tag_query)
        print("Observe capital letters in names")
    return tags


def get_category(metadata_collection, tag):
    cursor_dict = metadata_collection.find_one({'tagname': tag}, {'category': True})
    return cursor_dict['category']


def get_alias(metadata_collection, tag):
    cursor_dict = metadata_collection.find_one({'tagname': tag}, {'alias': True})
    return cursor_dict['alias']


def get_field_value(metadata_collection, query_dict, field):
    projection = {field: True}
    cursor_dict = metadata_collection.find_one(query_dict, projection)
    if cursor_dict is not None:
        return cursor_dict[field]
    else:
        return None


def dictionary_query(str_query):
    """
    takes a string query and convert in dictionary,
    useful when is needed to transform
    :param str_query: in format: f1:value1,f2:value2
    :return: dictionary
    """

    query = dict()
    if str_query != '{}':
        try:
            statement = str_query.split(',')
            for x in statement:
                f = x.split(':')
                query[f[0].strip()] = f[1].strip()
        except IndexError:
            print(str_query, ' has bad format for the query. \nCorrect format without spaces: f1:value1,f2:value2 \n')
            print("Observe capital letters in names")
    return query


def dictionary_regex_query(str_query):
    """
    takes a string query and convert in dictionary,
    that use simple regular expressions
    :param str_query: in format: f1:value1,f2:value2
    :return: dictionary
    """

    query = dict()
    if str_query != '{}':
        try:
            statement = str_query.split(',')
            for x in statement:
                f = x.split(':')
                query[f[0].strip()] = {"$regex": f[1].strip()}
        except IndexError:
            print(str_query, ' has bad format for the query. \nCorrect format without spaces: f1:value1,f2:value2 \n')
            print("Observe capital letters in names")
    return query


def dictionary_time(str_time):
    """ takes a string query and convert in dictionary
		useful when is needed to transform
		a string time in a dictionary from the command line
	"""

    time = {}
    if str_time != "":
        time['timestamp'] = {"$regex": str_time}
    return time


def dictionary_time_period(str_start_time, str_end_time):
    """ creates a string query that use regular expresions
	"""
    reg_exp = " "
    t2 = pd.Timestamp(str_end_time)
    t1 = pd.Timestamp(str_start_time)
    delta = t2 - t1

    if delta < pd.Timedelta(days=30):
        if t1.month <= 9:
            x = '0' + str(t1.month)
        if t2.month <= 9:
            y = '0' + str(t2.month)
        reg_exp = "(" + x + "|" + y + ")(.|/|-)" + str(t2.year)

    # time['timestamp'] = { "$regex": reg_exp }
    return reg_exp


def projection(lst_attributes, _id=False):
    """
    This function returns a dictionary in format {'at1':True,'at2':True, etc}
    :param lst_attributes: Contains the list of attributes
    :param _id: decide whether the '_id' column values is added or not
    :return: dictionary
    """
    assert isinstance(lst_attributes, list)
    dictionary = dict()
    for x in lst_attributes:
        dictionary[x] = True
    dictionary['_id'] = _id
    return dictionary


def get_tag_values(collection, regex_query, projection_query,
                   del_id=True, series_format='DF'):
    # TODO: time_format='%Y-%m-%d %H:%M' in case a specific format of date is needed
    """
    Gets values of a time series which name is 'tag' in a period of time given by regex_time dictionary/string
    :param collection: Mongo DB collection
    :param projection_query: String name of a single tag, or list, or dictionary
    :param regex_query:  It could be either a dictionary {'timestamp': {'$regex': '02.2013'}}
                              or string '(02|03).2013'
    :param del_id: True if you want delete the '_id' column
    :param series_format: 1) 'xy' format where: x = timestamp, y = list of values,
                          2) 'DF' (default) send a DataFrame
                          3) 'DF_t' having as an timestamp index including a replicated timestamp column
                          4) 'DF_idx' only with timestamp index
    :return: a pandas dataFrame
    The requested data base has the shape:
        timestamp:  12-06-2012
        tag1:       4520.2
        tag2:       12.2
        etc.
    """

    if not isinstance(projection_query, dict):
        if isinstance(projection_query, str):
            projection_dict = {projection_query: True}
        else:
            assert isinstance(projection_query, list)
            projection_dict = dict()
            for x in projection_query:
                projection_dict[x] = True
    else:
        projection_dict = projection_query
        assert isinstance(projection_dict, dict)

    projection_dict['timestamp'] = True

    if isinstance(regex_query, str):
        regex_query = dictionary_time(regex_query)

    # probe if regex_query and projection are dictionaries
    assert isinstance(regex_query, dict)
    assert isinstance(projection_dict, dict)

    # additional process if is needed
    if del_id:
        projection_dict['_id'] = False

    # query the Database
    cursor = collection.find(regex_query, projection_dict)
    # getting the Dataframe
    df = pd.DataFrame(list(cursor))
    if df.empty:
        print("The query for collection:", collection, "does not produce any value")
        print("Query: ", regex_query)
        print("Projection: ", projection_dict)
        df = pd.DataFrame(columns=list(projection_dict.keys()))
        return df

    if 'DF' == series_format:
        return df

    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M:%S")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct." +
                  "It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")

    if 'DF_t' == series_format:
        df['timestamp'] = df_aux
        df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        df.set_index(keys=['timestamp'], inplace=True)
        df.index = pd.to_datetime(df.index)  # <-
        df.sort_index(inplace=True)

        # df['timestamp'] = [str(x) for x in df.index]
        return df

    if 'DF_idx' == series_format:
        # this is for numeric values
        df['timestamp'] = df_aux
        # df = df.sort_values(['timestamp'], ascending=[1])
        df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        df.set_index(keys=['timestamp'], inplace=True)  # <-
        df.sort_index(inplace=True)
        df['timestamp'] = [str(x) for x in df.index]
        return df

    if 'xy' == series_format:
        df['timestamp'] = df_aux
        df = df.sort_values(['timestamp'], ascending=[1])
        x = [str(x) for x in df['timestamp']]
        y = list()
        try:
            y = list(df[projection_query])
        except TypeError:
            print("For xy format the projection_query must be a string and not: ", type(projection_query))
        return x, y

    print('Any series format was selected')
    return df


def to_numeric(df, tag_list):
    for tag in tag_list:
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        df[tag] = df[tag].fillna(df[tag].interpolate())

    return df


def validate_time_index(df):
    df_aux = df.copy()
    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M:%S")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct." +
                  "It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")

    df['timestamp'] = df_aux
    df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
    df.set_index(keys=['timestamp'], inplace=True)
    df.index = pd.to_datetime(df.index)  # <-
    df.sort_index(inplace=True)
    df['timestamp'] = [str(x) for x in df.index]
    return df


def get_epoch(date, date_format='%Y-%m-%d %H:%M:%S'):
    date_list = list()
    for d in date:
        date_list.append(epoch(d, date_format))

    return date_list


def epoch(date, date_format="%Y-%m-%d %H:%M:%S"):
    epoch_value = -1
    try:
        epoch_value = time.mktime(time.strptime(str(date), date_format))
    except ValueError:
        try:
            epoch_value = time.mktime(time.strptime(date, "%d.%m.%Y %H:%M:%S"))
        except ValueError:
            print('Observe the format of the timestamp in: \n{0}', date)
    return epoch_value


def get_timeline(collection, time_query, freq='1D'):
    projection_time = {'timestamp': True}
    if not freq is None:
        time_df = get_tag_values(collection, time_query, projection_time, series_format='DF_t')
        time_df = time_df.loc[time_df.index.to_series().dropna()]
        start_time = time_df.index[0]._date_repr
        end_time = time_df.index[-1]._date_repr
        time_line = pd.date_range(start_time, end_time, freq=freq)
        time_line = [x._date_repr for x in time_line]
        return time_line
    else:
        time_df = get_tag_values(collection, time_query, projection_time, series_format='DF_t')
        time_line = [x._date_repr for x in time_df.index]
        return time_line


# Get the vector to train from the Mongo DB
def get_samples_norm_vector(collection_source, time_line, filter_query, field):

    projection = {field: True, '_id':False, 'timestamp': True}
    # general statitistics:
    l = list()
    t = list()
    time_query = filter_query.copy()
    time_query.update({'timestamp': {'$exists': True}})
    dictionaries = collection_source.find(time_query, projection)
    for d in list(dictionaries):
        if d is not None:
            l.append(d[field])
            t.append(d['timestamp'])
    t = pd.DatetimeIndex(t)
    df = pd.DataFrame(index=t,data=l)
    df.sort_index(inplace=True)

    # selected period:
    df_result = df.loc[pd.DatetimeIndex(time_line)]

    # normalization
    for f in df_result:
        df_result[f].fillna(df_result[f].mean(), inplace=True)
        df_result[f] = (df_result[f] - df[f].mean()) / (df[f].std() + np.finfo(float).eps)

    df_result.index = time_line
    return df_result


# Get the vector to train from the Mongo DB
def get_vector(collection_source, time_line, filter_query, field):

    projection = {field: True, '_id':False, 'timestamp': True}
    # general statitistics:
    l = list()
    t = list()
    time_query = filter_query.copy()
    time_query.update({'timestamp': {'$in': time_line}})
    dictionaries = collection_source.find(time_query, projection)
    for d in list(dictionaries):
        if d is not None:
            l.append(d[field])
            t.append(d['timestamp'])
    t = pd.DatetimeIndex(t)
    df = pd.DataFrame(index=t,data=l)
    df.sort_index(inplace=True)

    # selected period:
    df_result = df.loc[pd.DatetimeIndex(time_line)]

    df_result.index = time_line
    return df_result



def nodes_and_links(list_sax_word, h_pref):
    cnt_sax = collections.Counter(list_sax_word)
    sax_words = sorted(cnt_sax.keys())

    nodes = list()
    links = list()
    for i in range(1, len(sax_words[0])):

        filter_sax = sorted(list(set([x[:i] for x in sax_words])))
        nodes += [name for name in filter_sax]
        for f in filter_sax:
            prefix = sorted(list(set([k[:(i + 1)] for k in sax_words if f in k[:i]])))

            for pf in prefix:
                sub_list = [k for k in sax_words if pf in k[:(i + 1)]]
                sub_list_values = [cnt_sax[sb] for sb in sub_list]
                r = sum(sub_list_values)
                nodes += prefix
                links.append({'source': f, 'target': pf, 'value': r})

    nodes = list(set(nodes))
    nodes = [{"name": name, "type": 'node'} for name in nodes]

    for idx in range(1, len(h_pref)):
        nodes += [{"name": h_pref[idx] + name, "type": 'heatmap'} for name in sax_words]
        links += [{'source': h_pref[idx - 1] + s,
                   'target': h_pref[idx] + s, 'value': cnt_sax[s]} for s in sax_words]

    return nodes, links, sax_words


def get_mean_std_tag(collection, time_query, tag):
    df_x = get_tag_values(collection, time_query, tag, series_format='DF_t')
    df_x[tag] = pd.to_numeric(df_x[tag], errors='coerce')
    df_x[tag].dropna(inplace=True)
    return df_x[tag].mean(), df_x[tag].std()


def get_tag_pivoted_values(collection, time_query, tag):
    if isinstance(time_query, dict):
        df = get_tag_values(collection, time_query, tag, series_format='DF_t')
        df['hour'] = [x.hour for x in df.index]
        df['date'] = [x._date_repr for x in df.index]

        # transform series in a table for hour and dates
        try:
            df = df.pivot(index='date', columns='hour', values=tag)
        except:
            print('No possible convertion, in format: index-> date, columns-> hours')
            df = pd.DataFrame()

        return df
    elif isinstance(time_query, list):
        df_result = pd.DataFrame()

        for t in time_query:
            t2 = pd.Timestamp(t) + pd.DateOffset(hours=23.9)
            query_time = dict_range_time(pd.Timestamp(t), t2)
            df = get_tag_values(collection, query_time, tag, series_format='DF_t')
            df[tag] = pd.to_numeric(df[tag], errors='coerce')
            df = df.rename(columns={tag: t})
            df.index = df.index.hour
            df_result = df_result.append(df.T)
        return df_result


def get_feature_vector(collection_statistics_daily, start_time, end_time, tag, feature_list):

    time_query = dict_range_time(start_time, end_time, '%Y-%m-%d')
    filter_query = {'tagname': tag}
    filter_query.update(time_query)
    df_feature = get_tag_values(collection_statistics_daily, filter_query,
                                   feature_list, series_format='DF_t')
    # feature normalization
    for f in feature_list:
        mean = df_feature[f].mean()
        df_feature[f].fillna(mean, inplace=True)
        df_feature[f] = (df_feature[f]-df_feature[f].mean())/(df_feature[f].max() - df_feature[f].min() + 0.001)

    return df_feature


def get_samples(collection_series, start_time='2012-06-23', end_time='2015-06-08',
                tag='V022_vent02_CO2', timeline=None, normalization=True):

    if timeline is None:
        time_query = dict_range_time(start_time, end_time, '%Y-%m-%d')
        df_result = get_tag_pivoted_values(collection_series,time_query,tag)

    else:
        df_result = get_tag_pivoted_values(collection_series,timeline,tag)

    for f in df_result:
        df_result[f].fillna(df_result[f].mean(), inplace=True)

    if normalization:
        # normalization
        for f in df_result:
            df_result[f] = (df_result[f] - df_result[f].mean()) / (df_result[f].std() + np.finfo(float).eps)

    return df_result


def HMM_trainning(training_set, n_component, k_size):
    # Working with the selected category
    # and make the k-fold cross validation
    N = len(training_set.index)
    index_set = range(0, N)
    n_chucks = range(0, k_size)
    chunk_size = math.ceil((N - k_size) / k_size)
    best_score = 0
    best_log_prob = -np.inf

    for n in n_chucks:
        # validate_index = list(np.random.randint(n * chunk_size,N,chunk_size))
        validate_index = range(n * chunk_size, (n + 1) * chunk_size)
        training_index = [x for x in index_set if x not in validate_index] # normal k-fold cross validation
        # validate_index = range(n * chunk_size, (k_size) * chunk_size)
        # training_index = range(0, n* chunk_size)

        validating = training_set.iloc[validate_index]
        training = training_set.iloc[training_index]

        Y_samples = validating.values
        X_samples = training.values
        # print(X_samples.shape)
        # print(Y_samples.shape)
        # Training the model
        model = GaussianHMM(n_components=n_component, covariance_type="diag").fit(X_samples)

        # Validating the model for each chunk
        score, log_prob = score_model(Y_samples, model)

        if score > best_score and log_prob > best_log_prob:
            best_score = score
            best_model = model
            best_log_prob = log_prob
        # print(score, log_prob)

    return best_model


def HMM_testing(testing_set, model):
    Y_testing = testing_set.values

    score = score_model(Y_testing, model)
    # print(score)
    return score


def score_model(Y_samples, model):
    r = 0
    n = len(Y_samples)

    score_samples = model.predict_proba(Y_samples)
    log_prob = model.score(Y_samples)


    for sample in score_samples:
        max_prob = max(sample)
        r += max_prob

    score = (r / n)
    return score, log_prob


def select_features(collection_feature_selection, tag, n_features):
    result = dict()
    field = 'summary'
    tag_filter = {'tagname': tag}
    projection_filter = {field: True}
    feature_list = collection_feature_selection.find_one(tag_filter, projection_filter)
    if feature_list is None:
        return list()

    feature_list = list(feature_list[field].keys())

    for f in feature_list:
        result[f] = 0

    tag_filter = {'tagname': tag}
    to_select = collection_feature_selection.find_one(tag_filter, projection_filter)
    to_select = to_select[field]

    if len(to_select) == 0:
        return list()

    # max_value = max([to_select[k]['J_value'] for k in feature_list])
    # if max_value == np.inf:
    #    max_value = 10

    for f in feature_list:
        result[f] += to_select[f]['J_value'] #/ max_value

    result = sorted(result, key=result.get, reverse=True)

    return result[:n_features]


def round_for_scale(number):
    if abs(number) > 1:
        number_str = str(int(number))
        n = len(number_str)
        if n > 3:
            scale = int(number / (10 ** (n - 2))) * 10 ** (n - 2)
        else:
            scale = int(number)
        return scale

    else:
        return number


def order_index(df):
    df_s = df.copy()
    df_s['sum'] = 0.1 * df[[0, 6, 7, 8, 10, 11, 14, 15, 21, 22]].sum(axis=1) \
                  + 0.4 * df[[0, 1, 13, 14, 19, 21, 23]].sum(axis=1)
    df_s.sort_values(by=['sum'], inplace=True)

    return df_s.index


def order_all_index(df):
    df_s = df.copy()
    df_s['sum'] = df.sum(axis=1)
    df_s.sort_values(by=['sum'], inplace=True)

    return df_s.index


def get_tag_list_tag_alias_category_name(collection_metadata, collection_daily_vector, selected_category,
                                         category_list_name='R_category_list'):
    filter_query = {'document': 'description'}
    cursor = collection_daily_vector.find_one(filter_query)
    category_list = cursor[selected_category][category_list_name]
    categories = sorted(set([x[:3] for x in category_list]))
    aux_tag_list = cursor[selected_category]['tag_list']
    tag_list = dict()
    tag_alias = dict()
    category_name = dict()
    for suf in categories:
        tag_list[suf] = list()
        cat_list = [x for x in category_list if x[:3] == suf]
        for cat in cat_list:
            for tag in aux_tag_list[cat]:
                tag_list[suf].append(tag)
                category_name[suf] = get_category(collection_metadata, tag)
                tag_alias[tag] = get_alias(collection_metadata, tag)

    return tag_list, tag_alias, category_name, categories


def chunkIt(seq, num):
    if num > 0:
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out
    else:
        return []


def get_lol_upl(collection, tag):
    filter_query = {'tagname': tag}
    general_std = collection.find_one(filter_query)
    min_v = general_std['lol']
    max_v = general_std['upl']
    return min_v, max_v


def get_limit_by_quartile(df, quantiles, limit=1):
    # limit = 1 implies the best polinomial regression when the percentil is 100%
    # limit = 0 implies the best polinomial regression when the percentil is 0%

    dy = df.quantile(quantiles)
    y = np.array(sorted(dy.values))
    x = np.array(quantiles)

    if y[0] == y[-1]:   # There are no changes, therefore y = b
        coeff = np.array([0, y[0]])
    else:               # There are changes, therefore y = mx + b
        coeff = np.polyfit(x, y, 1)

    ft = np.poly1d(coeff)

    return ft(limit)


def get_comfort_bin(temp_r, temp_o_m, out_comfort=0, in_comfort=1):
    m1 = 2 / 5.5  # slope 1
    m2 = 1.5 / 4.5  # slope 2

    b1 = 26.5 - m1*(17.5)
    b2 = 22 - m2*(23.5)

    c1 = out_comfort  # out - of - comfort value
    c2 = in_comfort  # in -comfort value

    comfort = c1  # comfort value: 0 outside, 1 inside

    # check borders
    if temp_o_m < 12:
        if 24.5  > temp_r >= 20.5:
            comfort = c2

    elif 17.5 > temp_o_m >= 12:
        y1 = m1 * temp_o_m + b1
        if y1 > temp_r >= 20.5:
            comfort = c2
    elif 19 > temp_o_m >= 17.5:
        if 26.5  > temp_r >= 20.5:
            comfort = c2
    elif 23.5 > temp_o_m >= 19:
        y2 = m2*temp_o_m + b2
        if y2 <= temp_r < 26.5 :
            comfort = c2

    elif temp_o_m >= 23.5:
        if 26.5  > temp_r >= 22:
            comfort = c2
        else:
            comfort = c1
    else:
        comfort = c1

    if comfort != c1 and comfort != c2:
        print("Possible error for {0} : comfort {1}".format(temp_o_m, comfort))

    return comfort



def get_tag_values_per_weekday(collection, projection_query, start_time, end_time):
    """
    Returns a list of dataframes that contains the measures per day
    :param collection: Is the collection to make the request
    :param projection_query: Selection of tagnames
    :param start_time: Datetime indicates the date to start
    :param end_time: Datetime indicates the date to end
    :return: List of Dataframes from [0-6] -> [Monday-Sunday]
    """
    assert isinstance(start_time, datetime.datetime)
    assert isinstance(end_time, datetime.datetime)
    if start_time > end_time:
        return []

    list_df = list()
    # Loop for all the day: Monday to Sunday
    for weekday in range(1, 8):
        # get periods of time:
        if weekday >= start_time.isoweekday():
            delta = weekday - start_time.isoweekday()
            ini_day = start_time + pd.DateOffset(days=delta)
        else:
            delta = (7 - start_time.isoweekday()) + weekday
            ini_day = start_time + pd.DateOffset(days=delta)
        date_range = pd.date_range(ini_day, end_time, freq='7D')
        str_reg_exp_time = str(date_range[0].date())
        for date in date_range[1:]:
            str_reg_exp_time = str_reg_exp_time + '|' + str(date.date())
        # get query time
        regex_query = dictionary_time(str_reg_exp_time)
        # get values
        df = get_tag_values(collection, regex_query, projection_query, series_format='DF_t')
        df['hour'] = df.index.hour
        df['week_of_year'] = df.index.weekofyear
        list_df.append(df)

    return list_df


def concat_values(data_frame_t, collection, projection_dict):
    """
    Takes a dataframe which indexes are expressed as datetime (ordered) and make a intersection
      with respect their indexes over the collection
    :param data_frame_t: Sorted dataframe by datetime index
    :param collection: Requested collection
    :param projection_dict: Dictionary that allows to concat the columns
    :return: DataFrame
    """
    # print(type(data_frame_t.index[0]))
    start_time = pd.to_datetime(data_frame_t.index[0])
    end_time = pd.to_datetime(data_frame_t.index[-1])
    range_epoch_query = dict_range_time(start_time, end_time)
    project_values = get_tag_values(collection, range_epoch_query, projection_dict, series_format='DF_t')
    df_result = pd.concat([data_frame_t, project_values], axis=1, join_axes=[data_frame_t.index])
    # df_result = df_result.dropna(inplace=True)
    # print(df_result.head(6))
    return df_result


def dict_range_time(start_time, end_time, format_time='%Y-%m-%d %H:%M:%S'):
    """
    Gets a query based on epoch
    :param start_time: String in format %Y-%m-%d or datetime
    :param end_time: String in format %Y-%m-%d or datetime
    :param format_time:  any supported format for datetime
    :return: dictionary: {'epoch':{'$gte': start_time,'$lte':end_time}}
    """
    try:
        start_time = datetime.datetime.strptime(str(start_time), format_time)
        end_time = datetime.datetime.strptime(str(end_time), format_time)
    except AttributeError:
        try:
            start_time = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
            end_time = datetime.datetime.strptime(str(end_time), '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            print("Observe the type of start_time: ", type(start_time))
            print("Observe the type of end_time: ", type(end_time))

    start_time = start_time.timestamp()
    end_time = end_time.timestamp()

    query_range = {'epoch': {'$gte': start_time, '$lte': end_time}}
    return query_range


def get_next_weekday(datetime_date, day_name_target):
    """
    Returns the closets date that correspond with the day_day_name_target
    :param datetime_date:  datetime_date
    :param day_name_target: 'Monday', 'Tuesday', etc.
    :return: datetime corresponding to day_day_name_target
    """
    # next_day = datetime_date()
    day_list = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    idx_target = day_list.index(day_name_target)
    weekday_date = datetime_date.isoweekday()
    if idx_target >= weekday_date:
        delta = idx_target - weekday_date
        next_day = datetime_date + pd.DateOffset(days=delta)
    else:
        delta = (7 - weekday_date) + idx_target
        next_day = datetime_date + pd.DateOffset(days=delta)

    return next_day.to_pydatetime()


def get_last_weekday(datetime_date, day_name_target):
    """
    Returns the closets date that correspond with the day_day_name_target
    :param datetime_date:  datetime_date
    :param day_name_target: 'Monday', 'Tuesday', etc.
    :return: datetime corresponding to day_day_name_target
    """
    # next_day = datetime_date()
    day_list = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    idx_target = day_list.index(day_name_target)
    weekday_date = datetime_date.isoweekday()
    if idx_target <= weekday_date:
        delta = idx_target - weekday_date
        last_day = datetime_date + pd.DateOffset(days=delta)
    else:
        delta = (7 - weekday_date) + idx_target
        last_day = datetime_date + pd.DateOffset(days=delta)

    return last_day.to_pydatetime()


def conv_hour(int_hour):
    """
    Transform int in hour string
    :return: string
    """
    if int_hour <= 9:
        str_hour = '0' + str(int_hour)
    else:
        str_hour = str(int_hour)
    return str_hour


def hmm_colour(int_hour):
    if int_hour <= 7:
        return 'Non occupied period 1'
    if int_hour == 8:
        return 'Arriving period'
    if int_hour >= 9 and int_hour <= 11:
        return 'First working period'
    if int_hour >= 12 and int_hour <= 16:
        return 'Second working period'
    if int_hour == 17:
        return 'Leaving period'
    else:
        return 'Non occupied period 2'


def month_name(int_month):
    if isinstance(int_month, int):
        if int_month > 12: return 'No valid'
        month_names = ['Jan.', 'Feb.', 'Mar.', 'Apr.', 'May.', 'Jun.', 'Jul.',
                       'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.']
        return month_names[int_month - 1]
    else:
        return 'no_month'


def filter_dictionary(dict_to, v1):
    result = dict()
    for k in dict_to.keys():
        if abs(dict_to[k]) >= v1:
            result[k] = dict_to[k]
    return result


def validate_time_series_request():
    # Getting the request
    list_projection = request.args.get('list_projection')
    time_query = request.args.get('time_query')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    json_format = request.args.get('json_format')
    ##############################################################
    # TODO: Use default values for:
    if list_projection is None:
        list_projection = ['V050_room202_temp', 'V022_vent02_CO2']
    else:
        list_projection = list_projection.split(',')
    if time_query is not None:
        time_query = dictionary_time(time_query)
    else:
        start_time, end_time = validate_time(start_time, end_time)
        time_query = dict_range_time(start_time, end_time)

    if json_format is None:  # possible options: list, records
        json_format = 'records'
    ##############################################################
    return list_projection, time_query, start_time, end_time, json_format


##############################################################


def validate_time(start_time, end_time):
    # TODO: Use default values for:
    if (start_time is None) or (end_time is None):
        start_time = datetime.datetime(2013, 1, 1, 0)
        end_time = datetime.datetime(2015, 5, 8, 23)
    else:
        # format_time = '%Y-%m-%d %H:%M:%S';
        format_time = '%Y-%m-%d'
        try:
            start_time = datetime.datetime.strptime(start_time, format_time)
            end_time = datetime.datetime.strptime(end_time, format_time)
        except ValueError:
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    return start_time, end_time

## This section is devoted to plot in Jupyter


def plot_trend_with_fringes(axes, start_time, end_time, collection, tag, n_tick=6,
                            y_to=None, y_lim=None, date_format='%Y-%m-%d', collection_metadata=None):

    if not collection_metadata is None:
        units = get_field_value(collection_metadata, {'tagname': tag}, 'units')
        axes.set_ylabel('[ ' +  units + ' ]')
    time_query = dict_range_time(start_time, end_time, date_format)
    timeline = pd.date_range(start_time, end_time, freq='1D')

    df_x = get_tag_values(collection, time_query, tag, series_format='DF_t')
    df_x[tag] = pd.to_numeric(df_x[tag], errors='coerce')
    df_x[tag] = df_x[tag].fillna(df_x[tag].interpolate())
    axes.plot(df_x)

    # update limits:
    dy = df_x[tag].max() - df_x[tag].min()
    if y_lim is None:
        y_lim = [df_x[tag].min() - 0.02 * dy, df_x[tag].max() + 0.1 * dy]

    y_tick = np.linspace(y_lim[0], y_lim[-1], n_tick)
    axes.set_yticks(y_tick)
    # create fringes per day:
    ax2 = axes.twiny()
    labels_x = [t1.weekday_name[:3] for t1 in timeline]
    id_x = range(len(labels_x))
    ax2.set_xticks(id_x)
    ax2.set_xticklabels(labels_x[:-1])

    plot_day_name(axes, timeline)


def plot_day_name(axes, timeline):
    j = 0
    for t1, t2 in zip(timeline, timeline[1:]):
        #axes.annotate(t1.weekday_name[:3], xy=(t1, y_value), xytext=(t1, y_value), clip_on=True)
        if j % 2 == 1:
            axes.axvspan(t1, t2, facecolor='g', alpha=0.1)
        j += 1


def plot_profiles(axes, model, list_to_plot, n_col =3, ylabel=None, ylim=None):

    i, j = 0, 0
    x = range(24)
    if ylim is None:
        max_v, min_v = max_min(model, list_to_plot)
        dv = max_v - min_v
        # print(dv)
        ylim = [min_v - 0.1*dv, max_v + 0.1*dv]

    for n in list_to_plot:
        y = model.means_[n]
        e = (np.diag(model.covars_[n]))**(0.5)
        if j>n_col-1:
            j=0
            i+=1
        axes[i][j].errorbar(x, y, e, linestyle='None', marker='o')
        axes[i][j].set_xticks(x)
        axes[i][j].set_xlim([0,23])
        axes[i][j].set_xticklabels(labels = x, rotation=-90)
        axes[i][j].set_ylim(ylim)
        axes[i][j].set_title('ID_candidate= ' + str(n))
        axes[-1][j].set_xlabel('hours')
        if ylabel is not None:
            axes[i][0].set_ylabel(ylabel)
        j+=1
        #print(e)


def max_min(model, list_to_plot):
    max_v, min_v = -np.inf, np.inf
    for n in list_to_plot:
        y = model.means_[n]
        if max_v < max(y):
            max_v = max(y)
        if min_v > min(y):
            min_v = min(y)

    return max_v, min_v

def test():
    print("This is working now =)")
