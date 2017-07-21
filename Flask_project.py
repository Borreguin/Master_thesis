import flask
from flask import render_template
from flask import request
from pymongo import MongoClient
import json
import datetime
import pandas as pd
import math
from bson import json_util
from collections import OrderedDict
# from bson.json_util import dumps
import sys

sys.path.append("/home/ubuntu/Desktop/Thesis_project/lib")
sys.path.append("/home/ubuntu/anaconda3")
import rs_common_framework_v4 as rs
import collections
# from sklearn.externals import joblib
from sklearn.cluster import KMeans
import numpy as np

app = flask.Flask(__name__)
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
db_name = 'project_db'
collection_metadata = connection[db_name]["metadata"]
# collection_series = connection[db_name]["ede2"]
collection_series = connection[db_name]["filtered_time_series"]
collection_register = connection[db_name]["filter_register"]
collection_detection_flat = connection[db_name]["detection_flat"]
collection_detection_outlier = connection[db_name]["detection_outlier"]
collection_detection_nan = connection[db_name]["detection_nan"]
collection_correlation = connection[db_name]["correlation_matrix_daily"]
collection_daily_vector = connection[db_name]["vector_daily"]
collection_statistics_general = connection[db_name]["statistics_general"]
#collection_sax = connection[db_name]["comfort_room_sax"]
collection_sax = connection[db_name]["sax_daily"]

# Define the path for the HMM_models
path_model = '/home/ubuntu/Desktop/Thesis_project/HMM_models/'


# This is the presentation web
@app.route('/')
def default():
    return render_template("menu.html")
    # return 'This is a project for enhance building data visualization!'


@app.route('/menu')
def menu():
    return render_template("menu.html")


@app.route('/record')
def list_record():
    return render_template("record.html")


@app.route('/par_coord')
def par_coord():
    return render_template('par_coord.html')


@app.route('/par_coord_v2')
def par_coord_v2():
    return render_template('par_coord_v2.html')


@app.route('/par_coord2')
def par_coord2():
    return render_template('par_coord2.html')


@app.route('/quality')
def quality():
    return render_template('quality.html')


@app.route('/correlation')
def correlation():
    return render_template('edge.html')


@app.route('/correlation_table')
def correlation_table():
    return render_template('correlation_table.html')


@app.route('/scatter')
def scatter():
    return render_template('scatter.html')


@app.route('/sankey')
def sankey_diagram():
    return render_template('sankey_heatmap.html')


@app.route('/profiles')
def profiles():
    return render_template('profiles.html')

@app.route('/list')
def list_list():
    # TODO: Remove this example
    return render_template("list.html")


@app.route('/test')
def test2():
    # TODO: Remove this example
    return render_template("test.html")


@app.route("/project")
# TODO: Remove this example
# this is the first example
def index():
    return render_template("index.html")


@app.route('/metadata')
def metadata():
    projection = request.args.get('list_projection')
    query = request.args.get('query')
    json_format = request.args.get('json_format')
    ##########################################################################
    # TODO: Put the values by default
    if projection is None:
        projection = rs.projection(['category', 'id'],
                                   _id=False)
    else:
        projection = projection.split(',')
        projection.append('id')
        projection = rs.projection(projection, _id=False)
    if query is None:
        query = rs.dictionary_query('')
    else:
        query = rs.dictionary_query(query)
    if json_format is None:  # possible options: list, records
        json_format = 'records'
    ##########################################################################

    cursor = collection_metadata.find(query, projection)
    df = pd.DataFrame(list(cursor))
    df.sort_values(['id'], inplace=True)
    if df.empty: print(query, projection)
    del df['id']
    df.drop_duplicates(inplace=True)
    values = df.to_dict(json_format)
    json_values = json.dumps(values, sort_keys=True)
    return json_values


@app.route('/data')
def get_data():
    """
    Getting data from the server, send the values as JSON format
    it could be either in a list form or records
    :return:
    """
    # Getting the request
    list_projection, time_query, start_time, end_time, json_format = rs.validate_time_series_request()
    projection = rs.projection(list_projection, _id=False)

    # getting values from the MongoDB server
    values = rs.get_tag_values(collection_series, time_query, projection, series_format='DF_idx')
    for tag in list_projection:
        values[tag] = pd.to_numeric(values[tag], errors='coerce')
    values.dropna(inplace=True)
    values = values.round(2)

    # send the data using json:
    values = values.to_dict(json_format)
    json_values = json.dumps(values, sort_keys=True)
    connection.close()
    print('Send:' + str(time_query) + str(projection))
    print(len(json_values))
    return json_values


@app.route('/data_hour_day')
def get_data_hour_day():
    """
    Getting data from the server, send the values as JSON format
    it could be either in a list form or records
    :return:
    """
    # Getting the request
    list_projection, time_query, start_time, end_time, json_format = rs.validate_time_series_request()
    ##############################################################

    tag_list = list_projection

    # Days of the week
    keys = ["Hour", 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # get the closets Monday
    start_time = rs.get_next_weekday(start_time, 'Monday')
    end_time = rs.get_next_weekday(end_time, 'Monday')
    # weeks = range(start_time.isocalendar()[1], end_time.isocalendar()[1])
    days_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # Initialization:
    # create syntax query based on epochs
    query_time = rs.dict_range_time(start_time, end_time)

    register = dict()
    for tag in tag_list:
        register[tag] = []
    df_result = pd.DataFrame()

    for tag in tag_list:
        init = True
        projection = {}
        for day_name in days_name:
            query_time['weekday_name'] = day_name
            projection['belongs_to'] = True
            projection[tag] = True
            df = rs.get_tag_values(collection_series, query_time, projection, series_format='DF_t')
            if init:
                df_aux = pd.DataFrame()
                df_aux['Hour'] = df.index.hour
                df_aux.index = list(df['belongs_to'])
                df_result = df_aux.copy()
                init = False
            df.set_index(keys=['belongs_to'], inplace=True)
            df.rename(columns={tag: day_name}, inplace=True)
            df_result = pd.concat([df_result, df], axis=1, join_axes=[df.index])

        df_result = df_result.astype('float64', raise_on_error=False)
        df_result = df_result.round(2)
        df_result.dropna(inplace=True)
        values = [OrderedDict(row) for i, row in df_result.iterrows()]
        register[tag] = values
        # print(df_result.info())
        # print(df_result.tail(4))

    # query = rs.dict_range_time(start_time, end_time)
    query = dict()
    for tag in tag_list:
        query['tagname'] = tag
        projection = rs.projection(['UCL', 'LCL'])
        df_limits = rs.get_tag_values(collection_register, query, projection, series_format='DF_t')
        # print(df_limits)
        if not df_limits.empty:
            ucl = round(df_limits['UCL'].max(), 2)
            lcl = round(df_limits['LCL'].min(), 2)
            values_ucl = [24] + [ucl] * 7
            values_lcl = [0] + [lcl] * 7
            m1 = zip(keys, values_ucl)
            m2 = zip(keys, values_lcl)
            register[tag].append(OrderedDict(m1))
            register[tag].append(OrderedDict(m2))
            # print("UCL,LCL",UCL,LCL)

    print(len(register[tag_list[0]]))
    register['keys'] = keys
    json_values = json.dumps(register)
    connection.close()
    return json_values


@app.route('/data_hmm')
def get_data_hmm():
    """
    Getting data from the server, send the values as JSON format
    it could be either in a list form or records
    :return:
    """
    # Getting the request
    projection = request.args.get('list_projection')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    ##############################################################
    # TODO: Use default values for:
    if projection is None:
        # list_selected_buttons = ['V050_room202_temp','V022_vent02_CO2']
        tag_list = ['V022_vent02_CO2']
        projection = rs.projection(tag_list, _id=False)
    else:
        projection = projection.split(',')
        tag_list = projection
        projection = rs.projection(projection, _id=False)

    start_time, end_time = rs.validate_time(start_time, end_time)

    ##############################################################
    # keys to send
    keys = ["Hour", "name", "group", 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    days_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # get the closets Monday
    start_time = rs.get_next_weekday(start_time, 'Monday')
    end_time = rs.get_next_weekday(end_time, 'Monday')
    # weeks = range(start_time.isocalendar()[1], end_time.isocalendar()[1])

    # create syntax query based on epochs
    query_time = rs.dict_range_time(start_time, end_time)
    projection['belongs_to'] = True
    register = dict()
    for tag in tag_list:
        register[tag] = []
    df_result = pd.DataFrame()

    for tag in tag_list:
        init = True
        for day_name in days_name:
            query_time['weekday_name'] = day_name
            df = rs.get_tag_values(collection_series, query_time, projection, series_format='DF_t')
            if init:
                df_aux = pd.DataFrame()
                df_aux['Hour'] = df.index.hour
                df_aux.index = list(df['belongs_to'])
                df_aux['name'] = list(df['belongs_to'])
                df_aux['group'] = df_aux['Hour'].apply(rs.hmm_colour)
                df_result = df_aux.copy()
                init = False
            df.set_index(keys=['belongs_to'], inplace=True)
            df.rename(columns={tag: day_name}, inplace=True)
            df_result = pd.concat([df_result, df], axis=1, join_axes=[df.index])

        df_result = df_result.astype('float64', raise_on_error=False)
        df_result = df_result.round(2)
        df_result.dropna(inplace=True)
        values = [OrderedDict(row) for i, row in df_result.iterrows()]
        register[tag] = values

    query = dict()
    for tag in tag_list:
        query['tagname'] = tag
        projection = rs.projection(['UCL', 'LCL'])
        df_limits = rs.get_tag_values(collection_register, query, projection, series_format='DF_t')
        # print(df_limits)
        if not df_limits.empty:
            ucl = round(df_limits['UCL'].mean(), 2)
            lcl = round(df_limits['LCL'].mean(), 2)
            values_ucl = [23] + ['UCL'] + ['UCL'] + [ucl] * 7
            values_lcl = [0] + ['LCL'] + ['LCL'] + [lcl] * 7
            m1 = zip(keys, values_ucl)
            m2 = zip(keys, values_lcl)
            register[tag].append(OrderedDict(m1))
            register[tag].append(OrderedDict(m2))

    print(len(register[tag_list[0]]))
    register['keys'] = keys
    json_values = json.dumps(register)
    connection.close()
    return json_values


@app.route('/quality_data')
def quality_data():
    # status values:
    # normal = 0  # this is by default
    flat = 1
    outlier = 2
    nan = 3
    #####################

    tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()
    tag_list = rs.get_tag_names(collection_metadata, '{}')
    projection = rs.projection(tag_list, _id=False)

    # order variables according to the statistics (number of detections)
    # ordering tags by number of detections, the biggest one is the last one
    query = {'document': 'statistics'}
    st_nan = pd.DataFrame(list(collection_detection_nan.find(query, projection)))
    st_flat = pd.DataFrame(list(collection_detection_flat.find(query, projection)))
    number_detections = {}
    for tag in tag_list:
        number_detections[tag] = (rs.get_category(collection_metadata, tag), st_flat[tag][0], st_nan[tag][0])
    # print(number_detections[tag])
    tag_list = sorted(number_detections, key=number_detections.__getitem__, reverse=False)

    time_line = list(pd.date_range(start_time, end_time, freq='1D'))
    time_line = [x._date_repr for x in time_line]

    records = list()

    # processing flat detections:
    projection = {'_id': False, 'epoch': False, 'percentil': False}
    time_query_1 = time_query.copy()
    time_query_1['percentil'] = 1
    list_documents = list(collection_detection_flat.find(time_query_1, projection))
    for register in list_documents:
        for k in register.keys():
            if k in tag_list:
                record = {'date': time_line.index(register['timestamp'])}
                record['tag'] = tag_list.index(k)
                record['value'] = flat
                records.append(record)

    # processing outlier detections:
    projection = {'_id': False, 'tagname': True, 'timestamp': True}
    time_query_1 = time_query.copy()
    list_documents = list(collection_detection_outlier.find(time_query_1, projection))
    for register in list_documents:
        if register['tagname'] in tag_list:
            aux = register['timestamp'].split(' ')
            record = {'date': time_line.index(aux[0])}
            record['tag'] = tag_list.index(register['tagname'])
            record['value'] = outlier
            records.append(record)

    # processing nan detections:
    projection = {'_id': False, 'epoch': False}
    time_query_1 = time_query.copy()
    list_documents = list(collection_detection_nan.find(time_query_1, projection))
    for register in list_documents:
        for k in register.keys():
            if k in tag_list:
                record = {
                    'date': time_line.index(register['timestamp']),
                    'tag': tag_list.index(k),
                    'value': nan
                }
                records.append(record)

    result = dict()
    result['data'] = records
    result['tags'] = [rs.get_alias(collection_metadata, t) for t in tag_list]
    result['timestamp'] = time_line
    json_values = json.dumps(result)
    connection.close()
    return json_values


@app.route('/correlation_data')
def correlation_data():
    tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()

    filter_category = request.args.get('filter_category')
    if filter_category is None:
        tag_list = rs.get_tag_names(collection_metadata, '{}')
    else:
        query_category = {'category': filter_category}
        tag_list = rs.get_tag_names(collection_metadata, query_category)

    registers = {'data': []}

    for tag in tag_list:
        correlation_dict = dict()
        times = dict()
        # date = dict()
        time_query['tagname'] = tag
        dict_list = list(collection_correlation.find(time_query, {"_id": False}))

        for d in dict_list:
            correlation_list = d['correlation_list']
            for t in correlation_list:
                if t in correlation_dict.keys():
                    correlation_dict[t] = correlation_dict[t] + d[t]
                    times[t] += 1
                else:
                    correlation_dict[t] = d[t]
                    times[t] = 1

        result = dict()
        result['imports'] = []
        result['name'] = rs.get_category(collection_metadata, tag) + '#' + rs.get_alias(collection_metadata, tag)
        result['tagname'] = tag
        for idx in correlation_dict.keys():
            #if times[idx] >= len(dict_list) * 0.05:
            name_v = rs.get_category(collection_metadata, idx) + '#' + rs.get_alias(collection_metadata, idx)
            result['imports'] += [name_v]
            #result[name_v] = round(correlation_dict[idx] / times[idx], 2)
            result[name_v] = round(correlation_dict[idx] / len(dict_list), 2)

        registers['data'].append(result)

    start_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    registers["start_time"] = start_time._date_repr
    registers["end_time"] = end_time._date_repr
    json_values = json.dumps(registers)
    connection.close()
    return json_values


@app.route('/correlation_table_data')
def correlation_table_data():
    tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()

    filter_category = request.args.get('filter_category')
    if filter_category is None:
        filter_list = rs.get_tag_names(collection_metadata, '{}')
    else:
        query_category = {'category': filter_category}
        filter_list = rs.get_tag_names(collection_metadata, query_category)

    tag = tag_list[0]
    correlation_dict = dict()
    times = dict()
    time_query['tagname'] = tag
    dict_list = list(collection_correlation.find(time_query, {"_id": False}))
    timestamp = rs.get_tag_values(collection_correlation, time_query, {'timestamp': True}, series_format='DF_t')
    timestamp = [x._date_repr for x in timestamp.index]
    for d in dict_list:
        correlation_list = d['correlation_list']
        for t in correlation_list:
            if t in correlation_dict.keys():
                correlation_dict[t] = correlation_dict[t] + d[t]
                times[t] += 1
            else:
                correlation_dict[t] = d[t]
                times[t] = 1

    for t in correlation_dict.keys():
        correlation_dict[t] = correlation_dict[t] / times[t]

    # correlation_dict = filter_dictionary(correlation_dict,0.50)

    to_sort = dict()
    filter_list = list(set(correlation_dict.keys()) & set(filter_list))
    for t in filter_list:
        to_sort[t] = (rs.get_category(collection_metadata, t), times[t], abs(correlation_dict[t]))

    correlation_list = sorted(to_sort, key=to_sort.get, reverse=True)

    result = dict()
    result['tagname'] = tag
    result['tagname_list'] = correlation_list
    result['tag_alias_list'] = [rs.get_alias(collection_metadata, x) for x in correlation_list]
    result['timestamp'] = timestamp
    result['title'] = rs.get_category(collection_metadata, tag) + ' / ' + rs.get_alias(collection_metadata, tag)
    result['data'] = []
    for d in dict_list:
        c_list = d['correlation_list']
        for tag in c_list:
            if tag in correlation_list:
                aux = {
                    'date': timestamp.index(d['timestamp']),
                    'tag': correlation_list.index(tag),
                    'value': round(d[tag], 2)
                }
                result['data'].append(aux)

    json_values = json.dumps(result)
    connection.close()
    return json_values


@app.route('/category_data_vector')
def category_data_vector():
    selected_category = request.args.get('selected_category')
    category_list_name = request.args.get('category_list')

    tag_list, tag_alias, category_name, categories = \
        rs.get_tag_list_tag_alias_category_name(
            collection_metadata, collection_daily_vector,
            selected_category, category_list_name)

    json_values = {
        'tag_list': tag_list,
        'category_list': categories,
        'category_name': category_name,
        'tag_alias': tag_alias
    }

    json_values = json.dumps(json_values)
    connection.close()
    return json_values


@app.route('/sankey_data')
def sankey_data():
    package = request.args.get('package')
    if package is not None:
        package = json.loads(package)
        season = package['season']
        day_type = package['day_type']
        selected_category = package['selected_category']
        start_time = package['start_time']
        end_time = package['end_time']
        time_query = rs.dict_range_time(start_time, end_time, format_time='%Y-%m-%dT%H:%M:%S.%fZ')
        h_pref = [''] + [x + '-' for x in sorted(package['categories'])]
    else:
        season = ['winter']
        day_type = ['working_day']
        selected_category = 'A'
        tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()
        h_pref = ['', 'A-', 'A_3-', 'A_4-', 'A_6-']

    if len(h_pref) <= 1:
        h_pref = ['', 'A-', 'A_3-', 'A_4-', 'A_6-']

    field_name = 'R_sax_word'
    projection_query = {field_name: 'True'}

    filter_query = {"breakout_group": selected_category,
                    'day_type': {'$in': day_type},
                    'seasonal_label': {'$in': season}}
    filter_query.update(time_query)

    df_sax_word = rs.get_tag_values(collection_daily_vector, filter_query, projection_query)

    if len(df_sax_word.index) == 0:
        return '{}'

    nodes, links, sax_words = rs.nodes_and_links(df_sax_word[field_name], h_pref)
    json_values = {
        'nodes': nodes,
        'links': links,
        'sax_words': sax_words
    }

    json_values = json.dumps(json_values)
    connection.close()
    return json_values


@app.route('/heatmap_sax_data')
def heatmap_sax_data():
    max_cluster_number = 25
    package = request.args.get('package')
    filter_sax = request.args.get('filter_sax')
    if package is not None:
        package = json.loads(package)
        season = package['season']
        day_type = package['day_type']
        selected_category = package['selected_category']
        start_time = package['start_time']
        end_time = package['end_time']
        tag_list = package['tag_list']
        time_query = rs.dict_range_time(start_time, end_time, format_time='%Y-%m-%dT%H:%M:%S.%fZ')
        categories = sorted(set(package['tag_list'].keys()) - set(selected_category))
    else:
        season = ['winter']
        day_type = ['working_day']
        selected_category = 'A'
        filter_sax = 'bbc'
        tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()

    if len(tag_list) < 3:
        tag_list, tag_alias, category_name, categories = \
            rs.get_tag_list_tag_alias_category_name(
                collection_metadata, collection_daily_vector,
                selected_category, 'R_category_list')
        tag_list = [v[0] for k, v in tag_list.items()]
        categories = sorted(set(categories) - set(selected_category))

    filter_query = {"breakout_group": selected_category,
                    'day_type': {'$in': day_type},
                    'seasonal_label': {'$in': season}
                    }
    filter_query.update(time_query)
    field_name = 'R_sax_word'
    tag = tag_list[selected_category]

    profiles_data, mask_date = rs.get_sax_profiles(collection_daily_vector, filter_query, field_name, filter_sax,
                                        collection_series, tag, max_cluster_number)

    chunk_list = profiles_data['days']
    min_v, max_v = rs.get_lol_upl(collection_statistics_general, tag)

    register = {
        'min': min_v,
        'max': max_v
    }
    register.update(profiles_data)

    result = {selected_category: register}

    # get the data from the collection series for each varaible
    # and keep in memory as a dictionary of DataFrames
    df_dict = dict()
    min_value = dict()
    max_value = dict()

    for cat in categories:
        # for tag in tag_list[cat]:
        tag = tag_list[cat]
        df = rs.get_tag_pivoted_values(collection_series, mask_date, tag)
        df.replace('NaN', 0, inplace=True)
        df_dict[tag] = df

        min_v, max_v = rs.get_lol_upl(collection_statistics_general, tag)
        max_value[cat] = rs.round_for_scale(max_v)
        min_value[cat] = rs.round_for_scale(min_v)

    for cat in categories:

        register = {
            'data': [],
            'min': min_value[cat],
            'max': max_value[cat],
            'days': chunk_list
        }

        for chunk, idx in zip(chunk_list, range(len(chunk_list))):

            tag = tag_list[cat]
            df = df_dict[tag].loc[chunk]
            df_result = df.mean(axis=0)
            for col, idy in zip(df_result, range(len(df_result))):
                register['data'].append(
                    {'day': idx, 'hour': idy,
                     'value': round(col, 0)})

        result[cat] = register
    result['sax_word'] = filter_sax;
    json_values = json.dumps(result)
    connection.close()
    return json_values


@app.route('/profile_data')
def profile_data():
    max_cluster_number = 20
    package = request.args.get('package')
    if package is not None:
        package = json.loads(package)
        season = package['season']
        day_type = package['day_type']
        start_time = package['start_time']
        end_time = package['end_time']
        tag = package['tag_list']
        time_query = rs.dict_range_time(start_time, end_time, format_time='%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        season = ['winter']
        day_type = ['working_day']
        tag_list, time_query, start_time, end_time, json_format = rs.validate_time_series_request()
        tag = tag_list[0]

    filter_query = {
        'day_type': {'$in': day_type},
        'seasonal_label': {'$in': season}
    }
    filter_query.update(time_query)

    df_sax = rs.get_tag_values(collection_sax, filter_query, tag, series_format='DF_t')
    if len(df_sax.index) == 0:
        return '{}'

    nodes, links, sax_words = rs.nodes_and_links(df_sax[tag], ['', 'prof-'])
    sankey_data = {
        'nodes': nodes,
        'links': links,
        'sax_words': sax_words
    }

    heatmap_data = dict()

    units = rs.get_field_value(collection_metadata, {'tagname': tag}, 'units')
    for filter_sax in sax_words:

        profiles_data, mask_date = rs.get_sax_profiles(collection_sax, filter_query, tag, filter_sax,
                                                       collection_series, tag, max_cluster_number)

        min_v, max_v = rs.get_lol_upl(collection_statistics_general, tag)

        register = {
            'min': min_v,
            'max': max_v,
            'units': units
        }
        register.update(profiles_data)
        heatmap_data[filter_sax] = register


    json_values = {
        'sankey_data': sankey_data,
        'heatmap_data': heatmap_data,
        'units': units
    }

    json_values = json.dumps(json_values)
    connection.close()
    return json_values


if __name__ == '__main__':
    app.run()

