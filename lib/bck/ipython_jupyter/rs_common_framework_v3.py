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


def __get_parameters(option='default'):
    # DEFAULT PARAMETERS
    dbs_name = 'Roberto'
    collection_metadata = 'metadata'
    collection_series = 'ede2'
    collection_filter = 'no_collection'
    collection_register = 'no_collection'
    query_tag = "tagname: V032_outdoor_temp"
    query_time = '2013'
    # PARSE VALUES
    parser = argparse.ArgumentParser(description='Calculate statistics values of a dataframe and save over a csv FILE.')
    parser.add_argument('-file', type=str, default="output.csv", help='Output file name, Ex: file.csv')
    parser.add_argument('-db', type=str, default=dbs_name, help='Mongo Data Base Name')
    parser.add_argument('-mt', type=str, default=collection_metadata, help='MongoDB Collection Name for the metadata')
    parser.add_argument('-sr', type=str, default=collection_series, help='MongoDB Collection Name for the time series')
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
        filter_factor = 4.5
        parser.add_argument('-ft', type=float, default=filter_factor,
                            help='Factor for filtering, Ex: 4.5 (based on the Central Limit Theorem)')
        parser.add_argument('-it', type=int, default=number_interactions, help='Number of interactions, Ex: 2')
        parser.add_argument('-filtered', type=str, default=collection_filter,
                            help='MongoDB Collection Name for saving the filtered values')
        parser.add_argument('-clone', action='store_true', help='clone database mode')

    return parser.parse_args()


def get_tag_names(metadata_collection, srt_query):
    """
    Useful to get the tag names for the metadata
    :param metadata_collection: collection for the meatadata
    :param srt_query: format to use: type:Temperature
    :return: List of tag variables
    """

    tags = []
    projection_value = {'tagname': True, '_id': False}
    query = dictionary_query(srt_query)
    # getting name of tags
    cursor = metadata_collection.find(query, projection_value)
    df = pd.DataFrame(list(cursor))
    try:
        tags = list(df['tagname'])
    except KeyError:
        print("The query does not produce any results :\nCorrect format without spaces: f1:value1,f2:value2 \n",
              srt_query)
        print("Observe capital letters in names")
    return tags


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
            for x in projection_dict:
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
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        df['timestamp'] = [str(x) for x in df.index]
        return df

    if 'DF_idx' == series_format:

        # this is for numeric values
        try:
            df['timestamp'] = df_aux
            df = df.sort_values(['timestamp'], ascending=[1])
            df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
            df.set_index(keys='timestamp', inplace=True)
            df.sort_index(inplace=True)

        except AttributeError:
            print("The query does not produce any results: ")
            print("query", projection_dict, "\nfield", regex_query)
            print("Observe capital letters in names and the format of the time query \n\n")
            print(collection)
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


def concat_values(data_frame_idx, collection, projection_dict):
    """
    Takes a dataframe which indexes are expressed as datetime (ordered) and make a intersection
      with respect their indexes over the collection
    :param data_frame_idx: Sorted dataframe by datetime index
    :param collection: Requested collection
    :param projection_dict: Dictionary that allows to concat the columns
    :return: DataFrame
    """
    print(type(data_frame_idx.index[0]))
    start_time = pd.to_datetime(data_frame_idx.index[0])
    end_time = pd.to_datetime(data_frame_idx.index[-1])
    range_epoch_query = dict_range_time(start_time,end_time)
    project_values = get_tag_values(collection, range_epoch_query,projection_dict,series_format='DF_idx')
    print(data_frame_idx.info())
    print(project_values.info())
    print(range_epoch_query, projection_dict)
    df_result = pd.concat([data_frame_idx,project_values],axis=1)
    print(df_result.head(6))
    return  df_result

def dict_range_time(start_time, end_time, format_time='%Y-%m-%d'):
    """
    Gets a query based on epoch
    :param start_time: String in format %Y-%m-%d or datetime
    :param end_time: String in format %Y-%m-%d or datetime
    :param format_time:  any supported format for datetime
    :return: dictionary: {'epoch':{'$gte': start_time,'$lte':end_time}}
    """
    query_range = dict()
    if not isinstance(start_time, datetime.datetime):
        start_time = datetime.datetime.strptime(start_time, format_time)
    if not isinstance(start_time, datetime.datetime):
        end_time = datetime.datetime.strptime(end_time, format_time)

    start_time = start_time.timestamp()
    end_time = end_time.timestamp()

    query_range = {'epoch': {'$gte': start_time, '$lte': end_time}}
    return query_range


def get_weekday(datetime_date, day_name_target):
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


def conv_hour(int_hour):
    """
    Transform int in hour string
    :return: string
    """
    if int_hour < 9:
        str_hour = '0'+str(int_hour)
    else:
        str_hour = str(int_hour)
    return str_hour


def test():
    print("This is working now =)")
