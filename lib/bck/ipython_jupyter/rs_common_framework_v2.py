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
    DBS_NAME = "Roberto"
    COLLECTION_METADATA = "metadata"
    COLLECTION_SERIES = "ede2"
    COLLECTION_FILTER = "no_collection"
    COLLECTION_REGISTER = "no_collection"

    # query for tags
    # QUERY_TAG = "type: Temperature, location:  out"
    QUERY_TAG = "tagname: V032_outdoor_temp"
    # query for time
    QUERY_TIME = "2013"
    parser = argparse.ArgumentParser(description='Calculate statistics values of a dataframe and save over a csv FILE.')
    parser.add_argument('-file', type=str, default="output.csv", help='Output file name, Ex: file.csv')
    parser.add_argument('-db', type=str, default=DBS_NAME, help='Mongo Data Base Name')
    parser.add_argument('-mt', type=str, default=COLLECTION_METADATA, help='MongoDB Collection Name for the metadata')
    parser.add_argument('-sr', type=str, default=COLLECTION_SERIES, help='MongoDB Collection Name for the time series')
    parser.add_argument('-q', type=str, default=QUERY_TAG,
                        help='Query for tags Names using the Metadata DB. Ex: "type: Temperature, location : out')
    parser.add_argument('-t', type=str, default=QUERY_TIME,
                        help='Query for specify  the time.  Ex1: 2013 | 2014  Ex2: 06.2013')
    parser.add_argument('-v', action='store_true', help='Verbose mode')

    parser.add_argument('-save', type=str, default=COLLECTION_REGISTER,
                        help='Name of MongoDB collection to save the results')

    if option == 'filter':
        number_interactions = 2
        # factor for filtering information
        filter_factor = 4.5
        parser.add_argument('-ft', type=float, default=filter_factor,
                            help='Factor for filtering, Ex: 4.5 (based on the Central Limit Theorem)')
        parser.add_argument('-it', type=int, default=number_interactions, help='Number of interactions, Ex: 2')
        parser.add_argument('-filtered', type=str, default=COLLECTION_FILTER,
                            help='MongoDB Collection Name for saving the filtered values')
        parser.add_argument('-clone', action='store_true', help='clone database mode')

    return parser.parse_args()


def get_tag_names(collection, srt_query):
    # Useful to get tag names of the DataSeries
    # from the metadata base

    Tags = []
    projection = {'tagname': True, '_id': False}
    query = dictionary_query(srt_query)
    # gettting name of Tags
    cursor = collection.find(query, projection)
    df = pd.DataFrame(list(cursor))
    try:
        Tags = list(df['tagname'])
    except:
        print("The query does not produce any results :\nCorrect format without spaces: f1:value1,f2:value2 \n",
              srt_query)
        print("Observe capital letters in names")
    return Tags


def dictionary_query(str_query):
    """ takes a string query and convert in dictionary
		useful when is needed to transform 
		a string query in a dictionary from the command line
	"""
    query = {}
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


def projection(lst_attributes):
    """
    This function returns a dictionary in format {'at1':True,'at2':True, etc}
    :param lst_attributes: Contains the list of attributes
    :return: dictionary
    """
    assert isinstance(lst_attributes, list)
    dictionary = dict()
    for x in lst_attributes:
        dictionary[x] = True
    return dictionary


def get_tag_values(collection, regex_query, projection_query , del_id=True, series_format = 'DF'):
    # TODO: time_format='%Y-%m-%d %H:%M' in case a specific format of date is needed
    """
    Gets values of a time series which name is 'tag' in a period of time given by regex_time dictionary/string
    :param collection: Mongo DB collection
    :param projection: String name of a single tag, or list, or dictionary
    :param regex_query:  It could be either a dictionary {'timestamp': {'$regex': '02.2013'}}
                              or string '(02|03).2013'
    :param del_id: True if you want delete the '_id' column
    :param series_format: 'xy' format where x = timestamp, y = list of values,  'DF' (default) send a DataFrame
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

    projection_dict['timestamp']= True

    if isinstance(regex_query, str):
        regex_query = dictionary_time(regex_query)

    assert isinstance(regex_query, dict)
    assert isinstance(projection_dict, dict)
    # query time and projection are dictionary types
    #print(regex_query)
    #print(projection_dict)

    cursor = collection.find(regex_query, projection_dict)
    # getting the Dataframe
    df = pd.DataFrame(list(cursor))
    if df.empty:
        print("The query for collection:", collection, "does not produce any value")
        df = pd.DataFrame(columns=list(projection_dict.keys()))
        return df

    # additional process if is needed
    if del_id:
        try:
            if del_id: del df['_id']
        except KeyError:
            print("Verify if del_id = True is needed ")

    if 'DF' == series_format:
        return df

    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct" +
                  ".It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")
	
    if 'DF_t' == series_format:
        df['timestamp'] = df_aux
        df.drop_duplicates(subset='timestamp',keep='last',inplace=True)
        #df = df.sort_values(['timestamp'], ascending=[1])
        df.set_index(keys=['timestamp'],inplace=True)
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        return df
				  
    if 'DF_idx' == series_format:

        # this is for numeric values
        try:
            df['timestamp'] = df_aux
            # df[projection_dict] = pd.to_numeric(df[projection_dict], errors='coerce')
            df = df.sort_values(['timestamp'], ascending=[1])
            df.set_index(keys='timestamp', inplace=True)

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
            print("For xy format the projection_query must be a string and not: ",type(projection_query) )
        return x,y

    print('Any series format was selected')
    return df


def get_tag_values_per_weekday(collection, projection_query, start_time, end_time):

    assert isinstance(start_time,datetime.datetime)
    assert isinstance(end_time,datetime.datetime)
    if start_time>end_time:
        return []

    list_df = list()
    # Loop for all the day: Monday to Sunday
    for weekday in range(1,8):
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
            str_reg_exp_time = str_reg_exp_time + '|'+ str(date.date())
        # get query time
        regex_query = dictionary_time(str_reg_exp_time)
        # get values
        df = get_tag_values(collection,regex_query,projection_query,series_format='DF_t')
        df['hour'] = df.index.hour
        df['week_of_year'] = df.index.weekofyear
        list_df.append(df)

    return list_df

def test():
    print("This is working now =)")
