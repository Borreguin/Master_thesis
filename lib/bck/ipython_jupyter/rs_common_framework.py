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


def get_tag_values(collection, str_tag, regex_time_query, del_id=True, time_format='%Y-%m-%d %H:%M'):
    """
    Gets values of a time series which name is 'tag' in a period of time given by regex_time dictionary/string
    :param collection: Mongo DB collection
    :param str_tag: String name of a single tag
    :param regex_time_query:  It could be either a dictionary {'timestamp': {'$regex': '02.2013'}}
                              or string '(02|03).2013'
    :param del_id: True if you want delete the '_id' column
    :param time_format: If a particular time format exists: '%Y-%m-%d %H:%M'
    :return: a pandas dataFrame
    The requested data base has the shape:
        timestamp:  12-06-2012
        tag1:       4520.2
        tag2:       12.2
        etc.
    """

    projection = {'timestamp': True, str_tag: True}
    if type(regex_time_query) == 'str':
        regex_time_query = dictionary_time(regex_time_query)
    # query time and projection are dictionary types
    cursor = collection.find(regex_time_query, projection)
    # getting the Dataframe
    df = pd.DataFrame(list(cursor))

    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct" +
                  ".It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")

    try:

        if del_id: del df['_id']
        df['timestamp'] = df_aux
        df[str_tag] = pd.to_numeric(df[str_tag], errors='coerce')
        df = df.sort_values(['timestamp'], ascending=[1])
        df.set_index(keys='timestamp', inplace=True)

    except KeyError:
        print("The query does not produce any results: ")
        print("query", str_tag, "\nfield", regex_time_query)
        print("Observe capital letters in names and the format of the time query \n\n")
        print(collection)

    return df


def test():
    print("This is working now =)")
