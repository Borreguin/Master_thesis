# Adds labels to each register in collection_to_change
# Author: Roberto Sanchez
# Personal library.
# 1. Read the complete collection to change
# 2. Add day Name
# 3. Add week reference
# 4. Add day type (working day, holiday, weekend)
# 5. Add epoch if is needed for range time queries
# 16/11/2016
##
from pymongo import MongoClient
import pandas as pd
import rs_common_framework_v4 as rs
import time
import pysax

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collections = ['C_1-C_2-C_4', 'statistics_daily' ]

collection_to_change = collections[0]
dictionary_query = {'timestamp': {'$exists': True}}
projection = {'timestamp': 1, '_id': 1}
# -------------------------------------------------


connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection = connection[MONGODB_DB][collection_to_change]


##############################################################


def main():
    # TODO: Put here the function to run
    add_day_Name()
    add_week_reference()
    add_day_type()
    add_epoch()
    return "End of this script"


###############################################################


def add_day_Name():
    cursor = collection.find(dictionary_query, projection)
    df = pd.DataFrame(list(cursor))
    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M:%S")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct." +
                  "It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")
            return

    df.timestamp = df_aux
    df.set_index(keys=['timestamp'], inplace=True)
    df.index = pd.to_datetime(df.index)  # <-
    weekday_name_list = list(df.index.weekday_name)
    count = 0
    idx_list = list(df[u'_id'])
    for idx, weekday_name in zip(idx_list, weekday_name_list):
        collection.find_and_modify(query={'_id': idx},
                                   update={"$set": {'weekday_name': weekday_name}})
        count += 1

    print(count, " registers were changed!!")


def add_week_reference():
    cursor = collection.find(dictionary_query, projection)
    df = pd.DataFrame(list(cursor))

    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M:%S")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct." +
                  "It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")
            return

    df.timestamp = df_aux
    # df.set_index(keys=['timestamp'], inplace=True)
    df['timestamp'] = pd.to_datetime(df.timestamp)  # <-
    df_aux = pd.Series(pd.to_datetime(df.timestamp))
    start_weekday_list = list(df_aux.apply(rs.get_last_weekday, args=('Monday',)))
    end_weekday_list = list(df_aux.apply(rs.get_next_weekday, args=('Sunday',)))

    count = 0
    idx_list = list(df[u'_id'])
    for idx, sta_day, end_day in zip(idx_list, start_weekday_list, end_weekday_list):
        week_description = rs.month_name(sta_day.month) + " " + str(sta_day.day) + " to " + \
                           rs.month_name(end_day.month) + " " + str(end_day.day) + ", " + str(end_day.year) \
                           + " at " + rs.conv_hour(end_day.hour) + ":00"
        collection.find_and_modify(query={'_id': idx},
                                   update={"$set": {'belongs_to': week_description}})
        count += 1

    print(count, " registers were changed!!")


def add_epoch():
    time_query = {'timestamp': {'$exists': True}}
    projection = {'timestamp': True}
    cursor = collection.find(time_query, projection)
    # cursor is a list of dictionaries
    list_dict = list(cursor)

    count = 0
    for x in list_dict:
        count += 1
        try:
            x_timestamp = str(pd.to_datetime(x['timestamp'], format='%d.%m.%Y %H:%M'))
            epoch_value = time.mktime(time.strptime(x_timestamp, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            try:
                x_timestamp = str(pd.to_datetime(x['timestamp'], format='%Y-%m-%d %H:%M'))
                epoch_value = time.mktime(time.strptime(x_timestamp, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                print('Observe the format of the timestamp in', collection_to_change)

        idx = x['_id']
        collection.find_and_modify(query={'_id': idx},
                                   update={"$set": {'epoch': epoch_value}})

    print(count, " registers were changed!!")


def add_day_type():
    cursor = collection.find(dictionary_query, projection)
    df = pd.DataFrame(list(cursor))

    count = 0
    try:
        df_aux = pd.to_datetime(df.timestamp, format="%d.%m.%Y %H:%M:%S")
    except ValueError:
        try:
            df_aux = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M")
        except ValueError:
            print("\n1. The timestamp in the collection is not in format:" +
                  "%Y-%m-%d %H:%M or %d.%m.%Y %H:%M" + "\n2. The time query is not correct." +
                  "It must correspond to the format in the collection: Ex1: -05-2013  Ex2: 05.2013 \n\n")
            return

    df.timestamp = df_aux
    df.set_index(keys=['timestamp'], inplace=True)
    df.index = pd.to_datetime(df.index)  # <-
    df['day_type'] = 'working_day'

    # processing weekends:
    df['aux'] = list(df.index.dayofweek)
    mask = (df.aux == 5) | (df.aux == 6)
    df.loc[mask, 'day_type'] = 'weekend'

    # processing Swiss holidays:
    holiday_list = get_holidays()

    day_list = [x._date_repr for x in df.index]
    df['aux'] = day_list
    mask = df.aux.isin(holiday_list)
    df['day_type'][mask] = 'holiday'

    idx_list = list(df[u'_id'])
    day_type_list = list(df['day_type'])
    for idx, day_type in zip(idx_list, day_type_list):
        collection.find_and_modify(query={'_id': idx},
                                   update={"$set": {'day_type': day_type}})
        count += 1

    print(count, " registers were changed!!")


def get_holidays():
    # data acquired from: http://www.feiertagskalender.ch/


    h_2012 = ['2012-01-01', '2012-01-02'
              '2012-02-13',
              '2012-03-29',
              '2012-04-06', '2012-04-28',
              '2012-05-01', '2012-05-17',
              '2012-06-17',
              '2012-08-01', '2012-08-15',
              '2012-10-01', '2012-10-02',
              '2012-11-01',
              '2012-12-08', '2012-12-25', '2012-12-26', '2012-12-31']

    h_2013 = ['2013-01-01', '2013-01-02',
              '2013-02-13',
              '2013-03-29',
              '2013-04-01', '2013-04-30',
              '2013-05-01', '2013-05-09', '2013-05-20',
              '2013-06-09',
              '2013-08-01', '2013-08-15',
              '2013-11-01',
              '2013-12-08', '2013-12-25', '2013-12-26', '2013-12-31']

    h_2014 = ['2014-01-01', '2014-01-02',
              '2014-02-18',
              '2014-03-29',
              '2014-04-21', '2014-04-18',
              '2014-05-01', '2014-05-29',
              '2014-06-09',
              '2014-08-01', '2014-08-15',
              '2014-11-01',
              '2014-12-08', '2014-12-25', '2014-12-26', '2014-12-31']

    h_2015 = ['2015-01-01', '2015-01-02',
              '2015-02-18',
              '2015-03-03', '2015-03-06',
              '2015-04-01', '2015-04-03', '2015-04-06', '2015-04-09', '2014-04-30',
              '2015-05-01', '2015-05-14', '2015-05-25'
              '2015-06-09',
              '2015-08-01', '2015-08-15',
              '2015-11-01',
              '2015-12-08', '2015-12-25', '2015-12-26', '2015-12-31']

    exceptions = [
        '2013-12-30', '2012-11-06', '2012-12-27', '2012-12-28', '2013-11-27', '2014-12-24',
        '2012-07-19', '2012-08-21', '2014-07-17', '2013-12-24', '2012-12-24', '2015-05-25'

    ]
    # maintenance was done in '2013-11-08'
    # '2013-11-11'
    return h_2012 + h_2013 + h_2014 + h_2015 + exceptions


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
