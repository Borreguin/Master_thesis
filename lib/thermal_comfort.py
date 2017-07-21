# Computes Thermal comfort based on ISA 180, 382/1.
# (computation based on ISO 7730 (Fanger-Theorie))
# parameters: room temperature, outdoor median temperature
# Note!: The original script was proposed by Roman Baeriswyl
# this is a translation to python language and applied to mongoDB
# Author: Roberto Sanchez
# 1. Obtain the tag names for each temperature room
# 2. Compute the sliding mean with a windows of 48 for the outdoor temperature
# 3. Compute the metric [0, 1] where 0 is less comfortable and 1 is comfortable
#       0 -> out-of-comfort value, 1 -> in-comfort value
##
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import math as m
import rs_common_framework_v4 as rs
import pysax

# TODO: change the name of the collection: Source---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'

# TODO: change the name of the collection: Final collection (result)---------
collection_comfort_room = 'comfort_room'

projection = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables
period = '1D'  # '1M'

# TODO: change limits for thermal confort metric ---------
out_comfort = 0
in_comfort = 1

h_min = 6
h_max = 20

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_comfort_room = connection[MONGODB_DB][collection_comfort_room]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_query = {'category': 'Temperature', 'floor': {"$in": [1, 2, 3]}}
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    tag_outdoor_temp = 'V032_outdoor_temp'
    compute_thermal_comfort(tag_list, tag_outdoor_temp)
    return "End of this script"


###############################################################

def compute_thermal_comfort(tag_list, tag_outdoor_temp):
    # get values for outdoor temperature and calculate his rolling mean with a windows of 48
    df_outdoor_temp = rs.get_tag_values(collection_series, time_query, tag_outdoor_temp, series_format='DF_t')
    df_outdoor_temp[tag_outdoor_temp] = pd.to_numeric(df_outdoor_temp[tag_outdoor_temp], errors='coerce')
    df_outdoor_temp = df_outdoor_temp.fillna(df_outdoor_temp.interpolate())
    df_outdoor_temp = df_outdoor_temp.rolling(window=48, min_periods=1).mean()
    # print(df_outdoor_temp.info())

    # get values for temperature for all the rooms (fix nan values)
    df_room_temp = rs.get_tag_values(collection_series, time_query, tag_list, series_format='DF_t')
    for tag in tag_list:
        df_room_temp[tag] = pd.to_numeric(df_room_temp[tag], errors='coerce')
        df_room_temp[tag] = df_room_temp.fillna(df_room_temp[tag].interpolate())

    # including in the general dataframe to work with
    # df_room_temp[tag_outdoor_temp] = df_outdoor_temp[tag_outdoor_temp]
    # df_comfort = pd.DataFrame()
    # function_to_map = lambda row: get_comfort_bin(row.iloc[0], row.iloc[1])

    # get values of comfort using the metric
    # for tag in tag_list:
    #    df_comfort[tag] = df_room_temp[[tag, tag_outdoor_temp]].apply(function_to_map, axis=1)

    for idx in df_room_temp.index:
        if h_min <= idx.hour <= h_max:
            timestamp = str(idx)
            epoch_value = time.mktime(time.strptime(timestamp, "%Y-%m-%d %H:%M:%S"))
            print("-- processing: {0}".format(timestamp))
            to_save = {
                'timestamp': timestamp,
                'epoch': epoch_value
            }
            filter_query = to_save.copy()
            for tag in tag_list:
                comfort = rs.get_comfort_bin(df_room_temp[tag].loc[idx],
                                             df_outdoor_temp[tag_outdoor_temp].loc[idx],
                                             out_comfort=out_comfort,
                                             in_comfort=in_comfort)
                to_save[tag] = comfort

            collection_comfort_room.update(
                spec=filter_query,
                document=to_save,
                upsert=True,
            )

###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
