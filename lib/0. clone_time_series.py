# This script creates a copy of a collection of series.
#   1. Selection of tagnames according to the METADATA and read the registers from: collection_origen
#   2. Transforming only to numeric values
#   3. Recognize the date time format (%Y-%m-%d %H:%M:%S)
#   4. Aggregating the epoch label (this allows to make time query using ranges of time)
#   5. Saving the results over: collection destine

from pymongo import MongoClient
import pandas as pd
import time
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
db_name = 'project_db'
collection_origen = 'original_time_series'
collection_destine = 'filtered_time_series'
collection_metadata = 'metadata'
date_format = '%Y-%m-%d %H:%M:%S'
time_query = {}  # all timestamps

args = rs.__get_parameters()
options = vars(args)
print(options)
if options['or'] != 'collection_origen':
    collection_origen = options['or']

if options['de'] != 'collection_destine':
    collection_destine = options['de']

print("CLONE: \n", collection_origen, "\nin: \n", collection_destine)
connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_origen = connection[db_name][collection_origen]
collection_destine = connection[db_name][collection_destine]
collection_metadata = connection[db_name][collection_metadata]


# -------------------------------------------------

# Clone the original data
def clone_time_series():
    print("Start:")
    cursor = collection_origen.find()
    # cursor is a list of dictionaries
    df = pd.DataFrame(list(cursor))
    tag_list = rs.get_tag_names(collection_metadata, {})
    size = len(df.index)

    df = rs.to_numeric(df, tag_list)
    df = rs.validate_time_index(df)
    df['epoch'] = rs.get_epoch(df.index, date_format)

    i = 0
    for idx in df.index:
        i += 1
        if i % 1000 == 0: print(i, ' / ', size)

        filter_query = {'timestamp': df['timestamp'].loc[idx],
                        'epoch': df['epoch'].loc[idx]}
        x = df.loc[idx].to_dict()
        collection_destine.update(
            spec=filter_query,
            document=x,
            upsert=True,
        )


def main():
    clone_time_series()


if __name__ == "__main__":
    main()
