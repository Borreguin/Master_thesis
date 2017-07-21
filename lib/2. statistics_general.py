# Calculates statistics
# Author: Roberto Sanchez
# Personal library.
# 1. Obtain the tag names
# 2. Create a line time
# 3. Compute the statistics
# 16/11/2016
##
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import rs_common_framework_v4 as rs

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_statistics = 'statistics_general'

projection = {'timestamp': True}
time_query = {}  # all the timeline
tag_query = {}  # all variables
low_quantile = np.linspace(0.03, 0.20, 15)
up_quantile = np.linspace(0.80, 0.97, 15)

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_series = connection[MONGODB_DB][collection_series]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_statistics = connection[MONGODB_DB][collection_statistics]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    compute_statistics(tag_list)
    return "End of this script"


###############################################################

def get_limit_by_quantile(df, quantiles, limit=1):
    # limit = 1 implies the best polinomial regression when the percentil is 100%
    # limit = 0 implies the best polinomial regression when the percentil is 0%

    dy = df.quantile(quantiles)
    y = np.array(list(dy.values))
    x = np.array(quantiles)

    coeff = np.polyfit(x, y, 2)
    f = np.poly1d(coeff)

    return f(limit)


"""
y_hat = f(x)
y_resid = y_hat - y
ssresid = sum(y_resid ** 2)
sstotal = len(y) * y.var()
rsq = 1 - ssresid / sstotal
y_mean = y.mean()

ssreg = np.sum((y_hat - y_mean) ** 2)
sstot = np.sum((y - y_mean) ** 2)
r = ssreg / sstot
"""


def compute_statistics(tag_list):
    for tag in tag_list:
        print('-- processing: ', tag)
        df = rs.get_tag_values(collection_series, time_query, tag, series_format='DF_t')
        df[tag] = pd.to_numeric(df[tag], errors='coerce')
        query_filter = {'tagname': tag}
        aux = collection_statistics.find_one(query_filter)
        if aux is None:
            register = query_filter
        else:
            register = aux

        statistics = df[tag].describe()
        statistics = statistics.T.to_dict()
        lol = get_limit_by_quantile(df[tag], low_quantile, 0)
        upl = get_limit_by_quantile(df[tag], up_quantile, 1)
        statistics['lol'] = lol
        statistics['upl'] = upl
        register.update(statistics)
        collection_statistics.find_one_and_replace(
            filter=query_filter,
            replacement=register,
            upsert=True
        )


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of th script')
