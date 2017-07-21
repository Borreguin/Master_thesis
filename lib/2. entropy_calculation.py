# Calculates statistics
# Author: Roberto Sanchez
# Personal library.
# 1. Obtain the tag names
# 2. List features
# 3. Compute the emtrophy by Kullback Leibler divergence
# 16/11/2016
##
from pymongo import MongoClient
import numpy as np
import numbers
import rs_common_framework_v4 as rs
import scipy.stats
import math as m

# TODO: change the name of the collection:---------
MONGODB_HOST = 'localhost'  # '192.168.6.132'
MONGODB_PORT = 27017
MONGODB_DB = 'project_db'
# collection_series = 'filtered_time_series'
collection_metadata = 'metadata'
collection_statistics = 'statistics_daily'
collection_details = 'feature_selection_details'
collection_final = 'feature_selection'

projection = {'_id': False, 'epoch': False, 'count': False}
time_query = {}  # all the timeline
tag_query = {}  # all variables
group_name = 'weekday_name'

connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collection_details = connection[MONGODB_DB][collection_details]
collection_final = connection[MONGODB_DB][collection_final]
collection_metadata = connection[MONGODB_DB][collection_metadata]
collection_statistics = connection[MONGODB_DB][collection_statistics]


##############################################################


def main():
    # TODO: Put here the function to run
    tag_list = rs.get_tag_names(collection_metadata, tag_query)
    feature_list = get_feature_list()
    group = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    select_features(tag_list, feature_list, group)
    return "End of this script"


###############################################################

def get_feature_list():
    register = collection_statistics.find_one(projection=projection)
    keys = {k: v for k, v in register.items() if isinstance(v, numbers.Number)}
    return keys.keys()


def select_features(tag_list, feature_list, group):
    tag_list = ['tre200b0', 'sre000b0', 'V032_outdoor_temp']
    for tag in tag_list:
        print("--processing: " + tag)
        query_filter = {'tagname': tag}

        select_J_Value = -np.inf
        for f in feature_list:
            max_value_J = -np.inf
            max_list = []
            print("feature: " + f)
            projection = {f: True}
            df = rs.get_tag_values(collection_statistics, query_filter, projection)
            v_max = df[f].max()
            v_min = df[f].min()
            n = df[f].count()

            v_bin = float(v_max - v_min) / n
            xs = np.linspace(v_min - v_bin, v_max + v_bin, n)
            ds = xs[1] - xs[0]

            for g1 in range(len(group)):
                query_filter[group_name] = group[g1]
                df = rs.get_tag_values(collection_statistics, query_filter, projection)
                pk = np.array(df[f].dropna())
                try:
                    kde_pk = scipy.stats.gaussian_kde(pk, bw_method='silverman')
                except:
                    kde_pk = None

                for g2 in range(g1, len(group)):
                    query_filter[group_name] = group[g2]
                    df = rs.get_tag_values(collection_statistics, query_filter, projection)
                    qk = np.array(df[f].dropna())

                    try:
                        kde_qk = scipy.stats.gaussian_kde(qk, bw_method='silverman')
                    except:
                        kde_qk = None

                    if kde_qk is None or kde_pk is None:
                        J = 0
                    else:
                        KLD_pq = scipy.stats.entropy(kde_pk(xs),kde_qk(xs))
                        KLD_qp = scipy.stats.entropy(kde_qk(xs), kde_pk(xs))

                        #KLD_pq = entropy_value(kde_pk(xs), kde_qk(xs), ds)
                        #KLD_qp = entropy_value(kde_qk(xs), kde_pk(xs), ds)

                        J = KLD_pq + KLD_qp

                    # save partial results:
                    filter = {'tagname': tag, 'feature': f}
                    sb = {'group1': group[g1], 'group2': group[g2]}
                    filter.update(sb)
                    # TO SAVE THE DETAILS IF IS NEEDED:#################################3
                    # aux = collection_details.find_one(filter=filter)
                    # if aux is None:
                    #    register = filter
                    # else:
                    #    register = aux
                    # register['J_value'] = J
                    # collection_details.find_one_and_replace(filter=filter, replacement=register, upsert=True)
                    ######################################################################

                    # getting the max divergence measure
                    if J > max_value_J and J != np.nan:
                        sb['J_value'] = J
                        max_list.append(sb)
                        max_value_J = J

            # save the maximum divergence
            filter = {'tagname': tag}
            aux = collection_final.find_one(filter)
            if aux is None:
                register = filter.copy()
                register['summary'] = {}
            else:
                register = aux

            register[f] = max_list
            ranked_list = rs.select_features(collection_final, tag, len(feature_list))
            register['ranked_list'] = ranked_list

            if len(max_list) > 0:
                register['summary'][f] = max_list[-1]
            if max_value_J > select_J_Value:
                aux = max_list[-1].copy()
                aux.update({'feature': f})
                register['selected_feature'] = aux
                select_J_Value = max_value_J

            collection_final.find_one_and_replace(filter, register, upsert=True)






def same_lenght(pk, qk):
    if len(pk) > len(qk):
        pk = pk[:len(qk)]
        return pk, qk
    if len(pk) < len(qk):
        qk = qk[:len(pk)]
        return pk, qk
    else:
        return pk, qk


def entropy_value(pdf_p, pdf_q, ds):
    x = pdf_p * ds
    y = pdf_q * ds
    entropy = 0
    for px, qy in zip(x, y):
        dv = (1 + px) / (1 + qy)
        entropy += px * m.log(dv, 2)

    # print(x.sum())
    # print(y.sum())
    return entropy


###################################
# TO RUN IN THIS APPLICATION
if __name__ == "__main__":
    main()
    print('end of the script')
