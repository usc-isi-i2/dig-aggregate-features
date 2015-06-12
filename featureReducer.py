#!/usr/bin/env python

import sys
import json
from reducers.reducerFactory import ReducerFactory
from features.featureParser import FeatureParser
from utils import util
from datetime import datetime

from pyspark import SparkContext


def get_feature_collection(cluster):
    fc = dict()
    if "hasFeatureCollection" in cluster:
        fc = cluster["hasFeatureCollection"]
    else:
        cluster["hasFeatureCollection"] = fc
    return fc


def aggregate_features(cluster, reducer_feature_names):
    parser = FeatureParser(cluster)
    fc = get_feature_collection(cluster)

    for reducer_feature_name in reducer_feature_names:
        (reducer_name, feature_name) = reducer_feature_name.split(":", 2)
        reducer = ReducerFactory.get_reducer(reducer_name)
        features = parser.get_features(feature_name)
        #print "aggregate feature:", feature_name, ", reducer:", reducer_name + ", num:", len(features)
        if len(features) > 0:
            reducer.add_values(features)
            agg_feature = reducer.get_aggregate_feature(feature_name)
            if agg_feature:
                if not isinstance(agg_feature, list):
                    agg_feature_name = agg_feature["featureName"]
                else:
                    agg_feature_name = agg_feature[0]["featureName"]
                fc[agg_feature_name + "_feature"] = agg_feature
    return cluster


def get_sorted_cluster(cluster):
    if "hasPost" in cluster:
        posts = util.to_list(cluster["hasPost"])
        posts = sorted(posts, key=lambda k: get_post_date(k))
        cluster["hasPost"] = posts
    return json.dumps(cluster)


def get_post_date(post):
    if "dateCreated" in post:
        date_str = post["dateCreated"]
    else:
        date_str = "1900-01-01T01:00:00"
    try:
        return util.parse_iso_date(date_str)
    except:
         sys.stderr.write("\nError parsing date:" + date_str)
    return datetime.today()


if __name__ == "__main__":
    """
        Usage: featureReducer.py [input] [output] [reducer:feature_name]...
    """
    sc = SparkContext(appName="DigFeatureReducer")

    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]

    data = sc.sequenceFile(inputFilename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    json_values = data.mapValues(lambda row: json.loads(row))

    aggregations = sys.argv[3:]
    if len(aggregations) > 0:
        result = json_values.mapValues(lambda cluster: aggregate_features(cluster, aggregations))

    result.mapValues(lambda cluster: get_sorted_cluster(cluster)).saveAsSequenceFile(outputFilename)



