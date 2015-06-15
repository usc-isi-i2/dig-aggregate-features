#!/usr/bin/env python

import sys
import json
from reducers.reducerFactory import ReducerFactory
from features.featureParser import FeatureParser
from utils import threadUtil

from pyspark import SparkContext


def aggregate_feature(cluster, reducer_feature_name):
    (reducer_name, feature_name) = reducer_feature_name.split(":", 2)
    parser = FeatureParser(cluster, feature_name)
    fc = parser.get_cluster_feature_collection()
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


def rdd_aggregate(rdd, reducer_feature_name):
    return rdd.mapValues(lambda cluster: aggregate_feature(cluster, reducer_feature_name))


def aggregate_features(cluster, reducer_feature_names):
    parser = FeatureParser(cluster, None)
    fc = parser.get_cluster_feature_collection()

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
    # result = json_values
    # for aggregation in aggregations:
    #     result = rdd_aggregate(result, aggregation)

    if len(aggregations) > 0:
        result = json_values.mapValues(lambda cluster: aggregate_features(cluster, aggregations))

    result.mapValues(lambda cluster: threadUtil.get_sorted_cluster(cluster)).saveAsSequenceFile(outputFilename)



