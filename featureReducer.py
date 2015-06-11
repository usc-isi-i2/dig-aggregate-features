#!/usr/bin/env python

import sys
import json
from reducers.reducerFactory import ReducerFactory
from features.featureParser import FeatureParser

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
        print "aggregate feature:", feature_name, ", reducer:", reducer_name + ", num:", len(features)
        if len(features) > 0:
            reducer.add_values(features)
            agg_feature = reducer.get_aggregate_feature(feature_name)
            if agg_feature:
                agg_feature_name = agg_feature["featureName"]
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
    if len(aggregations) > 0:
        result = json_values.mapValues(lambda cluster: aggregate_features(cluster, aggregations))
    result.mapValues(lambda cluster: json.dumps(cluster)).saveAsSequenceFile(outputFilename)



