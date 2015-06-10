#!/usr/bin/env python

import sys
import json
from reducers.histogramFeatureReducer import HistogramFeatureReducer
from features.featureParser import FeatureParser

from pyspark import SparkContext


def reduce_features(features, reducer):
    for feature in features:
        reducer.add_value(feature)
    return reducer.get_features()


def add_aggregate_feature(cluster, agg_feature_name, values):
    fc = dict()
    #print "add aggregate features: ", agg_feature_name, ("hasFeatureCollection" in cluster)
    if "hasFeatureCollection" in cluster:
        fc = cluster["hasFeatureCollection"]
    else:
        cluster["hasFeatureCollection"] = fc

    agg_feature = dict()
    fc[agg_feature_name + "_feature"] = agg_feature
    agg_feature["featureName"] = agg_feature_name
    agg_feature["a"] = "Feature"
    agg_feature["featureObject"] = values


def aggregate_feature(cluster, feature_name, reducer, agg_feature_name):
    features = FeatureParser.parse(cluster, [feature_name])
    #print "Cluster:", cluster
    #print "Got back " + str(len(features)) + " features for " + feature_name

    if len(features) > 0:
        agg_features = reduce_features(features, reducer)
        add_aggregate_feature(cluster, agg_feature_name, agg_features)

    return cluster


def histogram_rdd_aggregate(rdd, feature_name):
    reduced = rdd.mapValues(
        lambda cluster: aggregate_feature(cluster, feature_name, HistogramFeatureReducer(),
                                          feature_name + "_histogram"))
    return reduced


if __name__ == "__main__":
    """
        Usage: featureReducer.py [input] [output] [comma separated feature names]
    """
    sc = SparkContext(appName="DigFeatureReducer")

    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]
    featureNames = sys.argv[3].split(",")

    data = sc.sequenceFile(inputFilename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    json_values = data.mapValues(lambda row: json.loads(row))

    result = json_values
    for featureName in featureNames:
        print "aggregate feature:", featureName
        result = histogram_rdd_aggregate(result, featureName)

    result.mapValues(lambda cluster: json.dumps(cluster)).saveAsSequenceFile(outputFilename)



