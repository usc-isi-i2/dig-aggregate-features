#!/usr/bin/env python

import sys
import json
from reducers.histogramFeatureReducer import HistogramFeatureReducer

from pyspark import SparkContext


def reduce(features, reducer):
    for feature in features:
        reducer.add_value(feature)
    return reducer.get_features()

def to_list(some_object):
    if not isinstance(some_object, list):
        arr = []
        arr.append(some_object)
        return arr
    return some_object

def aggregate_feature(cluster, feature_name):
    features = []
    #print "Cluster:", cluster

    if "hasPost" in cluster:
        posts = to_list(cluster["hasPost"])
        for post in posts:
            if "hasFeatureCollection" in post:
                feature_collection_arr = to_list(post["hasFeatureCollection"])
                for feature_collection in feature_collection_arr:
                    for key in feature_collection:
                        feature_wrapper = feature_collection[key]
                        feature_arr = to_list(feature_wrapper)
                        for feature in feature_arr:
                           # print "Got feature:", feature
                            if "featureName" in feature:
                                if feature["featureName"] == feature_name:
                            #        print "-----------------------> add feature:", feature
                                    features.append(feature)

    if len(features) > 0:
        reduced_features = reduce(features, HistogramFeatureReducer())

        fc = {}
        if "hasFeatureCollection" in cluster:
            fc = cluster["hasFeatureCollection"]

        histogram_feature = {}
        fc[feature_name + "_histogram_feature"] = histogram_feature
        histogram_feature["featureName"] = feature_name + "_histogram"
        histogram_feature["a"] = "Feature"
        histogram_feature["featureObject"] = reduced_features

        cluster["hasFeatureCollection"] = fc

    return cluster


if __name__ == "__main__":
    """
        Usage: histogram-features [input] [output]
    """
    sc = SparkContext(appName="DigFeatureReducer")

    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]
    featureNames = sys.argv[3].split(",")

    data = sc.sequenceFile(inputFilename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    json_values = data.mapValues(lambda row: json.loads(row))

    for feature_name in featureNames:
        print "aggregate feature:", feature_name
        reduced = json_values.mapValues(lambda cluster: aggregate_feature(cluster, feature_name))
        json_values = reduced

    json_values.saveAsSequenceFile(outputFilename)



