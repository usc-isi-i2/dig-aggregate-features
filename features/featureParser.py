#!/usr/bin/env python

from utils import util


class FeatureParser:
    def __init__(self, json, feature_name_filter):
        self.features = dict()
        self.fc = dict()
        self.__parse(json, feature_name_filter)
        pass

    def __parse(self, cluster, feature_name_filter):

        if "hasFeatureCollection" in cluster:
            self.fc = cluster["hasFeatureCollection"]
        else:
            cluster["hasFeatureCollection"] = self.fc

        if "hasPost" in cluster:
            posts = util.to_list(cluster["hasPost"])
            for post in posts:
                if "hasFeatureCollection" in post:
                    feature_collection_arr = util.to_list(post["hasFeatureCollection"])
                    for feature_collection in feature_collection_arr:
                        for key in feature_collection:
                            feature_wrapper = feature_collection[key]
                            feature_arr = util.to_list(feature_wrapper)
                            for feature in feature_arr:
                                # print "Got feature:", feature
                                if "featureName" in feature:
                                    feature_name = feature["featureName"]
                                    if feature_name_filter:
                                        if feature_name_filter != feature_name:
                                            continue

                                    arr = []
                                    if feature_name in self.features:
                                        arr = self.features[feature_name]
                                    else:
                                        self.features[feature_name] = arr
                                    arr.append(feature)

    def get_features(self, feature_name):
        if feature_name in self.features:
            return self.features[feature_name]
        return []

    def get_cluster_feature_collection(self):
        return self.fc