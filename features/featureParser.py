#!/usr/bin/env python

from utils import util


class FeatureParser:
    def __init__(self, json, child_name, feature_name_filter):
        self.features = dict()
        self.fc = dict()
        self.__parse(json, child_name, feature_name_filter)
        pass

    def __parse(self, cluster, child_name, feature_name_filter):
        self.fc = cluster

        if child_name in cluster:
            children = util.to_list(cluster[child_name])
            for child in children:
                for key in child:
                    feature_wrapper = child[key]
                    feature_arr = util.to_list(feature_wrapper)
                    for feature in feature_arr:
                        # print "Got feature:", feature
                        arr = []
                        if key in self.features:
                            arr = self.features[key]
                        else:
                            self.features[key] = arr
                        arr.append(feature)

    def get_features(self, feature_name):
        feature_name_parts = feature_name.split(".")
        feature_name_start = feature_name_parts[0]
        #print "Find:", feature_name_start
        if feature_name_start in self.features:
            #print "Found"
            full_features = self.features[feature_name_start]
            if len(feature_name_parts) > 1:
                return self.__extract_value(full_features, feature_name_parts[1:])
            if type(full_features) == list:
                return full_features
            else:
                result = []
                result.append(full_features)
                return result
        return []

    def get_cluster_feature_collection(self):
        return self.fc

    def __extract_value(self, features, name_parts):
        result_arr = []
        for feature in features:
            result = feature
            for name_part in name_parts:
                if name_part in feature:
                    result = result[name_part]
                else:
                    result = None
                    break
            if result is not None:
                if type(result) == list:
                    result_arr.extend(result)
                else:
                    result_arr.append(result)
        return result_arr