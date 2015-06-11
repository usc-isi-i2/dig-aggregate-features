#!/usr/bin/env python

from reducers.featureReducerInterface import FeatureReducerInterface


class HistogramFeatureReducer(FeatureReducerInterface):

    features = []
    def __init__(self):
        del self.features[:]
        pass

    def add_value(self, feature):
        self.features.append(feature)

    def get_features(self):
        agg_features = []

        value_map = {}
        for feature in self.features:
            value = feature["featureValue"]
            count = 0
            if value_map.has_key(value):
                count = value_map[value]
            count += 1
            value_map[value] = count

        for value in value_map:
            count = value_map[value]
            feature = {"featureValue": value, "count": count}
            agg_features.append(feature)

        return agg_features

    def get_aggregate_feature(self, feature_name):
        features = self.get_features()
        agg_feature = dict()
        agg_feature["featureName"] = feature_name + "_histogram"
        agg_feature["a"] = "Feature"
        agg_feature["featureObject"] = features
        return agg_feature

