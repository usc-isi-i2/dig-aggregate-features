#!/usr/bin/env python

from reducers.featureReducerInterface import FeatureReducerInterface


class HistogramFeatureReducer(FeatureReducerInterface):

    features = []

    def __init__(self):
        del self.features[:]
        pass

    def add_value(self, feature):
        self.features.append(feature)

    def get_aggregate_feature(self, feature_name):
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
            feature = {"featureValue": value,
                       "count": count,
                       "featureName": feature_name + "_histogram",
                       "a": "Feature"}
            agg_features.append(feature)

        return sorted(agg_features, key=lambda k: k['count'], reverse=True)

