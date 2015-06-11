#!/usr/bin/env python


class FeatureReducerInterface:

    def __init__(self):
        pass

    def add_values(self, features):
        for feature in features:
            self.add_value(feature)

    def add_value(self, feature):
        pass

    def get_aggregate_feature(self, feature_name):
        pass