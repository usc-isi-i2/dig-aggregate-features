#!/usr/bin/env python

from reducers.featureReducerInterface import FeatureReducerInterface
from utils import util
import sys


class DateFeatureReducer(FeatureReducerInterface):

    features = []
    min_date = None
    max_date = None

    def __init__(self):
        del self.features[:]
        pass

    def add_value(self, feature):
        feature_date = feature["featureValue"]
        try:
            date = util.parse_iso_date(feature_date)
            if self.min_date is None:
                self.min_date = date
            elif self.min_date > date:
                self.min_date = date

            if self.max_date is None:
                self.max_date = date
            elif self.max_date < date:
                self.max_date = date

            self.features.append(feature)
        except:
            # exc_type, exc_value, exc_traceback = sys.exc_info()
            # lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            # sys.stderr.write("\nError in add_values:" + str(lines))
            sys.stderr.write("\nError parsing date:" + feature_date)
            pass

    def get_aggregate_feature(self, feature_name):
        if self.min_date is None:
            return None

        feature_value = self.min_date.isoformat()
        if self.min_date != self.max_date:
            feature_value = feature_value + " TO " + self.max_date.isoformat()
        agg_feature = dict()
        agg_feature["featureName"] = feature_name + "_aggregated"
        agg_feature["a"] = "Feature"
        agg_feature["featureValue"] = feature_value
        return agg_feature