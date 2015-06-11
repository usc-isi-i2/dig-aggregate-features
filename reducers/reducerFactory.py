#!/usr/bin/env python

from histogramFeatureReducer import HistogramFeatureReducer
from dateFeatureReducer import DateFeatureReducer


class ReducerFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_reducer(reducerName):
        if reducerName == "histogram":
            return HistogramFeatureReducer()
        elif reducerName == "date":
            return DateFeatureReducer()
        return None
