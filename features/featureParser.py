#!/usr/bin/env python

class FeatureParser:
    def __init__(self):
        pass

    @staticmethod
    def __to_list(some_object):
        if not isinstance(some_object, list):
            arr = []
            arr.append(some_object)
            return arr
        return some_object

    @staticmethod
    def parse(json, feature_name_filters):
        features = []

        if "hasPost" in json:
            posts = FeatureParser.__to_list(json["hasPost"])
            for post in posts:
                if "hasFeatureCollection" in post:
                    feature_collection_arr = FeatureParser.__to_list(post["hasFeatureCollection"])
                    for feature_collection in feature_collection_arr:
                        for key in feature_collection:
                            feature_wrapper = feature_collection[key]
                            feature_arr = FeatureParser.__to_list(feature_wrapper)
                            for feature in feature_arr:
                               # print "Got feature:", feature
                                if "featureName" in feature:
                                    if feature["featureName"] in feature_name_filters:
                                #        print "-----------------------> add feature:", feature
                                        features.append(feature)
        return features