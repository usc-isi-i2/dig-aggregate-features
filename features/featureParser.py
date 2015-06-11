#!/usr/bin/env python


class FeatureParser:
    def __init__(self, json):
        self.features = dict()
        self.__parse(json)
        pass

    @staticmethod
    def __to_list(some_object):
        if not isinstance(some_object, list):
            arr = list()
            arr.append(some_object)
            return arr
        return some_object

    def __parse(self, cluster):
        if "hasPost" in cluster:
            posts = FeatureParser.__to_list(cluster["hasPost"])
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
                                    feature_name = feature["featureName"]
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