#!/usr/bin/env python

import json

class Feature:
    def __init__(self):
        self.featureName = None
        self.featureValue = None
        self.uri = None
        pass


    def get_name(self):
        return self.featureName

    def get_value(self):
        return self.featureValue

    def get_uri(self):
        return self.uri

    def generate_json(self):
        return json.dumps(self)

    @staticmethod
    def parse(self, json):
        feature = Feature()
        feature.featureName = json["featureName"]
        feature.featureValue = json["featureValue"]
        if "uri" in json:
            feature.uri = json["uri"]
        pass
