#!/usr/bin/env python

from datetime import datetime


def to_list(some_object):
    if not isinstance(some_object, list):
        arr = list()
        arr.append(some_object)
        return arr
    return some_object


def parse_iso_date(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    return date