#!/usr/bin/env python

from utils import util
from datetime import datetime
import json
import sys

def get_sorted_cluster(cluster):
    if "hasPost" in cluster:
        posts = util.to_list(cluster["hasPost"])
        posts = sorted(posts, key=lambda k: get_post_date(k))
        cluster["hasPost"] = posts
    return json.dumps(cluster)


def get_post_date(post):
    if "dateCreated" in post:
        date_str = post["dateCreated"]
    else:
        date_str = "1900-01-01T01:00:00"
    try:
        return util.parse_iso_date(date_str)
    except:
         sys.stderr.write("\nError parsing date:" + date_str)
    return datetime.today()
