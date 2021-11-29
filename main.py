from pyspark.sql import SparkSession
import pandas
import json
from apihelper import TwitterAPI
import os

def create_directory(path):
    already_exists = os.path.exists(path)
    # if no, create; else, do nothing
    if not already_exists: os.makedirs(path)

def write_pretty_json_to_file(json_string, full_filepath):
    with open('./output/test.json', 'w', encoding="utf-8") as f:
        json.dump(json_string, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':

    # initial setup: create some sort of tracking file of ids -- done in notebook (can add later if can just run in 1 sitting/reasonable amount of time)
    # determine max number of inputs for endpoint
        # tweets - 900 requests / 15 min, 100 ids per request (no tweet cap)
        # replies
        # retweets
        # likes
        # follows
    # hit endpoint with throttler
    # write json output to file

    twitter = TwitterAPI()
    # res = twitter.search_tweets("1213330173736738817", fields="all")
    res = twitter.search_tweets("1213330173736738817,1223120931377123329", fields="all")
    # print(res)
    create_directory("./output")
    write_pretty_json_to_file(res, './output/test.json')