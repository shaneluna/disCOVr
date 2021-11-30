import tweepy
import yaml
import os
import json
from pyspark.sql import SparkSession
import pandas as pd

def read_yaml_config():
    with open("config.yaml", "r") as yamlfile:
        config = yaml.safe_load(yamlfile)
    return config

def get_tweepy_client():
    config = read_yaml_config()
    bearer_token = config["twitter"]["oauth2.0"]["bearer_token"]
    consumer_key = config["twitter"]["oauth1.0a"]["consumer_key"]
    consumer_secret = config["twitter"]["oauth1.0a"]["consumer_secret"]
    access_token = config["twitter"]["oauth1.0a"]["access_token"]
    access_token_secret = config["twitter"]["oauth1.0a"]["access_token_secret"]

    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True
    )
    return client

def create_directory(path):
    already_exists = os.path.exists(path)
    # if no, create; else, do nothing
    if not already_exists: os.makedirs(path)

def write_pretty_json_to_file(json_string, filepath):
    with open(filepath, 'w', encoding="utf-8") as f:
        json.dump(json_string, f, ensure_ascii=False, indent=4)

def write_json_to_csv(json_string, filepath):
    # file does not exist
    if not os.path.isfile(filepath):
        pd.json_normalize(json_string).to_csv(filepath, index=False, mode='w')
    # file already exists
    else:
        pd.json_normalize(json_string).to_csv(filepath, index=False, mode='a', header=False)
    

if __name__ == '__main__':
    client = get_tweepy_client()

    expansions = ",".join([
        "attachments.poll_ids",
        "attachments.media_keys", 
        "author_id",
        "entities.mentions.username",
        "geo.place_id",
        "in_reply_to_user_id",
        "referenced_tweets.id",
        "referenced_tweets.id.author_id"
    ])

    media_fields = ",".join([
        "duration_ms",
        "height",
        "media_key",
        "preview_image_url",
        "type",
        "url",
        "width",
        "public_metrics",
        "non_public_metrics",
        "organic_metrics",
        "promoted_metrics",
        "alt_text"
    ])
    
    place_fields = ",".join([
        "contained_within",
        "country",
        "country_code",
        "full_name",
        "geo",
        "id",
        "name",
        "place_type"
    ])

    poll_fields = ",".join([
        "duration_minutes",
        "end_datetime",
        "id",
        "options",
        "voting_status"
    ])

    user_fields = ",".join([
        "created_at",
        "description",
        "entities",
        "id",
        "location",
        "name",
        "pinned_tweet_id",
        "profile_image_url",
        "protected",
        "public_metrics",
        "url",
        "username",
        "verified",
        "withheld"
    ])

    tweet_fields = ",".join([
        "attachments",
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "id",
        "in_reply_to_user_id",
        "lang",
        "possibly_sensitive",
        "public_metrics",
        "referenced_tweets",
        "reply_settings",
        "source",
        "text",
        "withheld"
    ])

    # public_tweets = api.get_tweets(user_auth=True, ids=["1213330173736738817","1223120931377123329"])
    # public_tweets = client.get_tweets(
    #     user_auth=True, 
    #     ids=["1213330173736738817"], 
    #     expansions=expansions, 
    #     media_fields=media_fields, 
    #     place_fields=place_fields,
    #     poll_fields=poll_fields,
    #     user_fields=user_fields,
    #     tweet_fields=tweet_fields
    # )
    # print(public_tweets)

    create_directory("./output")

    spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Test") \
    .config('sprk.sql.session.timeZone', 'UTC') \
    .getOrCreate()

    spark.read.option("header", True).option("delimiter", ",").csv("./data/reCOVery/recovery-social-media-data.txt").createOrReplaceTempView("tweets")

    spark.sql("""
    SELECT *, monotonically_increasing_id() AS idx
    FROM tweets
    """).createOrReplaceTempView("tweets2")

    count = 0
    start = 0
    end = start+99
    while start < 140820: #row count
    # while start < 200:
        # get ids
        ids = ",".join(spark.sql(f"""
            SELECT tweet_id, idx
            FROM tweets2
            WHERE idx BETWEEN {start} AND {end}
        """).toPandas()['tweet_id'])
        start = end+1
        end = start+99

        response = client.request(
            method="GET", 
            route="/2/tweets", 
            params={
                # "ids": "1213330173736738817,1223120931377123329",
                "ids": ids,
                "tweet.fields": tweet_fields,
                "expansions": expansions,
                "media.fields": media_fields,
                "place.fields": place_fields,
                "poll.fields": poll_fields,
                "user.fields": user_fields
            }, 
            user_auth=True
        )
        # write_pretty_json_to_file(response.json(), './output/test.json')
        # print(response.json())
        response_json = response.json()
        write_pretty_json_to_file(response_json, f"./output/{str(count).zfill(9)}.json")
        # if "data" in response_json:
        #     write_json_to_csv(response_json["data"], "./output/data.csv")
        # if "includes" in response_json:
        #     write_json_to_csv(response_json["includes"], "./output/includes.csv")
        # if "errors" in response_json:
        #     write_json_to_csv(response_json["errors"], "./output/errors.csv")
        # if "meta" in response_json:
        #     write_json_to_csv(response_json["meta"], "./output/meta.csv")
        count+=1