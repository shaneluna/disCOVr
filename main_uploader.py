from typing import Dict, List
from py2neo import Graph
from py2neo.bulk import create_nodes
import utilities as util
import yaml
import glob
import json

# Follow for apoc: https://neo4j.com/developer/neo4j-apoc/
# Select db from neo4j desktop > plugins on right > apoc > install and restart
# In settings, add the following to the bottom: apoc.import.file.enabled=true

# https://neo4j.com/developer/guide-import-csv/
# Modifying acceptable import location 
# https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction
# default import folder locations
# Linux / macOS / Docker - <neo4j-home>/import
# Windows - <neo4j-home>/import
# Debian / RPM - /var/lib/neo4j/import

def get_files(path: str, extension: str) -> list:
    return glob.glob(f"{path}*.{extension}")

def read_json_file(filepath: str) -> dict:
    with open(filepath) as json_file:
        json_dict = json.load(json_file)
    return json_dict

def compress_folder(path: str) -> None:
    pass

def uncompress_folder(path: str) -> None:
    pass

def move_files_to_import(start_path: str, end_path: str) -> None:
    # Compress
    # Move and overwrite with simiar name
    # Uncompress
    pass

def delete_files(path: str) -> None:
    # Should be able to handle deleting single file, or whole folder with many files
    pass

def base_import():
    pass

##########
# NODES
##########

def import_tweets():
    pass

def import_users():
    pass

def import_hashtags():
    pass

def import_twitter_topics():
    pass

##########
# EDGES
##########

def import_posts():
    pass

def import_retweets():
    pass

def import_mentions():
    pass

def import_quotes():
    pass

def import_replies():
    pass

def import_tags():
    pass

def import_covers():
    pass

if __name__ == '__main__':
    config = util.read_yaml_config("config.yaml")
    uri = config["neo4j"]["uri"]
    user = config["neo4j"]["user"]
    password = config["neo4j"]["password"]

    tweet_files = get_files("./output/000000000", "json")
    print(tweet_files[0])
    json_dict = read_json_file(tweet_files[0])

    graph = Graph(uri=uri, auth=(user, password))
    # movies = graph.run("MATCH (m:Movie) RETURN m")
    data = json_dict['data']
    print(len(data))
    # create_nodes(graph.auto(), data, labels={"Tweet"})


    query = """
    WITH 'file:///000000000.json' as url 
    CALL apoc.load.json(url) YIELD value 
    unwind value.data as data
    with data, value.includes as includes
    unwind includes.tweets as include_tweets
    merge(t1: Tweet {tweet_id: data.id})
    set t1.text = data.text
    with data, t1, include_tweets
    merge(t2: Tweet {tweet_id: include_tweets.id})
    set t2.text = include_tweets.text
    return t1, t2
    """
    graph.run(query)
    print(graph.nodes.match("Tweet").count())
    