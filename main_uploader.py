from typing import Dict, List
from py2neo import Graph
from py2neo.bulk import create_nodes
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

# To-Do: update output to import directory name

def read_yaml_config():
    with open("config.yaml", "r") as yamlfile:
        config = yaml.safe_load(yamlfile)
    return config

def get_files(path: str, extension: str) -> List:
    return glob.glob(f"{path}*.{extension}")

def read_json_file(filepath: str) -> Dict:
    with open(filepath) as json_file:
        json_dict = json.load(json_file)
    return json_dict

if __name__ == '__main__':
    config = read_yaml_config()
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
    