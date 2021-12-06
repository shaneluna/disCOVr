from typing import Dict, List
from py2neo import Graph
import py2neo
from py2neo.bulk import create_nodes
import utilities as util
import tarfile
import shutil
import os
import time

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

def compress_folder(path: str) -> str:
    """
    Compress folder using gzip into current directory.
    Return the name of new gzipped object.
    """
    folder_name = os.path.basename(path)
    new_file = f"{folder_name}.tar.gz"
    tar = tarfile.open(new_file, "w:gz")
    tar.add(path, arcname=folder_name)
    tar.close()
    return new_file

def uncompress_folder(path: str) -> None:
    """
    Uncompress gzipped folder.
    """
    tar = tarfile.open(path)
    dirname = os.path.dirname(path)
    tar.extractall(dirname)
    tar.close()

def move_files_to_import(start_path: str, end_path: str) -> None:
    """
    Move files from folder start_path to be placed into end_path.
    """
    # Compress
    gzip_file = compress_folder(start_path)
    # Move and overwrite with simiar name
    moved_filepath = f"{end_path}/{gzip_file}"
    print(gzip_file)
    print(moved_filepath)
    shutil.move(gzip_file, moved_filepath)
    # Uncompress
    uncompress_folder(moved_filepath)
    # Delete tar file
    delete_file(moved_filepath)

def delete_file(path: str) -> None:
    """
    Delete a file.
    """
    os.remove(path)

##########
# Neo4j
##########

def create_constraints(graph: Graph) -> None:
    """
    Create constraints and primary keys for graph database.
    """
    try:
        graph.run("CREATE CONSTRAINT ON (tweet:Tweet) ASSERT tweet.tweet_id IS UNIQUE")
    except py2neo.errors.ClientError:
        pass
    try: 
        graph.run("CREATE CONSTRAINT ON (tweet:Tweet) ASSERT exists(tweet.tweet_id)")
    except py2neo.errors.ClientError:
        pass
    try: 
        graph.run("CREATE CONSTRAINT ON (user:User) ASSERT user.user_id IS UNIQUE")
    except py2neo.errors.ClientError:
        pass
    try: 
        graph.run("CREATE CONSTRAINT ON (user:User) ASSERT exists(user.user_id)")
    except py2neo.errors.ClientError:
        pass

##########
# NODES
##########

def import_tweets(graph: Graph) -> None:
    """
    Import tweet nodes to the graph database.
    """
    files = util.get_files("./raw/tweets/", "json")
    for file in files:
        query = f"""
        WITH 'file:///{file}' AS url 
        CALL apoc.load.json(url) YIELD value 
        UNWIND value.data AS data
        WITH data, value.includes as includes
        UNWIND includes.tweets AS include_tweets
        MERGE(t1: Tweet {{tweet_id: data.id}})
        SET t1.text = data.text
        SET t1.lang = data.lang
        SET t1.created_at = data.created_at
        SET t1.source = data.source
        WITH data, t1, include_tweets
        MERGE(t2: Tweet {{tweet_id: include_tweets.id}})
        SET t2.text = include_tweets.text
        SET t2.lang = include_tweets.lang
        SET t2.created_at = include_tweets.created_at
        SET t2.source = include_tweets.source
        """
        graph.run(query)


def import_users(graph: Graph) -> None: 
    """
    Import user nodes to the graph database.
    """
    files = util.get_files("./raw/tweets/", "json")
    for file in files:
        query = f"""
        WITH 'file:///{file}' AS url 
        CALL apoc.load.json(url) YIELD value 
        UNWIND value.data AS data
        WITH data, value.includes as includes
        UNWIND includes.users as include_users
        MERGE (u1: User {{user_id: include_users.id}})
        SET u1.username = include_users.username
        SET u1.name = include_users.name
        SET u1.description = include_users.description
        SET u1.location = include_users.location
        SET u1.created_at = include_users.created_at
        SET u1.verified = include_users.verified
        SET u1.public_metrics_followers_count = include_users.public_metrics.followers_count
        SET u1.public_metrics_following_count = include_users.public_metrics.following_count
        SET u1.public_metrics_tweet_count = include_users.public_metrics.tweet_count
        SET u1.public_metrics_listed_count = include_users.public_metrics.listed_count
        """
        graph.run(query)

def import_hashtags():
    pass

def import_twitter_topics():
    pass

##########
# EDGES
##########

def import_posts():
    pass

def import_referenced(graph: Graph) -> None:
    """
    Import refernced type edges between tweets to the graph database.
    """
    # retweets, mentions, quotes, replies
    files = util.get_files("./raw/tweets/", "json")
    for file in files:
        query = f"""
        WITH 'file:///{file}' AS url 
        CALL apoc.load.json(url) YIELD value 
        UNWIND value.data AS data
        MERGE(t1: Tweet {{tweet_id: data.id}})
        WITH data, t1
        UNWIND data.referenced_tweets AS ref_tweets
        MERGE(t2: Tweet {{tweet_id: ref_tweets.id}})
        WITH t1,t2,ref_tweets
        CALL apoc.create.relationship(t1, ref_tweets.type,null, t2) YIELD rel
        RETURN t1, t2, rel
        """
        graph.run(query)
        

def import_tags():
    pass

def import_covers():
    pass

if __name__ == '__main__':
    config = util.read_yaml_config("config.yaml")
    uri = config["neo4j"]["uri"]
    user = config["neo4j"]["user"]
    password = config["neo4j"]["password"]

    # neo4j_import_location = "/Users/lunez/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-4374ec2b-7704-4595-a764-596a0e2fe227/import"
    # move_files_to_import("./raw", neo4j_import_location)

    # tweet_files = get_files("./output/000000000", "json")
    # print(tweet_files[0])
    # json_dict = read_json_file(tweet_files[0])

    graph = Graph(uri=uri, auth=(user, password))
    # create_constraints(graph)
    start = time.time()
    # import_tweets(graph)
    import_referenced(graph)
    print(time.time()-start)
    # import_users(graph)

    # movies = graph.run("MATCH (m:Movie) RETURN m")
    # data = json_dict['data']
    # print(len(data))
    # create_nodes(graph.auto(), data, labels={"Tweet"})
    