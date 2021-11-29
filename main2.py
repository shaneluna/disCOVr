import tweepy
from tweepy import poll
import yaml

def read_yaml_config():
    with open("config.yaml", "r") as yamlfile:
        config = yaml.safe_load(yamlfile)
    return config

if __name__ == '__main__':
    config = read_yaml_config()
    bearer_token = config["twitter"]["oauth2.0"]["bearer_token"]
    consumer_key = config["twitter"]["oauth1.0a"]["consumer_key"]
    consumer_secret = config["twitter"]["oauth1.0a"]["consumer_secret"]
    access_token = config["twitter"]["oauth1.0a"]["access_token"]
    access_token_secret = config["twitter"]["oauth1.0a"]["access_token_secret"]

    api = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True
    )

    expansions = [
        "attachments.poll_ids",
        "attachments.media_keys", 
        "author_id",
        "entities.mentions.username",
        "geo.place_id",
        "in_reply_to_user_id",
        "referenced_tweets.id",
        "referenced_tweets.id.author_id"
    ]

    media_fields = [
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
    ]
    
    place_fields = [
        "contained_within",
        "country",
        "country_code",
        "full_name",
        "geo",
        "id",
        "name",
        "place_type"
    ]

    poll_fields = [
        "duration_minutes",
        "end_datetime",
        "id",
        "options",
        "voting_status"
    ]

    user_fields = [
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
    ]

    tweet_fields = [
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
    ]

    # public_tweets = api.get_tweets(user_auth=True, ids=["1213330173736738817","1223120931377123329"])
    public_tweets = api.get_tweets(
        user_auth=True, 
        ids=["1213330173736738817"], 
        expansions=expansions, 
        media_fields=media_fields, 
        place_fields=place_fields,
        poll_fields=poll_fields,
        user_fields=user_fields,
        tweet_fields=tweet_fields
    )
    print(public_tweets)