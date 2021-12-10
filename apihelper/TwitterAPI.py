# %%
# module imports
import requests
from requests.exceptions import RequestException
from requests_oauthlib import OAuth1Session
from urllib.parse import urlencode
import logging
import yaml
import time
from .LogHandler import LogHandler

# Ref: https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Tweet-Lookup/get_tweets_with_bearer_token.py
# Ref: https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Tweet-Lookup/get_tweets_with_user_context.py

# %%
# twitter api class
class TwitterAPI(LogHandler):

    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.propagate = True

    def __init__(self):
        super(TwitterAPI, self).__init__()

        with open("config.yaml", "r") as yamlfile:
            config = yaml.safe_load(yamlfile)
        self.BEARER_TOKEN = config["twitter"]["oauth2.0"]["bearer_token"]
        self.CONSUMER_KEY = config["twitter"]["oauth1.0a"]["consumer_key"]
        self.CONSUMER_SECRET = config["twitter"]["oauth1.0a"]["consumer_secret"]
        self.ACCESS_TOKEN = config["twitter"]["oauth1.0a"]["access_token"]
        self.ACCESS_TOKEN_SECRET = config["twitter"]["oauth1.0a"]["access_token_secret"]
        self.BASE_URL = "https://api.twitter.com/2"

        self.session = requests.Session()

    
    def get_bearer_header(self):
        return {
            "Authorization": f"Bearer {self.BEARER_TOKEN}"
        }

    def request(self, method, route, params=None, data=None, json=None, user_auth=False):
        headers = {}
        auth = None

        # Check for user auth or not
        if user_auth:
            auth = OAuth1Session(
                client_key = self.CONSUMER_KEY,
                client_secret = self.CONSUMER_SECRET,
                resource_owner_key = self.ACCESS_TOKEN,
                resource_owner_secret = self.ACCESS_TOKEN_SECRET
            )
        else:
            headers["Authorization"] = f"Bearer {self.BEARER_TOKEN}"

        # add debug statement before making request

        # https://docs.python-requests.org/en/latest/api/
        with self.session.request(
            method=method, 
            url=self.BASE_URL+route, 
            params=params, 
            data=data, 
            json=json, 
            headers=headers, 
            auth=auth
        ) as response:
            # add debug statement of response

            # update for more specific response codes
            # https://developer.twitter.com/en/support/twitter-api/error-troubleshooting
            # 400 - bad request
            # 401 - unauthorized
            # 403 - forbidden
            # 404 - not found
            # 429 - Too many requests: returned when a request cannot be served due to the app's rate limit having been exhausted
            if response.status_code == 429:
                reset_time = int(response.headers["x-rate-limit-reset"])
                sleep_time = reset_time - int(time.time()) + 1
                if sleep_time > 0:
                    self.log.warning(f"Rate limit exceeded. | Reset time: {reset_time}, Sleep time: {sleep_time}.")
                    time.sleep(sleep_time)
                return self.request(method, route, params, data, json, user_auth)
            if response.status_code not in range(200, 300): # if not 200-299, then unsuccessful
                self.log.error(f"Response Error | Non 200 - {response.status_code=}")
                raise RequestException(response)
            
            return response

    def search_tweets(self, ids: str, fields: str="all", user_auth: bool=False): # default fields "id,text"?
        twitter_fields = {
            "sensitive": [
                "non_public_metrics",
                "organic_metrics",
                "promoted_metrics"
            ],
            "non-sensitive": [
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
        }
        
        if fields == "all": # all non-sensitive
            fields = ",".join(twitter_fields["non-sensitive"])
        
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
        expansions = ",".join(expansions)

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
        media_fields = ",".join(media_fields)

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
        place_fields = ",".join(place_fields)

        poll_fields = [
            "duration_minutes",
            "end_datetime",
            "id",
            "options",
            "voting_status"
        ]
        poll_fields = ",".join(poll_fields)

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
        user_fields = ",".join(user_fields)

        self.log.info(locals())
        endpoint = f"{self.BASE_URL}/tweets"
        headers = self.get_bearer_header()
        additional_headers = {}
        headers.update(additional_headers)
        data = urlencode({
            "ids": ids,
            "tweet.fields": fields,
            "expansions": expansions,
            "media.fields": media_fields,
            "place.fields": place_fields,
            "poll.fields": poll_fields,
            "user.fields": user_fields
        })
        lookup_url = f"{endpoint}?{data}"
        r = requests.request("GET", lookup_url, headers=headers)
        if r.status_code not in range(200, 300): # if not 200-299, then unsuccessful
            self.log.error(f"Response Error | Non 200 - {r.status_code=}")
            # throw exception?
            return {}
        r_json = r.json()
        if "errors" in r_json:
            self.log.debug(f"API Endpoint Error | Total Count: {len(r_json['errors'])}")
            for idx, err in enumerate(r_json["errors"]):
                self.log.warning(f"API Endpoint Error | {r_json['errors'][idx]=}")
        return r_json

# %%
# demo scrape for singluar id
# twitter = TwitterAPI()
# res = twitter.search_tweets("1213330173736738817", fields="all")
# res = twitter.search_tweets("1214195710301618178", fields="all")
# res = twitter.search_tweets("1213330173736738817,1214195710301618178,1223118424814968832", fields="all")
# print()
# print(res)
