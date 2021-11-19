# %%
# module imports
import requests
from urllib.parse import urlencode
import logging
import time
import yaml

# Ref: https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Tweet-Lookup/get_tweets_with_bearer_token.py

FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(funcName)s() | %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logging.Formatter.converter = time.gmtime

# %%
# log handler function
class LoggingHandler():
    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)

# %%
# api function
class TwitterAPI(LoggingHandler):

    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.propagate = True

    def __init__(self):
        super(TwitterAPI, self).__init__()
        with open("config.yaml", "r") as yamlfile:
            config = yaml.safe_load(yamlfile)
        self.BEARER_TOKEN = config["twitter"]["bearer_token"]
        self.BASE_URL = "https://api.twitter.com/2"

    def get_bearer_header(self):
        return {
            "Authorization": f"Bearer {self.BEARER_TOKEN}"
        }
        # r.headers["User-Agent"] = "v2TweetLookupPython"

    def search_tweet(self, ids: str, fields: str="author_id,lang"):
        # Tweet field options include:
        # attachments, author_id, context_annotations,
        # conversation_id, created_at, entities, geo, id,
        # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
        # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
        # source, text, and withheld
        # self.log.info(f"{ids=}")
        self.log.info(locals())
        endpoint = f"{self.BASE_URL}/tweets"
        headers = self.get_bearer_header()
        additional_headers = {}
        headers.update(additional_headers)
        data = urlencode({
            "ids": ids,
            "tweet.fields": fields
        })
        lookup_url = f"{endpoint}?{data}"
        r = requests.request("GET", lookup_url, headers=headers)
        if r.status_code not in range(200, 300): # if not 200-299, then unsuccessful
            self.log.error(f"Response Error - Non 200 - {r.status_code=}")
            # throw exception?
            return {}
        r_json = r.json()
        # if "errors" in r_json:
        #     self.log.error(f"API Error - {r_json['errors']=}")
        return r_json

# %%
# demo scrape for singluar id
twitter = TwitterAPI()
res = twitter.search_tweet("1213330173736738817")
# res = twitter.search_tweet("1214195710301618178", fields="")
print()
print(res)
