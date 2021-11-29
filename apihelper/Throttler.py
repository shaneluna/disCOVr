import datetime
import time 
from .LogHandler import LogHandler

class Throttler(LogHandler):
    """
    Throttler for a single api endpoint.
    Can be imporoved for parallelization if high api rate given in future.
    """

    def __init__(self, max_requests, window):
        super(Throttler, self).__init__()
        self.max_requests = max_requests
        self.window = window
        self.num_requests = 0
        self.next_reset_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.window)

    def reset_num_requests(self):
        self.num_requests = 0
        self.next_reset_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.window)

    def check_next_reset_at(self):
        if datetime.datetime.utcnow() >= self.next_reset_at:
            self.reset_num_requests()

    def halt(self, wait_time):
        print(f"Waiting {wait_time} seconds")
        self.log.info(f"Throttling | Num Requests: {self.num_requests}, Next Reset: {self.next_reset_at}, Wait Time: {wait_time}")
        time.sleep(wait_time + 0.5)
    
    def request(self, api_obj, method, val):
        print("Making request")
        # reset the count if the period passed
        self.check_next_reset_at()

        # check exceeded max rate
        # if yes, wait until reset
        if self.num_requests >= self.max_requests:
            self.halt( (self.next_reset_at - datetime.datetime.utcnow()).seconds )
            # after waiting need to potentially reset before continuing for accurate counts; else counts potentially off by 1
            self.check_next_reset_at()

        # make request and increment count
        self.num_requests += 1
        method_to_call = getattr(api_obj, method)
        ret = method_to_call(val) # potential exception?
        print("Request complete")
        return ret

# from TwitterAPI import TwitterAPI
# twitter = TwitterAPI()
# throttler = Throttler(5, 30)

# for i in range(10):
#     print(i)
#     throttler.request(twitter, "search_tweets", "1213330173736738817,1214195710301618178,1223118424814968832")