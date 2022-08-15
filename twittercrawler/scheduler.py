import time, datetime, os
import numpy as np
from collections import deque
from twython.exceptions import TwythonError


class RequestScheduler():
    def __init__(self, time_frame, max_requests, sync_time, verbose=False):
        """Abstract scheduler object. It enables only 'max_requests' requests in every 'time_frame' seconds."""
        # scheduler parameters
        self.time_frame = time_frame
        self.max_requests = max_requests
        self.sync_time = sync_time
        self.verbose = verbose
        self._requests = deque([])
        self._writers = None

    def connect_output(self, writers):
        """Connect to a list of writer objects"""
        self._writers = writers

    def close(self):
        """Close writer objects"""
        try:
            if self._writers != None:
                for writer in self._writers:
                    writer.close()
            print("Connection was closed successfully!")
        except:
            raise

    def _check_remaining_limit(self, twitter_api, current_time):
        valid, wait_for = True, self.sync_time
        try:
            num_remaining = int(twitter_api.get_lastfunction_header('x-rate-limit-remaining'))
            rate_limit_reset = int(twitter_api.get_lastfunction_header('x-rate-limit-reset'))
            valid = num_remaining > 0
            wait_for = rate_limit_reset - current_time + self.sync_time
        except TwythonError:
            print("No former request were made!")
        except:
            raise
        finally:
            return valid, wait_for

    def _verify_new_request(self, twitter_api):
        print("010-_verify_new_request")
        """Return only when a request can be made"""
        current_time = time.time()
        valid, wait_for = self._check_remaining_limit(twitter_api, current_time)
        print('verify request', "valid=",valid, "wait_for=",wait_for)
        if valid:
            print('201')
            while len(self._requests) > 0 and current_time - self._requests[0] > self.time_frame:
                self._requests.popleft()
            print('202')
            if len(self._requests) >= self.max_requests:
                print('203')
                wait_for = self.time_frame - (current_time - self._requests[0]) + self.sync_time
                print("_verify_new_request_VERIFYING: sleeping for %.1f seconds" % wait_for)
                time.sleep(wait_for)
                # return False
        else:
            print("_verify_new_request_RATE LIMIT RESET in %.1f seconds" % wait_for)
            time.sleep(wait_for)
            return False
        return True

    def _register_request(self, delta_t, dev_ratio=0.1):
        """Register a request with time stamp"""
        self._requests.append(time.time())
        wait_for = np.random.normal(loc=delta_t, scale=delta_t * dev_ratio)
        if self.verbose:
            print("A REQUEST was made: sleeping for %.1f seconds" % wait_for)
        time.sleep(wait_for)


