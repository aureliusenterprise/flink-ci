import logging
import time
from pyflink.datastream import MapFunction


class DelayedMap(MapFunction):
    """ DelayedMap delays events, in case we are importing from Apache Atlas """

    def __init__(self):
        super(DelayedMap, self).__init__()
        self.last_timestamp = None
        self.time_to_sleep_s = 1
        self.time_between_messages_ms = 500

    def map(self, value):
        if isinstance(value, Exception):
            return value

        current_timestamp = time.time()

        if self.last_timestamp is not None:
            time_diff_ms = (current_timestamp - self.last_timestamp) * 1000
            if time_diff_ms < self.time_between_messages_ms:
                logging.debug("Delaying by %s seconds.", self.time_to_sleep_s)
                time.sleep(self.time_to_sleep_s)

        self.last_timestamp = time.time()

        return value
