"""
module for logging state
"""
import os
from datetime import datetime
import json
import logging
log = logging.getLogger()

import pytz


class StateLogger(object):
    def __init__(self, fn):
        """
        `fn` - filename to use for logging
        """
        self.fn = fn
        self.values = {}

        self.read()

    def read(self):
        """
        Reads the state log and updates self.values
        """
        if not os.path.exists(self.fn):
            return
        with open(self.fn) as f:
            for line in f:
                # Find trailing json document
                values_index = line.find(" {")
                values = json.loads(line[values_index + 1:])
                for k, v in values.items():
                    self.values[k] = v

    def log(self, msg, **kwargs):
        """
        Adds msg to the log
        Any additional keyword arguments are treated as key/value pairs to save in the state log
        """
        now = datetime.now(pytz.utc).isoformat()
        values = json.dumps(kwargs, separators=(',:'))
        for k, v in kwargs.items():
            self.values[k] = v
        line = "{now} {msg} {values}".format(
            now=now,
            msg=msg,
            values=values,
        )
	log.info(line)
        with open(self.fn, 'a+') as f:
            f.write(line)
            f.write("\n")

    def get(self, key, default=None):
	return self.values.get(key, default)

if __name__ == '__main__':
    s = StateLogger("test.log")

    print s.values
    s.log("ohai", state="ready")
    print s.values
