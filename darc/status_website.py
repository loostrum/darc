#!/usr/bin/env python
#
# Website 

import yaml
import logging
import logging.handlers
import multiprocessing as mp
import threading


class StatusWebsiteException(Exception):
    pass


class StatusWebsite(threading.Thread):
    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self.daemon = True

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['status_website']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~')}
        for key, value in config.items():
             if isinstance(value, str):
                value = value.format(**kwargs)
                setattr(self, key, value)

        # setup logger
        handler = logging.handlers.WatchedFileHandler(self.log_file)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        self.logger.info('Initialized')

        # create website directory
        try:
            os.makedirs(self.web_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                self.logger.error("Failed to create website directory: {}".format(e))
                raise StatusWebsiteException("Failed to create website directory: {}".format(e))

