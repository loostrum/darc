#!/usr/bin/env python
#
# Logic for listening on a network port

from time import sleep
import os
import sys
import socket
import yaml
import logging
import logging.handlers
from time import sleep, time
from queue import Queue, Empty
import threading

from darc.definitions import *


class VOEventGeneratorException(Exception):
    pass


class VOEventGenerator(threading.Thread):
    """
    Generate VOEvent from incoming trigger
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self.daemon = True

        self.voevent_queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['voevent_generator']

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

    def set_source_queue(self, queue):
        if not isinstance(queue, Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise VOEventGeneratorException('Given source queue is not an instance of Queue')
        self.voevent_queue = queue

    def run(self):
        if not self.voevent_queue:
            self.logger.error('Queue not set')
            raise VOEventGeneratorException('Queue not set')

        self.logger.info("Running VOEvent dummy")
        while not self.stop_event.is_set():
            self.stop_event.wait(timeout=5)
        self.logger.info("Stopping VOEvent dummy")
