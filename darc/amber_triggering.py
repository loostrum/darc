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


class AMBERTriggeringException(Exception):
    pass


class AMBERTriggering(threading.Thread):
    """
    Process AMBER triggers and turn into trigger message
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.amber_queue = None
        self.voevent_queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['amber_triggering']

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
            raise AMBERTriggeringException('Given source queue is not an instance of Queue')
        self.amber_queue = queue

    def set_target_queue(self, queue):
        if not isinstance(queue, Queue):
            self.logger.error('Given target queue is not an instance of Queue')
            raise AMBERTriggeringException('Given target queue is not an instance of Queue')
        self.voevent_queue = queue

    def run(self):
        if not self.amber_queue:
            self.logger.error('AMBER trigger queue not set')
            raise AMBERTriggeringException('AMBER trigger queue not set')
        if not self.voevent_queue:
            self.logger.error('VOEvent queue not set')
            raise AMBERTriggeringException('AMBER trigger queue not set')

        self.logger.info("Starting AMBER triggering")
        while not self.stop_event.is_set():
            # read triggers for _interval_ seconds
            triggers = []
            tstart = time()
            curr_time = tstart
            while curr_time < tstart + self.interval and not self.stop_event.is_set():
                # update time
                # Read from queue (non-blocking)
                try:
                    data = self.amber_queue.get_nowait()
                except Empty:
                    curr_time = time()
                    sleep(.1)
                    continue

                if isinstance(data, str):
                    triggers.append(data)
                elif isinstance(data, list):
                    triggers.extend(data)

                curr_time = time()
                sleep(.1)

            # start processing in thread
            if triggers:
                self.logger.info("Starting processing of {} triggers".format(len(triggers)))
                proc_thread = threading.Thread(target=self.process_triggers, args=[triggers])
                proc_thread.daemon = True
                proc_thread.start()
            else:
                self.logger.info("No triggers")
        self.logger.info("Stopping AMBER triggering")

    def process_triggers(self, triggers):
        pass