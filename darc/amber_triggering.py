#!/usr/bin/env python
#
# AMBER Triggering

import yaml
import logging
import logging.handlers
from time import time
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
import numpy as np

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

        self.header = None
        self.start_time = None

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
        """
        :param queue: Source of amber triggers
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise AMBERTriggeringException('Given source queue is not an instance of Queue')
        self.amber_queue = queue

    def set_target_queue(self, queue):
        """
        :param queue: Output queue for triggers
        """
        if not isinstance(queue, mp.queues.Queue):
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
                curr_time = time()
                try:
                    data = self.amber_queue.get(timeout=.1)
                except Empty:
                    continue

                if isinstance(data, str):
                    triggers.append(data)
                elif isinstance(data, list):
                    triggers.extend(data)

            # start processing in thread
            if triggers:
                proc_thread = threading.Thread(target=self.process_triggers, args=[triggers])
                proc_thread.daemon = True
                proc_thread.start()
            else:
                self.logger.info("No triggers")
        self.logger.info("Stopping AMBER triggering")

    def process_triggers(self, triggers):
        """
        Applies thresholding to triggers
        Put approved triggers on queue
        :param triggers: list of triggers to process
        """
        self.logger.info("Starting processing of {} triggers".format(len(triggers)))
        # check for header
        if triggers[0].startswith('#'):
            # TEMP: set observation start time to now
            self.start_time = time()
            # read header
            self.header = triggers[0].split()
            self.logger.info("Received header: {}".format(self.header))
            # Check if all required params are present
            keys = ['beam_id', 'integration_step', 'time', 'DM', 'SNR']
            for key in keys:
                if key not in self.header:
                    self.logger.error("Key missing from triggers header: {}".format(key))
                    raise AMBERTriggeringException("Key missing from triggers header")
            # remove header from triggers
            # might have been only "trigger" so catch IndexError
            try:
                triggers = triggers[1:]
            except IndexError:
                self.logger.info("Only header received - Canceling processing")
                return

        if not self.header:
            self.logger.error("Trigger received but header not set")
            return

        # set everything as float and make accessible by column name
        # there is probably a better way to do this...
        dtype = zip(self.header, [np.float]*len(self.header))
        # split strings
        triggers = map(lambda val: val.split(), triggers)
        triggers = np.array(triggers, dtype=dtype)

        self.logger.info("Applying thresholds")
        # get age of triggers
        age = time() - (triggers['time'] + self.start_time)
        # do thresholding
        dm_min = triggers['DM'] > self.dm_min
        dm_max = triggers['DM'] < self.dm_max
        snr_min = triggers['SNR'] > self.snr_min
        age_max = age < self.age_max
        good_triggers_mask = dm_min & dm_max & snr_min & age_max
        self.logger.info("Found {} good triggers".format(np.sum(good_triggers_mask)))
        if np.any(good_triggers_mask):
            good_triggers = triggers[good_triggers_mask]
            # find trigger with highest S/N
            ind = np.argmax(good_triggers['SNR'])
            trigger = good_triggers[ind]
            # put trigger on queue
            self.logger.info("Putting trigger on queue: {}".format(trigger))
            self.voevent_queue.put(trigger)
