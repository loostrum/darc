#!/usr/bin/env python
#
# DARC base class

import os
import socket
import threading
import multiprocessing as mp
import yaml
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from darc.logger import get_logger
from darc.definitions import CONFIG_FILE


class DARCBase(threading.Thread):
    """
    DARC Base class
    Provides common methods
    """

    def __init__(self):
        """
        Initialisation
        """
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = threading.Event()

        self.needs_source_queue = False
        self.needs_target_queue = True
        self.source_queue = None
        self.target_queue = None

        # name is like dada_trigger, but log should use Dada Trigger
        name = type(self).__name__
        self.log_name = name.replace('_', ' ').title()

        # load config
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)[name]

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(name, self.log_file)
        self.logger.info("{} initialized".format(self.log_name))

    def stop(self):
        """
        Set the stop event
        """
        self.stop_event.set()

    def set_source_queue(self, queue):
        """
        :param queue: Input queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error("Given source queue is not an instance of Queue")
            return

    def set_target_queue(self, queue):
        """
        :param queue: Output queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error("Given target queue is not an instance of Queue")
            return

    def run(self):
        """
        Main loop
        """
        # check queues
        if self.needs_source_queue and not self.source_queue:
            self.logger.error("Source queue not set")

        if self.needs_target_queue and not self.target_queue:
            self.logger.error("Source queue not set")

        self.logger.info("Starting {}".format(self.log_name))
        while not self.stop_event.is_set():
            # read from queue
            try:
                command = self.source_queue.get(timeout=.1)
            except Empty:
                continue
            # command received, process it
            if command['command'] == "start_observation":
                self.logger.info("Starting observation")
                try:
                    self.start_observation(command['obs_config'])
                except Exception as e:
                    self.logger.error("Failed to start observation: {}".format(e))
            elif command['command'] == "stop_observation":
                self.logger.info("Stopping observation")
                self.stop_observation()
            else:
                self.process_command(command)
        self.logger.info("Stopping {}".format(self.log_name))
        self.cleanup()

    def start_observation(self, *args, **kwargs):
        raise NotImplementedError("start_observation should be defined by subclass")

    def stop_observation(self, *args, **kwargs):
        raise NotImplementedError("stop_observation should be defined by subclass")

    def process_command(self, *args, **kwargs):
        raise NotImplementedError("process_command should be defined by subclass")

    def cleanup(self):
        raise NotImplementedError("cleanup should be defined by subclass")
