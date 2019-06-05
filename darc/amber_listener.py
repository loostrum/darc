#!/usr/bin/env python
#
# AMBER Listener

from time import sleep, time
import yaml
import multiprocessing as mp
import threading
import socket

from darc.definitions import *
from darc.logger import get_logger
from darc import util


class AMBERListenerException(Exception):
    pass


class AMBERListener(threading.Thread):
    """
    Listens to AMBER triggers and puts them on a queue.
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.queue = None
        self.observation = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['amber_listener']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)
        self.logger.info("AMBER Listener initialized")

    def set_target_queue(self, queue):
        """
        :param queue: Output queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given target queue is not instance of Queue')
            raise AMBERListenerException('Given target queue is not instance of Queue')
        self.queue = queue

    def run(self):
        """
        Initialize readers
        """
        if not self.queue:
            self.logger.error('Queue not set')
            raise AMBERListenerException('Queue not set')

        self.logger.info("Starting AMBER listener")
        while not self.stop_event.is_set():
            sleep(1)
        self.logger.info("Stopping AMBER listener")
            self.stop_observation()


    def start_observation(self, obs_config):
        """
        Start an observation
        :param obs_config: observation config dict
        :return:
        """
        #thread = threading.Thread(target=self._start_observation_master, args=[obs_config], name=taskid)
        #thread.daemon = True
        #self.threads[taskid] = thread
        amber_dir = obs_config['amber_dir']


        #self.queue.put(output.strip().split('\n'))

        #self.logger.info("Stopping AMBER Listener")

    def stop_observation(self):
        """
        Stop any running observation
        """
        for thread in self.observation_threads:
            thread.stop_event.set()

    def follow_file(self, fname, event):
        """
        Tail a file an put lines on queue
        :param fname: file to follow
        :param event: stop event
        """
        with open(fname, 'r') as f:
            for line in util.tail(f, event):
                self.queue.put(line.strip())