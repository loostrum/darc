#!/usr/bin/env python
#
# AMBER Listener

import os
from time import sleep, time
import yaml
import ast
import multiprocessing as mp
import threading
import socket
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from darc.definitions import CONFIG_FILE
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

        self.observation_queue = None
        self.amber_queue = None

        self.observation_threads = []
        self.observation_events = []

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

    def set_source_queue(self, queue):
        """
        :param queue: Output queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not instance of Queue')
            raise AMBERListenerException('Given source queue is not instance of Queue')
        self.observation_queue = queue

    def set_target_queue(self, queue):
        """
        :param queue: Output queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given target queue is not instance of Queue')
            raise AMBERListenerException('Given target queue is not instance of Queue')
        self.amber_queue = queue

    def run(self):
        """
        Initialize and wait for observations
        """
        if not self.observation_queue:
            self.logger.error('Observation queue not set')
            raise AMBERListenerException('Observation queue not set')
        if not self.amber_queue:
            self.logger.error('AMBER queue not set')
            raise AMBERListenerException('AMBER queue not set')

        self.logger.info("Starting AMBER listener")
        # Wait for observation command to arrive
        while not self.stop_event.is_set():
            # read queue
            try:
                command = self.observation_queue.get(timeout=1)
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
                self.logger.error("Unknown command received: {}".format(command['command']))
        self.logger.info("Stopping AMBER listener")
        self.stop_observation()

    def start_observation(self, obs_config):
        """
        Start an observation
        :param obs_config: observation config dict
        :return:
        """
        # Stop any running observation
        if self.observation_events or self.observation_threads:
            self.logger.info("Old observation found, stopping it first")
            self.stop_observation()

        self.logger.info("Reading AMBER settings")
        # Read number of amber threads from amber config file
        amber_conf_file = obs_config['amber_config']
        with open(amber_conf_file, 'r') as f:
            raw_amber_conf = f.read()
        amber_conf = util.parse_parset(raw_amber_conf)

        # get directory of amber trigger files
        amber_dir = obs_config['amber_dir']
        # get CB number
        beam = obs_config['beam']

        # start reader for each thread
        num_amber = len(ast.literal_eval(amber_conf['opencl_device']))
        self.logger.info("Expecting {} AMBER threads".format(num_amber))
        for step in range(1, num_amber+1):
            trigger_file = os.path.join(amber_dir, "CB{:02d}_step{}.trigger".format(beam, step))
            # start thread for reading
            event = threading.Event()
            self.observation_events.append(event)
            thread = threading.Thread(target=self._follow_file, args=[trigger_file, event], name="step{}".format(step))
            thread.daemon = True
            thread.start()
            self.observation_threads.append(thread)

        self.logger.info("Observation started")
        # ToDo: Automatic stop? Careful not to overwrite a new observation

    def stop_observation(self):
        """
        Stop any running observation
        """
        for event in self.observation_events:
            event.set()
        self.observation_events = []
        for thread in self.observation_threads:
            thread.join()
        self.observation_threads = []

    def _follow_file(self, fname, event):
        """
        Tail a file an put lines on queue
        :param fname: file to follow
        :param event: stop event
        """
        # wait until the file exists, with a timeout
        start = time()
        while time() - start < self.start_timeout:
            if not os.path.isfile(fname):
                self.logger.info("File not yet present: {}".format(fname))
                sleep(1)
            else:
                break
        if not os.path.isfile(fname):
            # file still does not exist, error
            self.logger.error("Giving up on following file: {}".format(fname))
            return
        # tail the file
        with open(fname, 'r') as f:
            for line in util.tail(f, event):
                line = line.strip()
                if line:
                    self.amber_queue.put({'command': 'trigger', 'trigger': line})
