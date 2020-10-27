#!/usr/bin/env python3
#
# AMBER Listener

import os
from time import sleep, time
import threading
import ast

from darc import DARCBase
from darc import util


class AMBERListenerException(Exception):
    pass


class AMBERListener(DARCBase):
    """
    Continuously read AMBER candidate files from disk and put
    candidates on output queue.
    """

    def __init__(self):
        """
        """
        super(AMBERListener, self).__init__()
        self.needs_target_queue = True

        self.observation_threads = []
        self.observation_events = []

    def start_observation(self, obs_config, reload=True):
        """
        Start an observation

        :param dict obs_config: observation config dict
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

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
        for step in range(1, num_amber + 1):
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
        Stop observation
        """
        for event in self.observation_events:
            event.set()
        self.observation_events = []
        for thread in self.observation_threads:
            thread.join()
        self.observation_threads = []

    # only start and stop observation commands exist for amber listener
    def process_command(self):
        pass

    def _follow_file(self, fname, event):
        """
        Tail a file an put lines on queue

        :param str fname: file to follow
        :param threading.Event event: stop event
        """
        # wait until the file exists, with a timeout
        start = time()
        while time() - start < self.start_timeout and not event.is_set():
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
        self.logger.info("Following file: {}".format(fname))
        with open(fname, 'r') as f:
            for line in util.tail(f, event):
                line = line.strip()
                if line:
                    # put trigger on queue
                    self.target_queue.put({'command': 'trigger', 'trigger': line})
                    # add same trigger to second queue if present
                    if self.second_target_queue is not None:
                        self.second_target_queue.put({'command': 'trigger', 'trigger': line})
