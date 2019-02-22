#!/usr/bin/env python
#
# DARC master process
# Controls all services

import sys
import ast
import yaml
import errno
import multiprocessing as mp
import threading

from darc.definitions import *
from darc.logger import get_logger
from darc.control import send_command


class ObservationControlException(Exception):
    pass

class ObservationControl(threading.Thread):
    def __init__(self, obs_config, host_type):
        threading.Thread.__init__(self)
        self.daemon = True

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['observation_control']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~')}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # store observation config and host type
        self.obs_config = obs_config
        self.host_type = host_type

        self.logger.info('ObservationControl initialized')

    def run(self):
        """
        Run either master or worker start_observation command
        """
        if self.host_type == 'master':
            self._start_observation_master()
        elif self.host_type == 'worker':
            self._start_observatoin_worker()
        else:
            self.logger.error("Unknown host type: {}".format(self.host_type))

    def _start_observation_master(self):
        """
        Start observation on master node
        """
        self.logger.info("Starting observation on master node")
        pass
        #send_command(self.timeout, service, 'status')

    def _start_observation_worker(self):
        """
        Start observation on worker node
        """
        self.logger.info("Starting observation on worker node")
        # make sure all service are started
        for service in ['amber_listener']; do:
            send_command(self.timeout, service, 'start')
