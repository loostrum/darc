#!/usr/bin/env python
#
# Logic for listening on a network port

from time import sleep
import socket
import yaml
import logging
import logging.handlers
from queue import Queue
import threading

from darc.definitions import *


class AMBERListenerException(Exception):
    pass


class AMBERListener(threading.Thread):
    """
    Listens to AMBER triggers and puts them in a queue.
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['amber_listener']

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

    def set_target_queue(self, queue):
        if not isinstance(queue, Queue):
            self.logger.error('Given target queue is not instance of Queue')
            raise AMBERListenerException('Given target queue is not instance of Queue')
        self.queue = queue

    def run_once(self):
        if not self.queue:
            self.logger.error('Queue not set')
            raise AMBERListenerException('Queue not set')

        self.logger.info("Starting AMBER listener")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((self.host, self.port))
        except socket.error as e:
            self.logger.error("Failed to create socket: {}".format(e))
            return False

        s.listen(5)
        self.logger.info("Waiting for client to connect")
        client, adr = s.accept()
        self.logger.info("Accepted connection from (host, port) = {}".format(adr))

        while True:
            output = client.recv(1024)
            if output.strip() == 'EOF' or not output:
                self.logger.info("Disconnecting")
                client.close()
                return True
            else:
                self.queue.put(output.strip().split('\n'))

    def run(self):
        while not self.stop_event.is_set():
            if not self.run_once():
                # failed to start - wait before retrying
                self.stop_event.wait(timeout=5)
        self.logger.info("Stopping AMBER Listener")
