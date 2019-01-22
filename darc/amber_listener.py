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
from queue import Queue

from darc.definitions import *


class AMBERListenerException(Exception):
    pass


class AMBERListener(object):
    """
    Listens to AMBER triggers and puts them in a queue.
    """

    def __init__(self):
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
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

    def set_queue(self, queue):
        if not isinstance(queue, Queue):
            self.logger.error('Given queue is not instance of Queue')
            raise AMBERListenerException('Given queue is not instance of Queue')
        self.queue = queue

    def start(self):
        if not self.queue:
            self.logger.error('Queue not set')
            raise AMBERListenerException('Queue not set')

        self.logger.info("Starting AMBER listener")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.SO_REUSEADDR)
            s.bind((self.host, self.port))
        except socket.error:
            self.logger.error("Failed to create socket")
            return False
        
        s.listen(5)
        self.logger.info("Waiting for client to connect")
        client, _ = s.accept()

        while True:
            output = client.recv(1024)
            if output.strip() == 'EOF' or not output:
                self.logger.info("Disconnecting")
                client.close()
                return True
            else:
                for line in output.strip().split('\n'):
                    self.queue.put(line)

    def run_forever(self):
        while True:
            if not self.start():
                # failed to start - wait before retrying
                sleep(1)
            

if __name__ == '__main__':
    listener = AMBERListener()
    listener.start()
