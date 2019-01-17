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

from darc.definitions import *



class Server(object):
    """
    Sets up a server that listens on a socket.
    """

    def __init__(self, mode):
        self.mode = mode

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)[self.mode]

        # set config, expanding strings
        home = os.path.expanduser('~')
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(home=home)
            setattr(self, key, value)

        self.received_data = []

        # setup logger
        handler = logging.handlers.WatchedFileHandler(self.log_file)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        #logging.basicConfig(filename=self.log_file, format='%(asctime)s %(message)s', level=logging.DEBUG)

    def get_mode(self):
        return self.mode

    def clear_data(self):
        self.received_data = []

    def _start(self):
        self.logger.info("Starting listener for mode: {}".format(self.mode))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.SO_REUSEADDR)
            s.bind((self.host, self.port))
        except socket.error:
            self.logger.error("Failed to create socket")
            return False
        
        s.listen(5)
        self.logger.info("Waiting for client to connect")
        clientSocket, addr = s.accept()

        while True:
            output = clientSocket.recv(1024)
            if output.strip() == 'EOF' or not output:
                self.logger.info("Disconnecting")
                clientSocket.close()
                return True
            else:
                self.received_data.append(output.strip().split('\n'))

    def run_once(self):
        self._start()

    def run(self):
        while True:
            if not self._start():
                # failed to start - wait before retrying
                sleep(1)
            

if __name__ == '__main__':
    server = Server(mode='amber_listener')
    server.run_once()
