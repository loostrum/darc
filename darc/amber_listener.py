#!/usr/bin/env python
#
# AMBER Listener

from time import sleep, time
import socket
import yaml
import multiprocessing as mp
import threading
import socket

from darc.definitions import *
from darc.logger import get_logger


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
        Initalize socket and put incoming triggers on queue
        """
        if not self.queue:
            self.logger.error('Queue not set')
            raise AMBERListenerException('Queue not set')

        self.logger.info("Starting AMBER listener")
        s = None
        start = time()
        while not s and time() - start < self.socket_timeout:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((self.host, self.port))
            except socket.error as e:
                self.logger.warning("Failed to create socket, will retry: {}".format(e))
                sleep(1)

        if not s:
            self.logger.error("Failed to create socket")
            raise AMBERListenerException("Failed to create socket")

        s.listen(5)
        s.settimeout(1)

        while not self.stop_event.is_set():
            self.logger.info("Waiting for client to connect")
            # Use timeout to avoid hanging when stop of service is requested
            client = None
            adr = None
            while not client and not self.stop_event.is_set():
                try:
                    client, adr = s.accept()
                except socket.timeout:
                    continue
                # client may still not exist if stop is called some loop
                if not client or not adr:
                    continue
                self.logger.info("Accepted connection from (host, port) = {}".format(adr))

            # again client may still not exist if stop is called some loop
            if not client or not adr:
                continue
            # keep listening until we receive a stop
            client.settimeout(1)
            while not self.stop_event.is_set():
                try:
                    output = client.recv(1024)
                except socket.timeout:
                    continue
                if output.strip() == 'EOF' or not output:
                    self.logger.info("Disconnecting")
                    client.close()
                    break
                else:
                    self.queue.put(output.strip().split('\n'))

        self.logger.info("Stopping AMBER Listener")
