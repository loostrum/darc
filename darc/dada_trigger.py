#!/usr/bin/env python
#
# dada_dbevent triggers

import os
import yaml
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
from textwrap import dedent
import socket
from astropy.time import Time, TimeDelta

from darc.definitions import CONFIG_FILE
from darc.logger import get_logger


class DADATriggerException(Exception):
    pass


class DADATrigger(threading.Thread):
    """
    Generate and send dada_dbevent triggers
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.event_queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['dada_trigger']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)
        self.logger.info("DADA trigger initialized")

    def set_source_queue(self, queue):
        """
        :param queue: Source of amber clusters
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise DADATriggerException('Given source queue is not an instance of Queue')
        self.event_queue = queue

    def run(self):
        if not self.event_queue:
            self.logger.error('DADA trigger queue not set')
            raise DADATriggerException('DADA trigger queue not set')

        self.logger.info("Starting DADA trigger")

        thread = None
        while not self.stop_event.is_set():
            try:
                command = self.event_queue.get(timeout=.1)
            except Empty:
                continue
            # dada trigger is observation agnostic, so does not need to act on start/stop
            if command['command'] in ['start_observation', 'stop_observation']:
                pass
            elif command['command'] == 'trigger':
                # trigger received, send to dada_dbevent
                thread = threading.Thread(target=self.send_event, args=[command['trigger']])
                thread.daemon = True
                thread.start()
            else:
                self.logger.error("Unknown command received: {}".format(command['command']))

        self.logger.info("Stopping DADA trigger")
        # wait for last thread to finish
        if thread:
            thread.join()

    def send_event(self, trigger):
        """
        Send trigger to dada_dbevent
        :param trigger: trigger dictionary
        """
        self.logger.info("Received trigger")

        # ensure utc_start is in iso format
        trigger['utc_start'] = trigger['utc_start'].iso

        event_start_full = Time(trigger['utc_start']) + TimeDelta(trigger['time'], format='sec') - \
                                   TimeDelta(trigger['window_size'] / 2, format='sec')
        event_end_full = event_start_full + TimeDelta(trigger['window_size'], format='sec')

        event_start, event_start_frac = event_start_full.iso.split('.')
        # event_start_frac = '.' + event_start_frac
        event_end, event_end_frac = event_end_full.iso.split('.')
        # event_end_frac = '.' + event_end_frac

        # Add utc start/end for event
        trigger['event_start'] = event_start
        trigger['event_start_frac'] = event_start_frac
        trigger['event_end'] = event_end
        trigger['event_end_frac'] = event_end_frac

        # create event. the "\" ensures the string does not start with a newline
        # the actual newline needs to be there to make dedent work properly
        event = dedent("""\
                       N_EVENTS 1
                       {utc_start}
                       {event_start} {event_start_frac} {event_end} {event_end_frac} {snr} {dm} {width} {beam}
                       """.format(**trigger))

        # open socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(("localhost", trigger['port']))
        except socket.error as e:
            self.logger.error("Failed to connect to stokes {} dada_dbevent on port {}: {}".format(trigger['stokes'],
                                                                                                  trigger['port'], e))
            return

        # send event
        try:
            sock.sendall(event.encode())
        except socket.timeout:
            self.logger.error("Failed to send stokes {} event".format(trigger['stokes']))
            return
        self.logger.info("Successfully sent stokes {} event".format(trigger['stokes']))

        # close socket
        sock.close()
