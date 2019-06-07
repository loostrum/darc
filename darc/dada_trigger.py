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

from darc.definitions import *
from darc import util
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

        # read dbevent listening ports from arts_survey_control config
        # if this fails for any reason, the defaults in the darc config file are used
        try:
            with open(self.arts_survey_control_conf) as f:
                raw_arts_config = f.read()
            arts_survey_config = util.parse_parset(raw_arts_config)
            self.port_i = arts_survey_config['ARTSSurveyControl.network_port_event_i']
            self.port_iquv = arts_survey_config['ARTSSurveyControl.network_port_event_iquv']
        except Exception as e:
            self.logger.warning("Failed to read dada_dbevent ports from arts_survey_control config"
                                " Using defaults from DARC config ({})".format(e))

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

        while not self.stop_event.is_set():
            try:
                trigger = self.event_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                thread = threading.Thread(target=self.send_event, args=[trigger])
                thread.daemon = True
                thread.start()

        self.logger.info("Stopping DADA trigger")

    def send_event(self, trigger):
        """
        Send trigger to dada_dbevent
        :param trigger: trigger dictionary
        """
        self.logger.info("Received trigger")
        # event parameters
        if trigger['stokes'].upper() == 'I':
            port = self.port_i
            window_size = self.window_size_i
        elif trigger['stokes'].upper() == 'IQUV':
            port = self.port_iquv
            window_size = self.window_size_iquv
        else:
            self.logger.error("Unknown stokes type: {}".format(trigger['stokes']))
            return

        event_start_full = Time(trigger['utc_start']) + TimeDelta(trigger['time'], format='sec') - \
                                TimeDelta(window_size / 2, format='sec')
        event_end_full = event_start_full + TimeDelta(window_size, format='sec')

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
            sock.connect(("localhost", port))
        except socket.error as e:
            self.logger.error("Failed to connect to stokes {} dada_dbevent on port {}: {}".format(trigger['stokes'],
                                                                                                  port, e))
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
