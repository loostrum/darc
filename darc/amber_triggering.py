#!/usr/bin/env python
#
# AMBER Triggering

import os
import yaml
from time import time
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
import socket
import numpy as np

from darc.definitions import MASTER
from darc.logger import get_logger
from darc.external import tools
from darc.voevent_generator import VOEventQueueServer


class AMBERTriggeringException(Exception):
    pass


class AMBERTriggering(threading.Thread):
    """
    Process AMBER triggers and turn into trigger message
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.amber_queue = None
        #self.voevent_queue = None
        self.cluster_queue = None

        self.hdr_mapping = {}
        self.start_time = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['amber_triggering']
            self.vo_config = yaml.load(f, Loader=yaml.SafeLoader)['voevent_generator']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # Setup VO Event queue server
        VOEventQueueServer.register('get_queue')
        host = MASTER
        port = self.vo_config['server_port']
        auth = self.vo_config['server_auth']
        self.voevent_queue_server = VOEventQueueServer(address=(host, port), authkey=auth)

        self.logger.info("AMBER Triggering initialized")

    def set_source_queue(self, queue):
        """
        :param queue: Source of amber triggers
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise AMBERTriggeringException('Given source queue is not an instance of Queue')
        self.amber_queue = queue

    def run(self):
        if not self.amber_queue:
            self.logger.error('AMBER trigger queue not set')
            raise AMBERTriggeringException('AMBER trigger queue not set')

        self.logger.info("Starting AMBER triggering")
        while not self.stop_event.is_set():
            # read triggers for _interval_ seconds
            triggers = []
            tstart = time()
            curr_time = tstart
            while curr_time < tstart + self.interval and not self.stop_event.is_set():
                curr_time = time()
                try:
                    data = self.amber_queue.get(timeout=.1)
                except Empty:
                    continue

                if isinstance(data, str):
                    triggers.append(data)
                elif isinstance(data, list):
                    triggers.extend(data)

            # start processing in thread
            if triggers:
                proc_thread = threading.Thread(target=self.process_triggers, args=[triggers])
                proc_thread.daemon = True
                proc_thread.start()
            else:
                self.logger.info("No triggers")
        self.logger.info("Stopping AMBER triggering")

    def process_triggers(self, triggers):
        """
        Applies thresholding to triggers
        Put approved triggers on queue
        :param triggers: list of triggers to process
        """
        self.logger.info("Starting processing of {} triggers".format(len(triggers)))
        # check for header
        if not self.hdr_mapping:
            self.logger.info("Checking for header")
            for trigger in triggers:
                if trigger.startswith('#'):
                    # TEMP: set observation start time to now
                    self.start_time = time()
                    # read header, remove comment symbol
                    header = trigger.split()[1:]
                    self.logger.info("Received header: {}".format(header))
                    # Check if all required params are present and create mapping to col index
                    keys = ['beam_id', 'integration_step', 'time', 'DM', 'SNR']
                    for key in keys:
                        try:
                            self.hdr_mapping[key] = header.index(key)
                        except ValueError:
                            self.logger.error("Key missing from triggers header: {}".format(key))
                            self.hdr_mapping = {}
                            return
                    # remove header from triggers
                    triggers.remove(trigger)
                    # triggers is now empty if only header was received
                    if not triggers:
                        self.logger.info("Only header received - Canceling processing")
                        return
                    else:
                        break

        if not self.hdr_mapping:
            self.logger.error("First triggers received but header not found")
            return

        # split strings
        triggers = np.array(list(map(lambda val: val.split(), triggers)), dtype=float)

        self.logger.info("Clustering")
        triggers_for_clustering = triggers[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'], 
                                               self.hdr_mapping['time'], self.hdr_mapping['integration_step'])]

        # ToDo: feed other obs parameters
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, _ = tools.get_triggers(triggers_for_clustering, tab=triggers[:, self.hdr_mapping['beam_id']])
        self.logger.info("Clustering done")
        self.logger.info("Generating VO for highest S/N")
        ind = np.argmax(cluster_snr)
        voevent_trigger = {'dm': cluster_dm[ind], 'dm_err': 0,
                          'width': cluster_downsamp[ind]*81.92E-3,
                          'snr': cluster_snr[ind], 'flux': 0,
                          'ra': 83.63322083333333, 'dec': 22.01446111111111,
                          'ymw16': 0, 'semiMaj': 15., 'semiMin': 15., 'name': 'B0531+21',
                          'importance': 0.1, 'utc': '2019-01-01-18:00:00.0'}
        
        self.logger.info("Putting trigger on voevent queue: {}".format(voevent_trigger))
        self.voevent_queue_server.connect()
        voevent_queue = self.voevent_queue_server.get_queue()
        voevent_queue.put(voevent_trigger)
