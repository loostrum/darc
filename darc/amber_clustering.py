#!/usr/bin/env python
#
# AMBER Clustering

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

from darc.definitions import *
from darc.logger import get_logger
from darc.external import tools


class AMBERClusteringException(Exception):
    pass


class AMBERClustering(threading.Thread):
    """
    Cluster AMBER clusters
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.amber_queue = None
        self.cluster_queue = None

        self.hdr_mapping = {}
        self.start_time = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['amber_clustering']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)
        self.logger.info("AMBER Clustering initialized")

    def set_source_queue(self, queue):
        """
        :param queue: Source of amber clusters
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise AMBERClusteringException('Given source queue is not an instance of Queue')
        self.amber_queue = queue

    def set_target_queue(self, queue):
        """
        :param queue: Output queue for clusters
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given target queue is not an instance of Queue')
            raise AMBERClusteringException('Given target queue is not an instance of Queue')
        self.cluster_queue = queue

    def run(self):
        if not self.amber_queue:
            self.logger.error('AMBER trigger queue not set')
            raise AMBERClusteringException('AMBER trigger queue not set')
        if not self.cluster_queue:
            self.logger.error('Cluster queue not set')
            raise AMBERClusteringException('Cluster queue not set')

        self.logger.info("Starting AMBER clustering")
        while not self.stop_event.is_set():
            # read clusters for _interval_ seconds
            amber_triggers = []
            tstart = time()
            curr_time = tstart
            while curr_time < tstart + self.interval and not self.stop_event.is_set():
                curr_time = time()
                try:
                    data = self.amber_queue.get(timeout=.1)
                except Empty:
                    continue
                # store trigger
                amber_triggers.append(data)

            # start processing in thread
            if amber_triggers:
                proc_thread = threading.Thread(target=self.process_triggers, args=[amber_triggers])
                proc_thread.daemon = True
                proc_thread.start()
            else:
                self.logger.info("No triggers received")
        self.logger.info("Stopping AMBER clustering")

    def process_triggers(self, triggers):
        """
        Applies thresholding to clusters
        Put approved clusters on queue
        :param triggers: list of clusters to process
        """
        self.logger.info("Starting processing of {} AMBER triggers".format(len(triggers)))
        # TEMP: set observation start time to now
        # ToDo: proper start time
        self.start_time = time()
        # check for header (always, because it is received once for every amber instance)
        if not self.hdr_mapping:
            for trigger in triggers:
                if trigger.startswith('#'):
                    # read header, remove comment symbol
                    header = trigger.split()[1:]
                    self.logger.info("Received header: {}".format(header))
                    # Check if all required params are present and create mapping to col index
                    keys = ['beam_id', 'integration_step', 'time', 'DM', 'SNR']
                    for key in keys:
                        try:
                            self.hdr_mapping[key] = header.index(key)
                        except ValueError:
                            self.logger.error("Key missing from clusters header: {}".format(key))
                            self.hdr_mapping = {}
                            return
                    # triggers is now empty if only header was received
                    if not triggers:
                        self.logger.info("Only header received - Canceling processing")
                        return

        if not self.hdr_mapping:
            self.logger.error("First clusters received but header not found")
            return

        # remove headers from triggers (i.e. any trigger starting with #)
        triggers = [trigger for trigger in triggers if not trigger.startswith('#')]

        # split strings and convert to numpy array
        try:
            triggers = np.array(list(map(lambda val: val.split(), triggers)), dtype=float)
        except Exception as e:
            self.logger.error("Failed to process triggers: {}".format(e))
            return

        # pick columns to feed to clustering algorithm
        triggers_for_clustering = triggers[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'],
                                               self.hdr_mapping['time'], self.hdr_mapping['integration_step'])]
        self.logger.info("Clustering")
        # ToDo: feed other obs parameters
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, _ = \
            tools.get_triggers(triggers_for_clustering,
                               tab=triggers[:, self.hdr_mapping['beam_id']])
        self.logger.info("Clustered {} raw triggers into {} clusters".format(len(triggers_for_clustering),
                                                                             len(cluster_snr)))

        # Apply thresholds
        self.logger.info("Applying thresholds")
        age = (cluster_time + self.start_time) - time()
        age_max_ok = age <= self.age_max
        dm_min_ok = cluster_dm >= self.dm_min
        dm_max_ok = cluster_dm <= self.dm_max
        snr_min_ok = cluster_snr >= self.snr_min
        mask = dm_min_ok & dm_max_ok & snr_min_ok & age_max_ok
        if np.any(mask):
            self.logger.info("Clusters after thresholding: {}. Putting clusters on queue".format(np.sum(mask)))
            # put good clusters on queue
            clusters = np.transpose([cluster_snr[mask], cluster_dm[mask], cluster_time[mask], cluster_downsamp[mask]])
            columns = {'SNR': 0, 'DM': 1, 'time': 2, 'integration_step': 3}
            self.cluster_queue.put({'clusters': clusters, 'columns': columns})
        else:
            self.logger.info("No clusters after thresholding")

