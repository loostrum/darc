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
            self.logger.error('AMBER cluster queue not set')
            raise AMBERClusteringException('AMBER cluster queue not set')
        if not self.cluster_queue:
            self.logger.error('Cluster queue not set')
            raise AMBERClusteringException('Cluster queue not set')

        self.logger.info("Starting AMBER clustering")
        while not self.stop_event.is_set():
            # read clusters for _interval_ seconds
            clusters = []
            tstart = time()
            curr_time = tstart
            while curr_time < tstart + self.interval and not self.stop_event.is_set():
                curr_time = time()
                try:
                    data = self.amber_queue.get(timeout=.1)
                except Empty:
                    continue

                if isinstance(data, str):
                    clusters.append(data)
                elif isinstance(data, list):
                    clusters.extend(data)

            # start processing in thread
            if clusters:
                proc_thread = threading.Thread(target=self.process_clusters, args=[clusters])
                proc_thread.daemon = True
                proc_thread.start()
            else:
                self.logger.info("No clusters")
        self.logger.info("Stopping AMBER clustering")
            

    def process_clusters(self, clusters):
        """
        Applies thresholding to clusters
        Put approved clusters on queue
        :param clusters: list of clusters to process
        """
        self.logger.info("Starting processing of {} clusters".format(len(clusters)))
        # check for header
        if not self.hdr_mapping:
            self.logger.info("Checking for header")
            for cluster in clusters:
                if cluster.startswith('#'):
                    # TEMP: set observation start time to now
                    self.start_time = time()
                    # read header, remove comment symbol
                    header = cluster.split()[1:]
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
                    # remove header from clusters
                    clusters.remove(cluster)
                    # clusters is now empty if only header was received
                    if not clusters:
                        self.logger.info("Only header received - Canceling processing")
                        return
                    else:
                        break

        if not self.hdr_mapping:
            self.logger.error("First clusters received but header not found")
            return


        # split strings
        clusters = np.array(list(map(lambda val: val.split(), clusters)), dtype=float)

        self.logger.info("Clustering")
        clusters_for_clustering = clusters[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'], self.hdr_mapping['time'], self.hdr_mapping['integration_step'])]

        # ToDo: feed other obs parameters
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, _ = tools.get_triggers(clusters_for_clustering, tab=clusters[:, self.hdr_mapping['beam_id']])
        self.logger.info("Clustered {} raw triggers into {} clusters".format(len(clusters_for_clustering), len(cluster_snr)))


        # Apply thresholds
        self.logger.info("Applying thresholds")
        age = time() - (cluster_time + 0)  # 0 to be replaced by start time of observation
        age_max = age <= self.age_max
        dm_min = cluster_dm >= self.dm_min
        dm_max = cluster_dm <= self.dm_max
        snr_min = cluster_snr >= self.snr_min
        mask = dm_min & dm_max & snr_min & age_max
        if np.any(mask):
            self.logger.info("Clusters after thresholding: {}. Putting clusters on queue".format(np.sum(mask)))
            # put good clusters on queue
            clusters = np.transpose([cluster_snr[mask], cluster_dm[mask], cluster_time[mask], cluster_downsamp[mask]])
            columns = {'SNR': 0, 'DM':1, 'time': 2, 'integration_step': 3}
            self.cluster_queue.put({'clusters': clusters, 'columns': columns})
        else:
            self.logger.info("No clusters after thresholding")

