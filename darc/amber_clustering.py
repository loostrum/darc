#!/usr/bin/env python
#
# AMBER Clustering

import os
import yaml
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
from time import sleep
import threading
import socket
import numpy as np
from astropy.time import Time, TimeDelta
import astropy.units as u

from darc.definitions import CONFIG_FILE, TSAMP, NCHAN, BANDWIDTH
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

        self.proc_thread = None
        self.hdr_mapping = {}
        self.obs_config = None
        self.observation_running = False
        self.amber_triggers = []

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
            # read queue
            try:
                command = self.amber_queue.get(timeout=1)
            except Empty:
                continue
            # command received, process it
            if command['command'] == "start_observation":
                self.logger.info("Starting observation")
                try:
                    self.start_observation(command['obs_config'])
                except Exception as e:
                    self.logger.error("Failed to start observation: {}".format(e))
            elif command['command'] == "stop_observation":
                self.logger.info("Stopping observation")
                self.stop_observation()
            elif command['command'] == 'trigger':
                if not self.observation_running:
                    self.logger.error("Trigger received but no observation is running - ignoring")
                else:
                    self.amber_triggers.append(command['trigger'])
            else:
                self.logger.error("Unknown command received: {}".format(command['command']))
        self.logger.info("Stopping AMBER clustering")
        self.stop_observation()

    def start_observation(self, obs_config):
        """
        Parse obs config and start listening for amber triggers on queue
        """

        # clean any old triggers
        self.amber_triggers = []
        # set config
        self.obs_config = obs_config

        self.observation_running = True

        # process triggers in thread
        self.proc_thread = threading.Thread(target=self.process_triggers)
        self.proc_thread.daemon = True
        self.proc_thread.start()

        self.logger.info("Observation started")

    def stop_observation(self):
        """
        Stop observation
        """
        # set running to false
        self.observation_running = False
        # clear triggers
        self.amber_triggers = []
        # clear header
        self.hdr_mapping = {}
        # clear config
        self.obs_config = None
        # clear the processing thead
        if self.proc_thread:
            self.proc_thread.join()
            self.proc_thread = None

    def process_triggers(self):
        """
        Applies thresholding to clusters
        Puts approved clusters on queue
        """

        # set observation parameters
        utc_start = Time(self.obs_config['startpacket'] / 781250., format='unix')
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq']*u.MHz + 0.5*BANDWIDTH).to(u.GHz).value # GHz
        network_port = self.obs_config['network_port_event_i']

        while self.observation_running:
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                triggers = self.amber_triggers

                self.amber_triggers = []
                self.logger.info("Starting processing of {} AMBER triggers".format(len(triggers)))
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

                # header should be present now
                if not self.hdr_mapping:
                    self.logger.error("First clusters received but header not found")
                    continue

                # remove headers from triggers (i.e. any trigger starting with #)
                triggers = [trigger for trigger in triggers if not trigger.startswith('#')]

                # triggers is empty if only header was received
                if not triggers:
                    self.logger.info("Only header received - Canceling processing")
                    continue

                # split strings and convert to numpy array
                try:
                    triggers = np.array(list(map(lambda val: val.split(), triggers)), dtype=float)
                except Exception as e:
                    self.logger.error("Failed to process triggers: {}".format(e))
                    continue

                # pick columns to feed to clustering algorithm
                triggers_for_clustering = triggers[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'],
                                                       self.hdr_mapping['time'], self.hdr_mapping['integration_step'])]
                triggers_for_clustering_sb = triggers[:, self.hdr_mapping['beam_id']].astype(int)
                self.logger.info("Clustering")
                cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _ = \
                    tools.get_triggers(triggers_for_clustering,
                                       tab=triggers[:, self.hdr_mapping['beam_id']],
                                       dm_min=self.dm_min, dm_max=self.dm_max, sig_thresh=self.snr_min,
                                       dt=dt, delta_nu_MHz=chan_width, nu_GHz=cent_freq, sb=triggers_for_clustering_sb)
                self.logger.info("Clustered {} raw triggers into {} clusters".format(len(triggers_for_clustering),
                                                                                     len(cluster_snr)))

                # Apply age threshold
                self.logger.info("Applying age threshold")
                age = (Time.now() - (TimeDelta(cluster_time, format='sec') + utc_start)).to(u.second).value
                mask = age <= self.age_max
                ncluster = int(np.sum(mask))
                if np.any(mask):
                    self.logger.info("Clusters after thresholding: {}. Putting clusters on queue".format(ncluster))
                    # put good clusters on queue
                    for i in range(ncluster):
                        # set window size to roughly two DM delays, and at least one page
                        window_size = max(1.024, cluster_dm[mask][i] * 2 / 1000.)
                        dada_trigger = {'stokes': 'I', 'dm': cluster_dm[mask][i], 'beam': cluster_sb[mask][i],
                                        'width': cluster_downsamp[mask][i], 'snr': cluster_snr[mask][i],
                                        'time': cluster_time[mask][i], 'utc_start': utc_start,
                                        'window_size': window_size, 'port': network_port}
                        self.cluster_queue.put({'command': 'trigger', 'trigger': dada_trigger})
                else:
                    self.logger.info("No clusters after thresholding")
            sleep(self.interval)
        self.logger.info("Observation finished")
