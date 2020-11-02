#!/usr/bin/env python3

import os
import socket
from argparse import Namespace
import threading
import multiprocessing as mp
from queue import Empty
from time import sleep
import yaml
import numpy as np
import astropy.units as u

from darc.definitions import CONFIG_FILE, BANDWIDTH, TSAMP, NCHAN
from darc.external import tools


class Clustering(threading.Thread):
    """
    Clustering and thresholding of AMBER triggers
    """

    def __init__(self, obs_config, output_dir, logger, input_queue, output_queue):
        """
        :param dict obs_config: Observation settings
        :param str output_dir: Output directory for data products
        :param Logger logger: Processor logger object
        :param Queue input_queue: Input queue for triggers
        :param Queue output_queue: Output queue for clusters
        """
        super(Clustering, self).__init__()
        self.logger = logger
        self.output_dir = output_dir
        self.obs_config = obs_config
        self.input_queue = input_queue
        self.output_queue = output_queue

        # set system parameters
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq'] * u.MHz + 0.5 * BANDWIDTH).to(u.GHz).value
        self.sys_params = {'dt': dt, 'delta_nu_MHz': chan_width, 'nu_GHz': cent_freq}

        # load config
        self.config = self._load_config()

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.output_file_handle = None
        self.ncluster = 0

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting clustering thread")
        # open the output file (line-buffered)
        self.output_file_handle = open(os.path.join(self.output_dir, self.config.output_file), 'w', buffering=1)
        # write header
        self.output_file_handle.write("#snr dm time downsamp sb\n")
        while not self.stop_event.is_set():
            # read triggers from input queue
            try:
                triggers = self.input_queue.get(timeout=.1)
            except Empty:
                self.input_empty = True
                continue
            else:
                self.input_empty = False
                # do clustering
                self._cluster(triggers)
        # close the output file
        self.output_file_handle.close()
        self.logger.info("Stopping clustering thread")

    def stop(self):
        """
        Stop this thread
        """
        # wait until the input queue is empty
        while not self.input_empty:
            sleep(1)
        # then stop
        self.stop_event.set()

    @staticmethod
    def _load_config():
        """
        Load configuration
        """
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['clustering']
        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.format(**kwargs)
            # replace any -1 by infinite
            elif value == -1:
                config[key] = np.inf
        # return as Namespace so the keys can be accessed as attributes
        return Namespace(**config)

    def _cluster(self, triggers):
        """
        Execute trigger clustering

        :param np.ndarray triggers: Input triggers, columns: DM, S/N, time,
                              integration_step, sb
        """
        # input columns are DM, SNR, time, integration step, SB
        # run clustering
        # ignored column is indices of kept events in original triggers
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _, ncand_per_cluster = \
            tools.get_triggers(triggers,
                               dm_min=self.config.dm_min, dm_max=self.config.dm_max,
                               sig_thresh=self.config.snr_min_clustering, t_window=self.config.clustering_window,
                               read_beam=True, return_clustcounts=True, sb_filter=self.config.sb_filter,
                               sb_filter_period_min=self.config.sb_filter_period_min,
                               sb_filter_period_max=self.config.sb_filter_period_max,
                               **self.sys_params)

        # apply S/N and width threshold to clusters
        mask = (np.array(cluster_downsamp) <= self.config.width_max) & (np.array(cluster_snr) >= self.config.snr_min)
        cluster_snr = np.array(cluster_snr)[mask]
        cluster_dm = np.array(cluster_dm)[mask]
        cluster_time = np.array(cluster_time)[mask]
        cluster_downsamp = np.array(cluster_downsamp)[mask].astype(int)
        cluster_sb = np.array(cluster_sb)[mask].astype(int)
        ncluster = len(cluster_snr)

        self.logger.info(f"Clustered {len(triggers)} triggers into {ncluster} clusters")
        self.ncluster += ncluster

        # put the clusters on the output queue for further analysis
        # note the for-loop is effectively skipped if ncluster is zero
        for ind in range(ncluster):
            self.output_queue.put([cluster_dm[ind], cluster_snr[ind], cluster_time[ind], cluster_downsamp[ind],
                                   cluster_sb[ind]])
            # write the cluster info to the output file (different order to remain compatible with old files)
            self.output_file_handle.write(f"{cluster_snr[ind]:.2f} {cluster_dm[ind]:.2f} {cluster_time[ind]:.3f} "
                                          f"{cluster_downsamp[ind]:.0f} {cluster_sb[ind]:.0f}\n")
        return
