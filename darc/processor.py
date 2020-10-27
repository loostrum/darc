#!/usr/bin/env python3
#
# real-time data processor

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

from darc import DARCBase
from darc import util
from darc.definitions import CONFIG_FILE, BANDWIDTH, TSAMP, NCHAN
from darc.external import tools


class ProcessorException(Exception):
    pass


class ProcessorManager(DARCBase):
    """
    Control logic for running several Processor instances, one per observation
    """

    def __init__(self):
        """
        """
        super(ProcessorManager, self).__init__()

        self.observations = {}
        self.current_observation = None

        # create a thread scavenger
        self.scavenger = threading.Thread(target=self.thread_scavenger)
        self.scavenger.start()

    def thread_scavenger(self):
        """
        Remove any finished threads at regular intervals
        """
        while not self.stop_event.is_set():
            for taskid, thread in self.observations.copy().items():
                if not thread.is_alive():
                    # if the thread is dead, remove it from the list
                    self.observations.pop(taskid)
                    self.logger.info(f"Scavenging thread of taskid {taskid}")

    def cleanup(self):
        """
        Upon stop of the manager, stop any remaining observations
        """
        # loop over dictionary items. Use copy to avoid changing dict in loop
        for taskid, obs in self.observations.copy().items():
            self.logger.info(f"Stopping observation with taskid {taskid}")
            obs.stop_observation()
            obs.join()

    def start_observation(self, obs_config, reload=True):
        """
        Initialize a Processor and call its start_observation
        """
        if reload:
            self.load_config()

        # add parset to obs config
        obs_config['parset'] = self._load_parset(obs_config)
        # get task ID
        taskid = obs_config['parset']['task.taskID']

        self.logger.info(f"Starting observation with task ID {taskid}")

        # refuse to do anything if an observation with this task ID already exists
        if taskid in self.observations.keys():
            self.logger.error(f"Failed to start observation: task ID {taskid} already exists")
            return

        # initialize a Processor for this observation
        proc = Processor()
        # create a source queue
        proc.set_source_queue(mp.Queue())
        proc.start()
        # start the observation and store thread
        proc.start_observation(obs_config, reload)
        self.observations[taskid] = proc
        self.current_observation = proc
        return

    def stop_observation(self, obs_config):
        """
        Stop observation with task ID as given in parset
        """
        # load the parset
        parset = self._load_parset(obs_config)
        # get task ID
        taskid = parset['task.taskID']
        # check if an observation with this task ID exists
        if taskid not in self.observations.keys():
            self.logger.error("Failed to stop observation: no such task ID {taskid}")

        # signal the processor of this observation to stop
        # this also calls its stop_observation method
        self.observations[taskid].stop()

    def process_command(self, command):
        """
        Forward any data from the input queue to the running observation
        """
        if self.current_observation is not None:
            self.current_observation.source_queue.put(command)
        else:
            self.logger.error("Data received but no observation is running - ignoring")
        return

    def _load_parset(self, obs_config):
        """
        Load the observation parset

        :param dict obs_config: Observation config
        :return: parset as dict
        """
        try:
            # encoded parset is already in config on master node
            # decode the parset
            raw_parset = util.decode_parset(obs_config['parset'])
            # convert to dict and store
            parset = util.parse_parset(raw_parset)
        except KeyError:
            self.logger.info("Observation parset not found in input config, looking for master parset")
            # Load the parset from the master parset file
            master_config_file = os.path.join(obs_config['master_dir'], 'parset', 'darc_master.parset')
            try:
                # Read raw config
                with open(master_config_file) as f:
                    master_config = f.read().strip()
                # Convert to dict
                master_config = util.parse_parset(master_config)
                # extract obs parset and decode
                raw_parset = util.decode_parset(master_config['parset'])
                parset = util.parse_parset(raw_parset)
            except Exception as e:
                self.logger.warning(
                    "Failed to load parset from master config file {}, "
                    "setting parset to None: {}".format(master_config_file, e))
                parset = None

        return parset


class Processor(DARCBase):
    """
    Real-time processing of candidates

    1. Clustering + thresholding
    2. Extract data from filterbank

    After observation end:
    3. Run classifier
    4. Send results to master node
    """

    def __init__(self):
        """
        """
        super(Processor, self).__init__()
        self.observation_running = False
        self.threads = {}
        self.amber_triggers = []
        self.hdr_mapping = {}
        self.obs_config = None

        # create queues
        self.clustering_queue = mp.Queue()
        self.extractor_queue = mp.Queue()

        # intialise analysis tools
        # self.rtproc = realtime_tools.RealtimeProc()
        # self.model_freqtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_freqtime))
        # self.model_dmtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_dmime))

        # For some reason, the model's first prediction takes a long time.
        # pre-empt this by classifying an array of zeros before looking at real data
        # self.model_freqtime.predict(np.zeros([1, self.nfreq, self.ntime, 1]))
        # self.model_dmtime.predict(np.zeros([1, self.ndm, self.ntime, 1]))

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: Command to process
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                self.amber_triggers.append(command['trigger'])
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def start_observation(self, obs_config, reload=True):
        """
        Parse obs config and start listening for amber triggers on queue

        :param dict obs_config: Observation configuration
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

        # clean any old triggers
        self.amber_triggers = []
        # set config
        self.obs_config = obs_config

        self.observation_running = True

        # start processing
        self.threads['processing'] = threading.Thread(target=self._read_and_process_data, name='processing')
        self.threads['processing'].daemon = True
        self.threads['processing'].start()

        # start clustering
        self.threads['clustering'] = Clustering(obs_config, self.logger, self.clustering_queue, self.extractor_queue)
        self.threads['clustering'].daemon = True
        self.threads['clustering'].start()

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
        # clear processing thread
        if self.threads['processing'] is not None:
            self.threads['processing'].join()
        # signal clustering to stop
        self.threads['clustering'].stop()
        self.threads['clustering'].join()
        self.threads = {}

    def _read_and_process_data(self):
        """
        Process incoming AMBER triggers
        """
        # main loop
        while self.observation_running and not self.stop_event.is_set():
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                triggers = self.amber_triggers

                self.amber_triggers = []
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
                                                       self.hdr_mapping['time'], self.hdr_mapping['integration_step'],
                                                       self.hdr_mapping['beam_id'])]

                # put triggers on clustering queue
                self.threads['clustering'].input_queue.put(triggers_for_clustering)
            sleep(self.interval)


class Clustering(threading.Thread):
    """
    Clustering and thresholding of AMBER triggers
    """

    def __init__(self, obs_config, logger, input_queue, output_queue):
        """
        :param dict obs_config: Observation settings
        :param Logger logger: Processor logger object
        :param Queue input_queue: Input queue for triggers
        :param Queue output_queue: Output queue for clusters
        """
        super(Clustering, self).__init__()
        self.logger = logger
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

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting Clustering thread")
        while not self.stop_event.is_set():
            # read triggers from input queue
            try:
                triggers = self.input_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                # do clustering
                self._cluster(triggers)
        self.logger.info("Stopping clustering thread")

    def stop(self):
        """
        Stop this thread
        """
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
                              integration_step, beam
        """
        # input columns are DM, SNR, time, integration step, beam
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
        mask = (np.array(cluster_downsamp) <= self.config.width_max) | (np.array(cluster_snr) >= self.config.snr_min)
        cluster_snr = np.array(cluster_snr)[mask]
        cluster_dm = np.array(cluster_dm)[mask]
        cluster_time = np.array(cluster_time)[mask]
        cluster_downsamp = np.array(cluster_downsamp)[mask].astype(int)
        cluster_sb = np.array(cluster_sb)[mask].astype(int)
        ncluster = len(cluster_snr)

        self.logger.info(f"Clustered {len(triggers)} triggers into {ncluster} clusters")

        # put the clusters on the output queue for further analysis
        # note the for-loop is effectively skipped if ncluster is zero
        for ind in range(ncluster):
            self.output_queue.put([cluster_dm[ind], cluster_snr[ind], cluster_time[ind], cluster_downsamp[ind],
                                   cluster_sb[ind]])
        return
