#!/usr/bin/env python3
#
# real-time data processor

import os
import threading
import multiprocessing as mp
from queue import Empty
from time import sleep

import numpy as np

from darc import util
from darc.base import DARCBase


class ProcessorException(Exception):
    pass


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
        # parse parset
        obs_config['parset'] = self._load_parset(obs_config)
        # set config
        self.obs_config = obs_config

        self.observation_running = True

        # start processing
        self.threads['processing'] = threading.Thread(target=self._read_and_process_data, name='processing')
        self.threads['processing'].daemon = True
        self.threads['processing'].start()

        # start clustering
        self.threads['clustering'] = Clustering(obs_config, self.logger)
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
                    "Failed to load parset from master config file {}, setting parset to None: {}".format(master_config_file, e))
                parset = None

        return parset


class Clustering(threading.Thread):
    """
    Clustering and thresholding of AMBER triggers
    """

    def __init__(self, obs_config, logger):
        """
        :param dict obs_config: Observation settings
        :param Logger logger: Processor logger object
        """
        super(Clustering, self).__init__()
        self.logger = logger
        self.obs_config = obs_config

        # create a queue for input triggers
        self.input_queue = mp.Queue()
        # create stop event
        self.stop_event = mp.Event()

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting Clustering")
        while not self.stop_event.is_set():
            # read triggers from input queue
            try:
                triggers = self.input_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                # do clustering
                self._cluster(triggers)
        self.logger.info("Stopping clustering process")

    def stop(self):
        """
        Finish clustering
        """
        self.stop_event.set()

    def _cluster(self, triggers):
        """
        Execute trigger clustering

        :param list triggers: Input triggers
        """
        self.logger.info(f"Clustering {len(triggers)} triggers")
        # input columns are DM, SNR, time, integration step, beam
