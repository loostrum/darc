#!/usr/bin/env python3
#
# real-time data processor

import os
import threading
import multiprocessing as mp
from time import sleep
import numpy as np

from darc import DARCBase
from darc.processor_tools import Clustering, Extractor, Classifier, Visualizer
from darc import util


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
        self.scavenger = threading.Thread(target=self.thread_scavenger, name='scavenger')
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
        Upon stop of the manager, abort any remaining observations
        """
        # loop over dictionary items. Use copy to avoid changing dict in loop
        for taskid, obs in self.observations.copy().items():
            self.logger.info(f"Aborting observation with taskid {taskid}")
            obs.stop_observation(abort=True)
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
        proc.name = taskid
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

        :param dict obs_config: Observation config
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

    #. Clustering + thresholding
    #. Extract data from filterbank
    #. Run classifier

    After observation finishes, results are sent to the master node
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
        self.output_dir = None

        # create queues
        self.clustering_queue = mp.Queue()
        self.extractor_queue = mp.Queue()
        self.classifier_queue = mp.Queue()
        self.all_queues = (self.clustering_queue, self.extractor_queue, self.classifier_queue)

        # lock for accessing AMBER trigger list
        self.lock = threading.Lock()

        # load config
        self.config = self.load_config()

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: Command to process
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                with self.lock:
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

        # add observation-specific path to result_dir
        self.central_result_dir = os.path.join(self.result_dir, obs_config['date'], obs_config['datetimesource'])

        # create output dir
        output_dir = os.path.join('{output_dir}'.format(**obs_config), self.output_dir)

        for path in (output_dir, self.central_result_dir):
            try:
                util.makedirs(path)
            except Exception as e:
                self.logger.error(f"Failed to create directory {path}: {e}")
                raise ProcessorException(f"Failed to create directory {path}: {e}")

        self.output_dir = output_dir

        self.observation_running = True  # this must be set before starting the processing thread

        # start processing
        thread = threading.Thread(target=self._read_and_process_data, name='processing')
        thread.daemon = True
        thread.start()
        self.threads['processing'] = thread

        # start clustering
        thread = Clustering(obs_config, output_dir, self.logger, self.clustering_queue, self.extractor_queue)
        thread.name = 'clustering'
        thread.daemon = True
        thread.start()
        self.threads['clustering'] = thread

        # start extractor(s)
        for i in range(self.num_extractor):
            thread = Extractor(obs_config, output_dir, self.logger, self.extractor_queue, self.classifier_queue)
            thread.name = f'extractor_{i}'
            thread.daemon = True
            thread.start()
            self.threads[f'extractor_{i}'] = thread

        # start classifier
        thread = Classifier(self.logger, self.classifier_queue)
        thread.name = 'classifier'
        thread.daemon = True
        thread.start()
        self.threads['classifier'] = thread

        self.logger.info("Observation started")

    def stop_observation(self, abort=False):
        """
        Stop observation

        :param bool abort: Whether or not to abort the observation
        """
        # set running to false
        self.observation_running = False
        # if abort, clear all queues
        if abort:
            for queue in self.all_queues:
                util.clear_queue(queue)
        # clear processing thread
        try:
            self.threads['processing'].join()
        except KeyError:
            # there was no processing thread
            pass
        # signal clustering to stop
        try:
            self.threads['clustering'].stop()
            self.threads['clustering'].join()
        except KeyError:
            pass
        # signal extractor(s) to stop
        for i in range(self.num_extractor):
            try:
                self.threads[f'extractor_{i}'].stop()
                self.threads[f'extractor_{i}'].join()
            except KeyError:
                pass
        # signal classifier to stop
        try:
            self.threads['classifier'].stop()
            self.threads['classifier'].join()
        except KeyError:
            pass

        # now fire up the visualization
        if not abort:
            self.logger.debug(self.threads['classifier'].candidates_to_visualize)
            Visualizer(self.output_dir, self.central_result_dir, self.logger, self.obs_config,
                       self.threads['classifier'].candidates_to_visualize)
        self.logger.info(f"Observation finished: {self.obs_config['parset']['task.taskID']}: "
                         f"{self.obs_config['datetimesource']}")

    def _read_and_process_data(self):
        """
        Process incoming AMBER triggers
        """
        # main loop
        while self.observation_running and not self.stop_event.is_set():
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                with self.lock:
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
