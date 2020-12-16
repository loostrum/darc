#!/usr/bin/env python3
#
# real-time data processor

import os
import threading
import multiprocessing as mp
from time import sleep
import numpy as np
import yaml
import h5py

from darc import DARCBase
from darc.processor_tools import Clustering, Extractor, Classifier, Visualizer
from darc import util
from darc.logger import get_queue_logger, get_queue_logger_listener


class ProcessorException(Exception):
    pass


class ProcessorManager(DARCBase):
    """
    Control logic for running several Processor instances, one per observation
    """

    def __init__(self, *args, **kwargs):
        """
        """
        # init DARCBase without logger, as we need a non-default logger
        super(ProcessorManager, self).__init__(*args, no_logger=True, **kwargs)

        # initialize queue logger listener
        self.log_queue = mp.Queue()
        self.log_listener = get_queue_logger_listener(self.log_queue, self.log_file)
        self.log_listener.start()

        # create queue logger
        self.logger = get_queue_logger(self.module_name, self.log_queue)

        self.observations = {}
        self.observation_queues = {}
        self.current_observation_queue = None
        self.scavenger = None

        self.logger.info("{} initialized".format(self.log_name))

    def run(self):
        """
        Main loop. Create thread scavenger, then run parent class run method
        """
        # create a thread scavenger
        self.scavenger = threading.Thread(target=self.thread_scavenger, name='scavenger')
        self.scavenger.start()
        super(ProcessorManager, self).run()

    def thread_scavenger(self):
        """
        Remove any finished threads at regular intervals
        """
        while not self.stop_event.is_set():
            for taskid, thread in self.observations.copy().items():
                if not thread.is_alive():
                    # if the thread is dead, remove it from the list
                    self.logger.info(f"Scavenging thread of taskid {taskid}")
                    self.observations.pop(taskid)
                    self.observation_queues.pop(taskid)
            self.stop_event.wait(self.scavenger_interval)

    def cleanup(self):
        """
        Upon stop of the manager, abort any remaining observations
        """
        # loop over dictionary items. Use copy to avoid changing dict in loop
        for taskid, obs in self.observations.copy().items():
            if obs.is_alive():
                self.logger.info(f"Aborting observation with taskid {taskid}")
                self.observation_queues[taskid].put('abort')
            obs.join()
        # stop the log listener
        self.log_listener.stop()
        self.log_listener.join()

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
        queue = mp.Queue()
        proc = Processor(source_queue=queue, log_queue=self.log_queue, config_file=self.config_file)
        proc.name = taskid
        proc.start()
        # start the observation and store thread
        queue.put({'command': 'start_observation', 'obs_config': obs_config, 'reload': reload})
        self.observations[taskid] = proc
        self.observation_queues[taskid] = queue
        self.current_observation_queue = queue
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
            self.logger.error(f"Failed to stop observation: no such task ID {taskid}")
            return

        # signal the processor of this observation to stop the observation
        # when processing is finished, this also stops the Process
        self.observation_queues[taskid].put({'command': 'stop_observation'})

    def process_command(self, command):
        """
        Forward any data from the input queue to the running observation
        """
        if command['command'] == 'stop':
            self.stop()
        elif command['command'] == 'get_attr':
            self.get_attribute(command)
        elif self.current_observation_queue is not None:
            self.current_observation_queue.put(command)
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
    #. Visualize candidates

    After observation finishes, results are gathered in a central location to be picked up by the master node
    """

    def __init__(self, log_queue, *args, **kwargs):
        """
        :param Queue log_queue:
        """
        # init DARCBase without logger, as we need a non-default logger
        super(Processor, self).__init__(*args, no_logger=True, **kwargs)

        # create queue logger
        self.logger = get_queue_logger(self.module_name, log_queue)
        self.log_queue = log_queue

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

        # lock for accessing AMBER trigger list and obs stats
        self.amber_lock = threading.Lock()
        self.obs_stats_lock = threading.Lock()

        # initalize observation statistics.
        self.obs_stats = {'ncand_raw': 0,
                          'ncand_post_clustering': 0,
                          'ncand_post_thresholds': 0,
                          'ncand_post_classifier': 0}

        self.ncluster = mp.Value('i', 0)
        self.ncand_above_threshold = mp.Value('i', 0)

        self.candidates_to_visualize = []
        self.classifier_parent_conn, self.classifier_child_conn = mp.Pipe()

        self.logger.info("{} initialized".format(self.log_name))

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: Command to process
        """
        if command['command'] == 'get_attr':
            self.get_attribute(command)
        elif command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                with self.amber_lock:
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
        output_dir = os.path.join('{output_dir}'.format(**obs_config), self.output_subdir)

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
        self.threads['processing'] = thread

        # start clustering
        thread = Clustering(obs_config, output_dir, self.log_queue, self.clustering_queue, self.extractor_queue,
                            self.ncluster, self.config_file)
        thread.name = 'clustering'
        self.threads['clustering'] = thread

        # start extractor(s)
        for i in range(self.num_extractor):
            thread = Extractor(obs_config, output_dir, self.log_queue, self.extractor_queue, self.classifier_queue,
                               self.ncand_above_threshold, self.config_file)
            thread.name = f'extractor_{i}'
            self.threads[f'extractor_{i}'] = thread

        # start classifier
        thread = Classifier(self.log_queue, self.classifier_queue, self.classifier_child_conn, self.config_file)
        thread.name = 'classifier'
        self.threads['classifier'] = thread

        # start all threads/processes
        for thread in self.threads.values():
            thread.start()

        self.logger.info("Observation started")

    def stop_observation(self, abort=False):
        """
        Stop observation

        :param bool abort: Whether or not to abort the observation
        """
        if (not self.observation_running) and (not abort):
            # nothing to do
            return

        if abort:
            self.logger.info("Aborting observation")
        else:
            self.logger.info("Finishing observation")
            # wait for a short time in case some last AMBER triggers are still coming in
            sleep(self.stop_delay)
        # set running to false
        self.observation_running = False
        # if abort, clear all queues and terminate processing
        if abort:
            for queue in self.all_queues:
                util.clear_queue(queue)
            # processing is a thread, cannot terminate but it should stop very quickly when running is set to False
            self.threads['processing'].join()
            self.threads['clustering'].terminate()
            for i in range(self.num_extractor):
                self.threads[f'extractor_{i}'].terminate()
            self.threads['classifier'].terminate()
            self.logger.info(f"Observation aborted: {self.obs_config['parset']['task.taskID']}: "
                             f"{self.obs_config['datetimesource']}")
            # A stop observation should also stop this processor, as there is only one per observation
            self.stop_event.set()
            return

        # clear processing thread
        self.threads['processing'].join()
        # signal clustering to stop
        self.clustering_queue.put('stop')
        self.threads['clustering'].join()
        # signal extractor(s) to stop
        for i in range(self.num_extractor):
            self.extractor_queue.put(f'stop_extractor_{i}')
            self.threads[f'extractor_{i}'].join()
        # signal classifier to stop
        self.classifier_queue.put('stop')
        # read the output of the classifier
        self.candidates_to_visualize = self.classifier_parent_conn.recv()
        self.threads['classifier'].join()

        # store obs statistics
        # if no AMBER header was received, something failed and there are no candidates
        # set all values to -1 to indicate this
        if not self.hdr_mapping:
            for key in self.obs_stats.keys():
                self.obs_stats[key] = -1
        else:
            # already have number of raw candidates
            # store number of post-clustering candidates
            self.obs_stats['ncand_post_clustering'] = self.ncluster.value
            # store number of candidates above local S/N threshold
            self.obs_stats['ncand_post_thresholds'] = self.ncand_above_threshold.value
            # store number of candidates post-classifier
            self.obs_stats['ncand_post_classifier'] = len(self.candidates_to_visualize)

        # Store the statistics and start the visualization
        if len(self.candidates_to_visualize) > 0:
            Visualizer(self.output_dir, self.central_result_dir, self.log_queue, self.obs_config,
                       self.candidates_to_visualize, self.config_file)
        else:
            self.logger.info(f"No post-classifier candidates found, skipping visualization for taskid "
                             f"{self.obs_config['parset']['task.taskID']}")
        # Store statistics after visualization, as master will start combining results once all stats are present
        self._store_obs_stats()

        self.logger.info(f"Observation finished: {self.obs_config['parset']['task.taskID']}: "
                         f"{self.obs_config['datetimesource']}")
        # stop this processor
        self.stop_event.set()

    def _read_and_process_data(self):
        """
        Process incoming AMBER triggers
        """
        # main loop
        while self.observation_running and not self.stop_event.is_set():
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                with self.amber_lock:
                    triggers = self.amber_triggers
                    self.amber_triggers = []
                # update number of raw candidates
                with self.obs_stats_lock:
                    self.obs_stats['ncand_raw'] += len(triggers)
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
                self.clustering_queue.put(triggers_for_clustering)
            sleep(self.interval)

    def _store_obs_stats(self):
        """
        Store observation statistics to central result directory
        """

        # overview statistics
        info_file = os.path.join(self.central_result_dir, f'CB{self.obs_config["beam"]:02d}_summary.yaml')
        self.logger.debug(f"Storing observation statistics to {info_file}")
        with open(info_file, 'w') as f:
            yaml.dump(self.obs_stats, f, default_flow_style=False)

        # list of triggers
        trigger_file = os.path.join(self.central_result_dir, f'CB{self.obs_config["beam"]:02d}_triggers.txt')
        self.logger.debug(f"Storing trigger metadata to {trigger_file}")
        with open(trigger_file, 'w') as f:
            f.write('#cb snr dm time downsamp sb p\n')
            for fname in self.candidates_to_visualize:
                with h5py.File(fname, 'r') as h5:
                    line = "{beam:02d} {snr:.2f} {dm:.2f} {toa:.4f} " \
                           "{downsamp:.0f} {sb:.0f} " \
                           "{prob_freqtime:.2f}\n".format(beam=self.obs_config['beam'], **h5.attrs)
                f.write(line)
