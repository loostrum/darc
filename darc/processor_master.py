#!usr/bin/env python3

import os
import socket
import threading
import yaml
from astropy.time import Time

from darc import DARCBase
from darc import util
from darc.definitions import CONFIG_FILE, WORKERS


class ProcessorMasterManager(DARCBase):
    """
    Control logic for running several ProcessorMaster instances, one per observation
    """

    def __init__(self):
        """
        """
        super(ProcessorMasterManager, self).__init__()

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
        Initialize a ProcessorMaster and call its start_observation
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
        proc = ProcessorMaster()
        proc.name = taskid
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

    # only start and stop observation commands exist
    def process_command(self, command):
        pass

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


class ProcessorMaster(DARCBase):
    """
    Combine results from worker node processors
    """
    def __init__(self):
        super(ProcessorMaster, self).__init__()

        self.needs_source_queue = False

        # read result dir from worker processor config
        self.result_dir = self._get_result_dir()

        self.obs_config = None
        self.warnings_sent = []
        self.status = None

    def start_observation(self, obs_config, reload=True):
        """
        Parse obs config and start listening for amber triggers on queue

        :param dict obs_config: Observation configuration
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

        # add observation-specific path to result_dir
        self.central_result_dir = os.path.join(self.result_dir, obs_config['date'], obs_config['datetimesource'])

        self.obs_config = obs_config

        # wait until the observation finishes
        start_processing_time = Time(obs_config['parset']['task.stopTime'])
        self.logger.info("Sleeping until {}".format(start_processing_time.iso))
        self.status = 'Observation in progress'
        util.sleepuntil_utc(start_processing_time, event=self.stop_event)

        # wait for all result files to be present
        self.status = 'Waiting for nodes to finish processing'
        self._wait_for_workers()

        # combine results, copy to website and generate email
        self.status = 'Combining node results'
        email, attachments = self._process_results()
        self._send_email(email, attachments)
        self.status = 'Done'

    def stop_observation(self, abort=False):
        """
        Stop observation

        :param bool abort: Whether or not to abort the observation
        """
        # nothing to stop unless we are aborting
        if not abort:
            return

    @staticmethod
    def _get_result_dir():
        """
        Get result directory from worker processor config
        """
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        return config['result_dir'].format(**kwargs)

    def _wait_for_workers(self):
        """
        Wait for all worker nodes to finish processing this observation
        """
        twait = 0
        for beam in self.obs_config['beams']:
            result_file = os.path.join(self.central_result_dir, f'CB{beam:02d}.yaml')
            # wait until the result file is present
            while not os.path.isfile(result_file):
                # wait until the next check time
                self.stop_event.wait(self.check_interval)
                twait += self.check_interval
                # if we waited a long time, check if a warning should be sent if the node is offline
                node = WORKERS[beam]
                if (twait > self.max_wait_time) and (node not in self.warnings_sent) and \
                        (not self._check_node_online(node)):
                    # node is not in warnings and offline, send a warning
                    self._send_warning(node)
                    # store that we sent a warning
                    self.warnings_sent.append(node)

    def _check_node_online(self, node):
        """
        Check if the processor on a node is still online

        :param str node: Hostname of node to check
        :return: status (bool): True if node is online, else False
        """
        self.logger.warning("Node status check not yet implemented, returning True")
        return True

    def _send_warning(self, node):
        """
        Send a warning email about a node
        """
        self.logger.warning("Warning email not yet implemented")

    def _process_results(self):
        """
        Load statistics and plots from the nodes. Copy to website directory and return data to be sent as email

        :return: email (str), attachments (list)
        """
        self.logger.warning("Result processing not yet implemented, returning dummy email/attachments")
        email = "TEST EMAIL"
        attachment = {'path': os.path.join(self.central_result_dir, 'CB00.pdf'),
                      'name': 'CB00.pdf',
                      'type': 'pdf'}
        return email, [attachment]

    def _send_email(self, email, attachments):
        """
        Send email with observation results

        :param str email: Email body
        :param list attachments: Attachments
        """

        subject = f"ARTS FRB Alert System - {self.obs_config['datetimesource']}"

        # set other email settings
        frm = "ARTS FRB Alert System <arts@{}.apertif>".format(socket.gethostname())
        to = self.email_settings['to']
        body = {'type': 'html', 'content': email}

        util.send_email(frm, to, subject, body, attachments)
