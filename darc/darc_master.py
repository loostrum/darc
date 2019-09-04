#!/usr/bin/env python
#
# DARC master process
# Controls all services

import sys
# ensure no bytecode is written, to make reload work
sys.dont_write_bytecode = True
import os
import ast
import yaml
import multiprocessing as mp
import threading
import socket
from time import sleep, time
from shutil import copy2
# reload is only built-in in python <=3.3
try:
    from importlib import reload
except ImportError:
    pass
from darc.definitions import MASTER, WORKERS, CONFIG_FILE
from darc import util
from darc.logger import get_logger
import darc.amber_listener
import darc.amber_triggering
import darc.amber_clustering
import darc.voevent_generator
import darc.status_website
import darc.offline_processing
import darc.dada_trigger
import darc.processor


class DARCMasterException(Exception):
    pass


class DARCMaster(object):
    def __init__(self):
        """
        Setup queues, config, logging
        """
        # setup stop event for master
        self.stop_event = threading.Event()

        # save host name
        self.hostname = socket.gethostname()

        # setup queues
        self.amber_listener_queue = mp.Queue()  # only used for start observation commands
        self.amber_trigger_queue = mp.Queue()  # for amber triggers
        self.dadatrigger_queue = mp.Queue()  # for dada triggers
        self.processor_queue = mp.Queue()  # for offline or real-time processing

        self.all_queues = [self.amber_listener_queue, self.amber_trigger_queue, self.dadatrigger_queue,
                           self.processor_queue]

        # Load config file
        self._load_config()

        # store hostname
        self.hostname = socket.gethostname()
        # store services
        if self.hostname == MASTER:
            if self.real_time:
                self.services = self.services_master_rt
            else:
                self.services = self.services_master_off
        elif self.hostname in WORKERS:
            if self.real_time:
                self.services = self.services_worker_rt
            else:
                self.services = self.services_worker_off
        else:
            self.services = []
        # service to class mapper
        self.service_mapping = {'voevent_generator': darc.voevent_generator.VOEventGenerator,
                                'status_website': darc.status_website.StatusWebsite,
                                'amber_listener': darc.amber_listener.AMBERListener,
                                'amber_triggering': darc.amber_triggering.AMBERTriggering,
                                'amber_clustering': darc.amber_clustering.AMBERClustering,
                                'dada_trigger': darc.dada_trigger.DADATrigger,
                                'processor': darc.processor.Processor,
                                'offline_processing': darc.offline_processing.OfflineProcessing}

        # create main log dir
        log_dir = os.path.dirname(self.log_file)
        try:
            util.makedirs(log_dir)
        except Exception as e:
            raise DARCMasterException("Cannot create log directory: {}".format(e))

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # Initialize services. Log dir must exist at this point
        self.threads = {}
        for service in self.services:
            _service_class = self.service_mapping[service]
            self.threads[service] = _service_class()

        # setup listening socket
        command_socket = None
        start = time()
        while not command_socket and time()-start < self.socket_timeout:
            try:
                command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                command_socket.bind(("", self.port))
            except socket.error as e:
                self.logger.warning("Failed to create socket, will retry: {}".format(e))
                command_socket = None
                sleep(1)

        if not command_socket:
            self.logger.error("Failed to ceate socket")
            raise DARCMasterException("Failed to setup command socket")

        self.command_socket = command_socket

        self.logger.info('DARC Master initialized')

    def _load_config(self):
        """
        Load configuration file
        """
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': self.hostname}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

    def run(self):
        """
        Listen for message on the command socket
        """

        # wait for commands
        self.command_socket.listen(5)
        self.logger.info("Waiting for commands")

        # main loop
        while not self.stop_event.is_set():
            try:
                client, adr = self.command_socket.accept()
            except Exception as e:
                self.logger.error('Caught exception while waiting for command: {}'.format(e))
                raise DARCMasterException('Caught exception while waiting for command: {}', e)

            raw_message = client.recv(1024).decode()
            self.logger.info("Received message: {}".format(raw_message))
            try:
                status, reply = self.parse_message(raw_message)
            except Exception as e:
                status = "Error"
                reply = "Caught exception: {}".format(e)

            # construct reply as dict
            if isinstance(reply, str):
                full_reply = "{{'status': \"{}\", 'message': \"{}\"}}".format(status, reply)
            else:
                full_reply = "{{'status': \"{}\", 'message': {}}}".format(status, reply)
            try:
                client.sendall(full_reply.encode())
            except socket.error as e:
                self.logger.error("Failed to send reply: {}".format(e))
            client.close()
        self.logger.info("Received stop. Exiting")
        # close any connection
        try:
            self.command_socket.close()
        except Exception as e:
            self.logger.warning("Failed to cleanly shutdown listening socket: {}".format(e))
        sys.exit()

    def parse_message(self, raw_message):
        """
        Parse raw received message
        :param raw_message: message as single string
        :return: status (str), reply (dict)
        """

        try:
            message = ast.literal_eval(raw_message)
        except Exception as e:
            self.logger.error("Failed to parse command message: {}".format(e))
            return "Error", {'error': "Cannot parse message"}

        try:
            command = message['command']
        except KeyError as e:
            self.logger.error("Command missing from command message: {}".format(e))
            return "Error", {'error': "No command specified"}

        service = message.get('service', None)
        payload = message.get('payload', None)
        status, reply = self.process_message(service, command, payload)

        return status, reply

    def process_message(self, service, command, payload):
        """
        :param service: service to interact with
        :param command: command to run
        :param payload: payload for command
        :return: status (str), reply (dict)
        """

        # First check for commands that do not require service argument
        # Start observation
        if command == 'start_observation':
            if not payload:
                self.logger.error('Payload is required when starting observation')
                status = 'Error'
                reply = {'error': 'Payload missing'}
            else:
                status, reply = self.start_observation(payload)
            return status, reply
        # Stop observation
        # only stop in real-time mode, as offline processing runs after the observation
        elif command == 'stop_observation':
            if self.real_time:
                status, reply = self.stop_observation()
            else:
                self.logger.info("Ignoring stop observation command in offline processing mode")
                status = 'Succes'
                reply = 'Ignoring stop in offline processing mode'
            return status, reply
        # Abort observation
        # always stop if aborted
        elif command == 'abort_observation':
            status, reply = self.stop_observation()
            return status, reply
        # Stop master
        elif command == 'stop_master':
            status, reply = self.stop()
            return status, reply
        # master config reload
        elif command == 'reload':
            self._load_config()
            return 'Success', ''

        # Service interaction
        if service == 'all':
            services = self.services
        else:
            if service not in self.services:
                self.logger.info("Invalid service for {}: {}".format(self.hostname, service))
                status = 'Error'
                reply = {'error': 'Invalid service: {}'.format(service)}
                return status, reply
            services = [service]

        status = 'Success'
        reply = {}
        for service in services:
            if command.lower() == 'start':
                _status, _reply = self.start_service(service)
            elif command.lower() == 'stop':
                _status, _reply = self.stop_service(service)
            elif command.lower() == 'restart':
                _status, _reply = self.restart_service(service)
            elif command.lower() == 'status':
                _status, _reply = self.check_status(service)
            else:
                self.logger.error('Received unknown command: {}'.format(command))
                status = 'Error'
                reply = {'error': 'Unknown command: {}'.format(command)}
                return status, reply
            if _status != 'Success':
                status = 'Error'
            reply[service] = _reply

        return status, reply

    def check_status(self, service):
        """
        :param service: Service to check status of
        :return: status, reply
        """

        status = 'Success'

        thread = self.threads[service]
        self.logger.info("Checking status of {}".format(service))
        if thread is None:
            # no thread means the service is not running
            self.logger.info("{} is stopped".format(service))
            reply = 'stopped'
        elif thread.isAlive():
            self.logger.info("{} is running".format(service))
            reply = 'running'
        else:
            self.logger.info("{} is stopped".format(service))
            reply = 'stopped'

        return status, reply

    def start_service(self, service):
        """
        :param service: service to start
        :return: status
        """

        # settings for specific services
        if service == 'amber_listener':
            source_queue = self.amber_listener_queue
            target_queue = self.amber_trigger_queue
        elif service == 'amber_triggering':
            source_queue = self.amber_trigger_queue
            target_queue = None
        elif service == 'amber_clustering':
            source_queue = self.amber_trigger_queue
            target_queue = self.dadatrigger_queue
        elif service == 'voevent_generator':
            source_queue = None
            target_queue = None
        elif service == 'status_website':
            source_queue = None
            target_queue = None
        elif service == 'offline_processing':
            source_queue = self.processor_queue
            target_queue = None
        elif service == 'dada_trigger':
            source_queue = self.dadatrigger_queue
            target_queue = None
        elif service == 'processor':
            source_queue = self.processor_queue
            target_queue = self.dadatrigger_queue
        else:
            self.logger.error('Unknown service: {}'.format(service))
            status = 'Error'
            reply = "Unknown service"
            return status, reply

        # get thread
        thread = self.threads[service]

        # check if a new thread has to be generated
        if thread is None:
            self.logger.info("Creating new thread for service {}".format(service))
            self.create_thread(service)
            thread = self.threads[service]

        # start the specified service
        self.logger.info("Starting service: {}".format(service))
        # check if already running
        if thread.isAlive():
            status = 'Success'
            reply = 'already running'
            self.logger.warning("Service already running: {}".format(service))
        else:
            # set queues
            if source_queue:
                thread.set_source_queue(source_queue)
            if target_queue:
                thread.set_target_queue(target_queue)
            # start
            thread.start()
            # check status
            if not thread.isAlive():
                status = 'Error'
                reply = "failed"
                self.logger.error("Failed to start service: {}".format(service))
            else:
                status = 'Success'
                reply = "started"
                self.logger.info("Started service: {}".format(service))

        return status, reply

    def stop_service(self, service):
        """
        :param service: service to stop
        :return: status, reply
        """

        # settings for specific services
        if service not in self.services:
            status = 'Error'
            reply = "Unknown service"
            self.logger.error("Unknown service: {}".format(service))
            return status, reply

        # get thread and event
        thread = self.threads[service]

        # check is it was running at all
        if thread is None or not thread.isAlive():
            self.logger.info("Service not running: {}".format(service))
            reply = 'Success'
            status = 'Stopped service'
            return status, reply

        # stop the specified service
        self.logger.info("Stopping service: {}".format(service))
        thread.stop()
        tstart = time()
        while thread.isAlive() and time()-tstart < self.stop_timeout:
            sleep(.1)
        if thread.isAlive():
            status = 'error'
            reply = "Failed to stop service before timeout"
            self.logger.error("Failed to stop service before timeout: {}".format(service))
        else:
            status = 'Success'
            reply = 'stopped'
            self.logger.info("Stopped service: {}".format(service))
        # remove thread
        self.threads[service] = None

        return status, reply

    def restart_service(self, service):
        """
        :param service: service to restart
        """
        status = 'Success'
        _status, reply_stop = self.stop_service(service)
        if _status != 'Success':
            status = _status
        _status, reply_start = self.start_service(service)
        if _status != 'Success':
            status = _status
        reply = {'stop': reply_stop, 'start': reply_start}
        return status, reply

    def create_thread(self, service):
        """
        :param service: service to create a new thread for
        """
        if service in self.service_mapping.keys():
            # Force reimport
            #self._reload(service)
            # Instantiate a new instance of the class
            self.threads[service] = self.service_mapping[service]()
        else:
            self.logger.error("Cannot create thread for {}".format(service))

    def stop(self):
        """
        Stop all services and exit
        """
        self.logger.info("Stopping all services")
        for service in self.services:
            self.stop_service(service)
        self.stop_event.set()
        status = 'Success'
        reply = "Stopping master"
        return status, reply

    def start_observation(self, config_file):
        """
        Start an observation
        :param config_file: Path to observation config file
        """

        self.logger.info("Starting observation with config file {}".format(config_file))
        # check if config file exists
        if not os.path.isfile(config_file):
            self.logger.error("File not found: {}".format(config_file))
            return "Error", "Failed: config file not found"
        # load config
        if config_file.endswith('.yaml'):
            config = self._load_yaml(config_file)
        elif config_file.endswith('.parset'):
            config = self._load_parset(config_file)
        else:
            self.logger.error("Failed to determine config file type from {}".format(config_file))
            return "Error", "Failed: unknown config file type"
        # check if process_triggers is enabled
        if not config['proctrigger']:
            self.logger.info("Process triggers is disabled; not starting observation")
            return "Success", "Process triggers disabled - not starting"

        # store the config for future reference
        config_output_dir = os.path.join(self.parset_dir, config['datetimesource'])
        try:
            util.makedirs(config_output_dir)
        except Exception as e:
            raise DARCMasterException("Cannot create config output directory: {}".format(e))
        try:
            copy2(config_file, config_output_dir)
        except Exception as e:
            self.logger.error("Could not store config file: {}".format(e))

        # initialize observation
        # Real-time procesing
        if self.real_time:
            if self.hostname == MASTER:
                self.logger.info("Nothing to start yet for real-time processing on master node")
            elif self.hostname in WORKERS:
                self.logger.info("Starting real-time processing on worker node")
                # clear all queues
                self.logger.info("Clearing queues")
                for queue in self.all_queues:
                    util.clear_queue(queue)
                # ensure all services are running
                self.logger.info("Making sure all services are running")
                for service in self.services:
                    self.start_service(service)
                # start observation by feeding obs config to all queues
                command = {'command': 'start_observation', 'obs_config': config}
                for queue in self.all_queues:
                    queue.put(command)
            else:
                self.logger.error("Running on unknown host: {}".format(self.hostname))
                return "Error", "Failed: running on unknown host"

            return "Success", "Observation started for real-time processing"
        # Offline processing
        else:
            # ensure services are running
            for service in self.services:
                self.start_service(service)
            # set host type and send config to offline processing
            command = {}
            if self.hostname == MASTER:
                command['host_type'] = 'master'
            elif self.hostname in WORKERS:
                command['host_type'] = 'worker'
            else:
                self.logger.error("Running on unknown host: {}".format(self.hostname))
                return "Error", "Failed: running on unknown host"

            command['obs_config'] = config
            # put commands on queue
            self.processor_queue.put(command)
            return "Success", "Observation started for offline processing"

    def stop_observation(self):
        """
        Stop an observation
        :return: status, reply message
        """
        # offline processing runs after an observation, so ignore stop
        # call stop_observation for all relevant services through their queues
        for queue in self.all_queues:
            queue.put({'command': 'stop_observation'})
        status = 'Success'
        reply = "Stopped observation"
        self.logger.info("Stopped observation")
        return status, reply

    def _load_yaml(self, config_file):
        """
        Load yaml file and convert to observation config
        :param config_file: Path to yaml file
        :return: observation config dict
        """
        self.logger.info("Loading yaml config {}".format(config_file))
        if not os.path.isfile(config_file):
            self.logger.error("Yaml file not found: {}".format(config_file))
            return {}

        with open(config_file) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        return config

    def _load_parset(self, config_file):
        """
        Load parset file and convert to observation config
        :param config_file: Path to parset file
        :return: observation config dict
        """
        self.logger.info("Loading parset {}".format(config_file))
        if not os.path.isfile(config_file):
            self.logger.error("Parset not found: {}".format(config_file))
            # not parset - do not process this observation
            return {'proctrigger': False}

        # Read raw parset
        with open(config_file) as f:
            parset = f.read().strip()
        # Convert to dict
        config = util.parse_parset(parset)
        return config

    def _reload(self, service):
        """
        Reimport service from .py file
        :param service: which service to reload
        :return:
        """
        if service == 'amber_listener':
            reload(darc.amber_listener)
            import darc.amber_listener
        elif service == 'amber_triggering':
            reload(darc.amber_triggering)
            import darc.amber_triggering
        elif service == 'amber_clustering':
            reload(darc.amber_clustering)
            import darc.amber_clustering
        elif service == 'voevent_generator':
            reload(darc.voevent_generator)
            import darc.voevent_generator
        elif service == 'status_website':
            reload(darc.status_website)
            import darc.status_website
        elif service == 'offline_processing':
            reload(darc.offline_processing)
            import darc.offline_processing
        elif service == 'dada_trigger':
            reload(darc.dada_trigger)
            import darc.dada_trigger
        elif service == 'processor':
            reload(darc.processor)
            import darc.processor
        else:
            self.logger.error("Unknown how to reimport class for {}".format(service))

        return


def main():
    master = DARCMaster()
    master.run()

