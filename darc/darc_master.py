#!/usr/bin/env python
#
# DARC master process
# Controls all services

import sys
import ast
import yaml
import errno
import multiprocessing as mp
import threading
import socket
from time import sleep, time

from darc.definitions import *
from darc.logger import get_logger
from darc.amber_listener import AMBERListener
from darc.amber_triggering import AMBERTriggering
from darc.voevent_generator import VOEventGenerator
from darc.status_website import StatusWebsite
from darc.offline_processing import OfflineProcessing


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
        self.amber_listener_queue = mp.Queue()
        self.voevent_queue = mp.Queue()
        self.processing_queue = mp.Queue()

        # Load config file
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': self.hostname}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # store hostname
        self.hostname = socket.gethostname()
        # store services
        if self.hostname == MASTER:
            self.services = self.services_master
        elif self.hostname in WORKERS:
            self.services = self.services_worker
        else:
            self.services = []
        # service to class mapper
        self.service_mapping = {'voevent_generator': VOEventGenerator,
                                'status_website': StatusWebsite,
                                'amber_listener': AMBERListener,
                                'amber_triggering': AMBERTriggering,
                                'offline_processing': OfflineProcessing}

        # create main log dir
        log_dir = os.path.dirname(self.log_file)
        try:
            os.makedirs(log_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                self.logger.error("Cannot create log directory: {}".format(e))
                raise DARCMasterException("Cannot create log directory")

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # Initalize services. Log dir must exist at this point
        self.events = {}
        self.threads = {}
        for service in self.services:
            _event = threading.Event()
            _service_class = self.service_mapping[service]
            self.events[service] = _event
            self.threads[service] = _service_class(_event)

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
                self.logger.error('Caught exception while waiting for command: {}', e)
                raise DARCMasterException('Caught exception while waiting for command: {}', e)

            raw_message = client.recv(1024).decode()
            self.logger.info("Received message: {}".format(raw_message))
            try:
                status, reply = self.parse_message(raw_message.decode())
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
            self.command_socket.shutdown()
            self.command_socket.close()
        except Exception:
            pass
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
        # Stop master
        elif command == 'stop_master':
            status, reply = self.stop()
            return status, reply

        # Service interaction
        if service == 'all':
            services = self.services
        else:
            if not service in self.services:
                self.logger.info("Invalid service for {}: {}".format(self.hostname, service))
                status = 'Error'
                reply =  {'error': 'Invalid service: {}'.format(service)}
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
                _status, _reply =  self.check_status(service)
            else:
                self.logger.error('Received unknown command: {}'.format(command))
                status  = 'Error'
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
            status = 'Error'
            reply = 'No thread found'
            return status, reply

        if thread.isAlive():
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
            source_queue = None
            target_queue = self.amber_listener_queue
        elif service == 'amber_triggering':
            source_queue = self.amber_listener_queue
            target_queue = self.voevent_queue
        elif service == 'voevent_generator':
            source_queue = self.voevent_queue
            target_queue = None
        elif service == 'status_website':
            source_queue = None
            target_queue = None
        elif service == 'offline_processing':
            source_queue = self.processing_queue
            target_queue = None
        else:
            self.logger.error('Unknown service: {}'.format(service))
            status = 'Error'
            reply = "Unknown service"
            return status, reply

        # get thread and event
        thread = self.threads[service]
        event = self.events[service]

        # set event to allow running
        event.clear()

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
            reply = "Service already running: {}".format(service)
            self.logger.warning(status)
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
                reply = "Failed to start service"
                self.logger.error("Failed to start service: {}".format(service))
            else:
                status = 'Success'
                reply = "Started service"
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
        event = self.events[service]

        # stop the specified service
        self.logger.info("Stopping service: {}".format(service))

        if not thread.isAlive():
            status = 'Success'
            reply = "Stopped service"
            # this thread is done, create a new thread
            self.create_thread(service)
            self.logger.warning("Already stopped service: {}".format(service))
        else:
            event.set()
            tstart = time()
            while thread.isAlive() and time()-tstart < self.stop_timeout:
                sleep(.1)
            if thread.isAlive():
                status = 'error'
                reply = "Failed to stop service before timeout"
                self.logger.error("Failed to stop service before timeout: {}".format(service))
            else:
                status = 'Success'
                reply = 'Stopped service'
                # this thread is done, create a new thread
                self.create_thread(service)
                self.logger.info("Stopped service: {}".format(service))

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
            module = self.service_mapping[service]
            # Force reimport
            self._reload(module)
            # Instantiate a new instance of the class
            self.threads[service] = module(self.events[service])
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
        reply = "Master stop event set"
        return status, reply

    def start_observation(self, config_file):
        """
        Start an observation
        :param config: Path to observation config file
        """

        self.logger.info("Starting observation with config file {}".format(config_file))
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
        # start the observation
        # for now use startpacket as unique ID. Should move to task ID when available

        # check if offline processing is running. If not, start it
        _, offline_processing_status = self.check_status('offline_processing')
        if not offline_processing_status == 'running':
            self.logger.info('offline_processing not running - starting')
            self.start_service('offline_processing')
        
        # initialize observation
        command = {}
        if self.hostname == MASTER:
            command['host_type'] = 'master'
        elif self.hostname in WORKERS:
            command['host_type'] = 'worker'
        else:
            self.logger.error("Running on unknown host: {}".format(self.hostname))
            return "Error", "Failed: running on unknown host"

        command['obs_config'] = config
        self.processing_queue.put(command)
        return "Success", "Observation started for offline processing"

    def _load_yaml(self, config_file):
        """
        Load yaml file and convert to observation config
        :param config_file: Path to yaml file
        :return: observation config dict
        """
        self.logger.info("Loading yaml config")
        with open(config_file) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        return config

    def _load_parset(self, config_file):
        """
        Load yaml file and convert to observation config
        :param config_file: Path to parset file
        :return: observation config dict
        """
        self.logger.error("Loading parset config not implemented yet!")
        return None

    def _reload(self, service):
        """
        Reimport class of a service
        :param service: which service to reload class of
        :return:
        """
        if service == 'amber_listener':
            del AMBERListener
            from darc.amber_listener import AMBERListener
        elif service == 'amber_triggering':
            del AMBERTriggering
            from darc.amber_triggering import AMBERTriggering
        elif service == 'voevent_generator':
            del VOEventGenerator
            from darc.voevent_generator import VOEventGenerator
        elif service == 'status_website':
            del StatusWebsite
            from darc.status_website import StatusWebsite
        elif service == 'offline_processing':
            del OfflineProcessing
            from darc.offline_processing import OfflineProcessing
        else:
            self.logger.error("Unknown how to reimport class for {}".format(service))

        return


def main():
    master = DARCMaster()
    master.run()

