#!/usr/bin/env python3
#
# DARC master process
# Controls all services

import sys
import os
import argparse
import ast
import yaml
import multiprocessing as mp
from queue import Empty
import socket
from time import sleep, time
from shutil import copy2
from astropy.time import Time, TimeDelta

import darc
from darc.definitions import MASTER, WORKERS, CONFIG_FILE, TIME_UNIT
from darc import util
from darc.logger import get_logger


class DARCMasterException(Exception):
    pass


class DARCMaster:
    """
    DARC master service that controls all other services and queues

    Interact with this service through the 'darc' executable
    """
    def __init__(self, config_file=CONFIG_FILE):
        """
        :param str config_file: path to DARC configuration file
        """
        # setup stop event for master
        self.stop_event = mp.Event()

        # dict to hold thread for each service
        self.threads = {}
        self.services = []

        # save host name
        self.hostname = socket.gethostname()

        # setup queues
        self.amber_listener_queue = mp.Queue()  # only used for start observation commands
        self.amber_trigger_queue = mp.Queue()  # for amber triggers
        self.dadatrigger_queue = mp.Queue()  # for dada triggers
        self.processor_queue = mp.Queue()  # for semi-realtime processing
        self.offline_queue = mp.Queue()  # for offline processing
        self.status_website_queue = mp.Queue()  # for controlling the StatusWebsite service
        self.lofar_trigger_queue = mp.Queue()  # for controlling the LOFARTrigger service
        self.voevent_generator_queue = mp.Queue()  # for controlling the VOEventGenerator service

        self.control_queue = mp.Queue()  # for receiving data from services

        # all queues that are an input to a service
        self.all_queues = [self.amber_listener_queue, self.amber_trigger_queue, self.dadatrigger_queue,
                           self.processor_queue, self.offline_queue, self.status_website_queue]

        # store hostname
        self.hostname = socket.gethostname()

        # Load config file, this also initializes the logger
        self.config_file = config_file
        self._load_config()

        # service to class mapper
        self.service_mapping = {'voevent_generator': darc.VOEventGenerator,
                                'status_website': darc.StatusWebsite,
                                'amber_listener': darc.AMBERListener,
                                'amber_clustering': darc.AMBERClustering,
                                'dada_trigger': darc.DADATrigger,
                                'lofar_trigger': darc.LOFARTrigger,
                                'offline_processing': darc.OfflineProcessing}

        # select which processor to run on this host
        if self.hostname == MASTER:
            self.service_mapping['processor'] = darc.ProcessorMasterManager
        elif self.hostname in WORKERS:
            self.service_mapping['processor'] = darc.ProcessorManager
        else:
            self.logger.warning("Cannot determine host type; setting processor to worker mode")
            self.service_mapping['processor'] = darc.ProcessorManager

        # setup listening socket
        command_socket = None
        start = time()
        while not command_socket and time() - start < self.socket_timeout:
            try:
                command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                command_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                command_socket.bind(("", self.port))
            except socket.error as e:
                self.logger.warning("Failed to create socket, will retry: {}".format(e))
                command_socket = None
                sleep(1)

        if not command_socket:
            self.logger.error("Failed to create socket")
            raise DARCMasterException("Failed to setup command socket")

        self.command_socket = command_socket

        self.logger.info('DARC Master initialized')

    def _load_config(self):
        """
        Load configuration file
        """

        with open(self.config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': self.hostname}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # create main log dir
        log_dir = os.path.dirname(self.log_file)
        try:
            util.makedirs(log_dir)
        except Exception as e:
            raise DARCMasterException("Cannot create log directory: {}".format(e))

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # store services
        if self.hostname == MASTER:
            if self.mode == 'real-time':
                self.services = self.services_master_rt
            elif self.mode == 'mixed':
                self.services = self.services_master_mix
            else:
                self.services = self.services_master_off
        elif self.hostname in WORKERS:
            if self.mode == 'real-time':
                self.services = self.services_worker_rt
            elif self.mode == 'mixed':
                self.services = self.services_worker_mix
            else:
                self.services = self.services_worker_off
        else:
            self.services = []

        # terminate any existing threads in case this is a reload
        for service, thread in self.threads.items():
            if thread is not None:
                self.logger.warning(f"Config reload, terminating existing {service}")
                thread.terminate()

        # start with empty thread for each service
        for service in self.services:
            self.threads[service] = None

        # print config file path
        self.logger.info("Loaded config from {}".format(self.config_file))

    def run(self):
        """
        Main loop

        Listen for messages on the command socket and process them
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
            self.logger.debug("Received message: {}".format(raw_message))
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

        :param str raw_message: message as single string
        :return: status, reply
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
        Process received message

        :param str service: service to interact with
        :param str command: command to run
        :param payload: payload for command
        :return: status, reply
        """

        # First check for commands that do not require service argument
        # Start observation
        if command == 'start_observation':
            if not payload:
                self.logger.error('Payload is required when starting observation')
                status = 'Error'
                reply = {'error': 'Payload missing'}
            else:
                status, reply = self.start_observation(payload, service)
            return status, reply
        # Stop observation
        # only stop in real-time modes, as offline processing runs after the observation
        elif command == 'stop_observation':
            if self.mode in ['real-time', 'mixed']:
                if not payload:
                    self.logger.error('Payload is required when stopping observation')
                    status = 'Error'
                    reply = {'error': 'Payload missing'}
                else:
                    status, reply = self.stop_observation(payload, service)
            else:
                self.logger.info("Ignoring stop observation command in offline processing mode")
                status = 'Success'
                reply = 'Ignoring stop in offline processing mode'
            return status, reply
        # Abort observation
        # always stop if aborted
        elif command == 'abort_observation':
            if not payload:
                self.logger.error('Payload is required when aborting observation')
                status = 'Error'
                reply = {'error': 'Payload missing'}
            status, reply = self.stop_observation(payload, service=service, abort=True)
            return status, reply
        # Stop master
        elif command == 'stop_master':
            status, reply = self.stop()
            return status, reply
        # master config reload
        elif command == 'reload':
            self._load_config()
            return 'Success', 'Config reloaded'
        # lofar / voevent trigger commands
        elif command.startswith('lofar') or command.startswith('voevent'):
            status, reply = self._switch_cmd(command)
            return status, reply
        # get attribute command for master
        elif command.startswith('get_attr') and service == 'None':
            status, reply = self._get_attribute('master', command)
            return status, reply

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
            elif command.startswith('get_attr'):
                _status, _reply = self._get_attribute(service, command)
            else:
                self.logger.error('Received unknown command: {}'.format(command))
                status = 'Error'
                reply = {'error': 'Unknown command: {}'.format(command)}
                return status, reply
            if _status != 'Success':
                status = 'Error'
            reply[service] = _reply

        return status, reply

    def _get_attribute(self, service, command):
        """
        Get attribute of a service instance

        :param str service: Which service to get an attribute of
        :param str command: Full service command ("get_attr {attribute}")
        """
        # split command in actual command and attribute
        cmd, attr = command.split(' ')

        # check if we need to get attribute of self
        if service == 'master':
            try:
                value = getattr(self, attr)
            except KeyError:
                self.logger.error("Missing 'attribute' key from command")
                status = 'Error'
                reply = 'missing attribute key from command'
            except AttributeError:
                status = 'Error'
                reply = f"No such attribute: {attr}"
            else:
                status = 'Success'
                reply = f"DARCMaster.{attr} = {value}"
            print(reply)
            return status, reply

        # get the service instance. process_message already checks for invalid service, no need to
        # to that here
        thread = self.threads[service]
        if (thread is None) or (not thread.is_alive()):
            status = 'Error'
            reply = f"Service not running: {service}"
            return status, reply

        # send attribute get request to service queue
        queue = self.get_queue(service)
        queue.put({'command': cmd, 'attribute': attr})
        # retrieve reply
        try:
            status, reply = self.control_queue.get(timeout=self.control_timeout)
        except Empty:
            return 'Error', f"Command sent to service, but no reply received within {self.control_timeout} seconds"

        return status, reply

    def check_status(self, service):
        """
        Check status of a service

        :param str service: Service to check status of
        :return: status, reply
        """

        status = 'Success'

        thread = self.threads[service]
        # self.logger.info("Checking status of {}".format(service))
        if thread is None:
            # no thread means the service is not running
            # self.logger.info("{} is stopped".format(service))
            reply = 'stopped'
        elif thread.is_alive():
            # self.logger.info("{} is running".format(service))
            reply = 'running'
        else:
            # self.logger.info("{} is stopped".format(service))
            reply = 'stopped'

        return status, reply

    def get_queue(self, service):
        """
        Get control queue corresponding to a service

        :param str service: Service to get queue for
        :return: queue (Queue)
        """
        if service == 'amber_listener':
            queue = self.amber_listener_queue
        elif service == 'amber_clustering':
            queue = self.amber_trigger_queue
        elif service == 'voevent_generator':
            queue = self.voevent_generator_queue
        elif service == 'status_website':
            queue = self.status_website_queue
        elif service == 'offline_processing':
            queue = self.offline_queue
        elif service == 'dada_trigger':
            queue = self.dadatrigger_queue
        elif service == 'lofar_trigger':
            queue = self.lofar_trigger_queue
        elif service == 'processor':
            queue = self.processor_queue
        else:
            queue = None
        return queue

    def start_service(self, service):
        """
        Start a service

        :param str service: service to start
        :return: status, reply
        """

        if service not in self.service_mapping.keys():
            self.logger.error('Unknown service: {}'.format(service))
            status = 'Error'
            reply = "Unknown service"
            return status, reply

        # get thread
        thread = self.threads[service]

        # check if a new thread has to be generated
        if thread is None or not thread.is_alive():
            self.logger.info("Creating new thread for service {}".format(service))
            self.create_thread(service)
            thread = self.threads[service]

        # start the specified service
        self.logger.info("Starting service: {}".format(service))
        # check if already running
        if thread.is_alive():
            status = 'Success'
            reply = 'already running'
            self.logger.warning("Service already running: {}".format(service))
        else:
            # start
            thread.start()
            # check status
            if not thread.is_alive():
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
        Stop a service

        :param str service: service to stop
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
        if thread is None or not thread.is_alive():
            # is_alive is false if thread died
            # remove thread if that is the case
            if thread is not None and not thread.is_alive():
                self.threads[service] = None
            self.logger.info("Service not running: {}".format(service))
            reply = 'Success'
            status = 'Stopped service'
            return status, reply

        # stop the specified service
        self.logger.info("Stopping service: {}".format(service))
        queue = self.get_queue(service)
        if not queue:
            status = 'Error'
            reply = f"No queue to stop {service}"
            return status, reply

        queue.put('stop')
        tstart = time()
        while thread.is_alive() and time() - tstart < self.stop_timeout:
            sleep(.1)
        if thread.is_alive():
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
        Restart a service

        :param str service: service to restart
        :return: status, reply
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
        Initialise a service thread

        :param str service: service to create a new thread for
        """
        # settings for specific services
        source_queue = self.get_queue(service)
        second_target_queue = None
        if service == 'amber_listener':
            target_queue = self.amber_trigger_queue
            # only output to processor if this is not the master
            if self.hostname != MASTER:
                second_target_queue = self.processor_queue
        elif service == 'amber_clustering':
            target_queue = self.dadatrigger_queue
        elif service == 'voevent_generator':
            target_queue = None
        elif service == 'status_website':
            target_queue = None
        elif service == 'offline_processing':
            target_queue = None
        elif service == 'dada_trigger':
            target_queue = None
        elif service == 'lofar_trigger':
            target_queue = None
        elif service == 'processor':
            target_queue = None
        else:
            self.logger.error("Cannot create thread for {}".format(service))
            return

        # Instantiate a new instance of the class
        self.threads[service] = self.service_mapping[service](source_queue=source_queue,
                                                              target_queue=target_queue,
                                                              second_target_queue=second_target_queue,
                                                              control_queue=self.control_queue,
                                                              config_file=self.config_file)

    def stop(self):
        """
        Stop all services and exit

        :return: status, reply
        """
        self.logger.info("Stopping all services")
        for service in self.services:
            self.stop_service(service)
        self.stop_event.set()
        status = 'Success'
        reply = "Stopping master"
        return status, reply

    def start_observation(self, config_file, service=None):
        """
        Start an observation

        :param str config_file: Path to observation config file
        :param str service: Which service to send start_observation to (default: all)
        :return: status, reply
        """
        if service == "None":
            service = None

        self.logger.info("Received start_observation command with config file {}".format(config_file))
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
        # ensure services are running
        if service is None:
            for s in self.services:
                self.start_service(s)
        else:
            self.start_service(service)

        # check host type
        if self.hostname == MASTER:
            host_type = 'master'
        elif self.hostname in WORKERS:
            host_type = 'worker'
        else:
            self.logger.error("Running on unknown host: {}".format(self.hostname))
            return "Error", "Failed: running on unknown host"

        # create command
        command = {'command': 'start_observation', 'obs_config': config, 'host_type': host_type}

        # wait until start time
        utc_start = Time(config['startpacket'] / TIME_UNIT, format='unix')
        utc_end = utc_start + TimeDelta(config['duration'], format='sec')
        # if end time is in the past, only start offline processing and processor
        if utc_end < Time.now():
            self.logger.warning("End time in past! Only starting offline processing and/or processor")
            if service is None:
                self.offline_queue.put(command)
                self.processor_queue.put(command)
                return "Warning", "Only offline processing and processor started"
            elif service == 'offline_processing':
                self.offline_queue.put(command)
                return "Warning", "Only offline processing started"
            elif service == 'processor':
                self.processor_queue.put(command)
                return "Warning", "Only processor started"
            else:
                return "Error", "Can only start offline processing and processor when end time is in past"

        t_setup = utc_start - TimeDelta(self.setup_time, format='sec')
        self.logger.info("Starting observation at {}".format(t_setup.isot))
        util.sleepuntil_utc(t_setup)

        if service is None:
            # clear queues, then send command
            for queue in self.all_queues:
                util.clear_queue(queue)
            for queue in self.all_queues:
                queue.put(command)
            return "Success", "Observation started"
        else:
            # only start specified service. As this is only used in case e.g. something fails during
            # an observation, do not clear the queues first
            queue = self.get_queue(service)
            queue.put(command)
            return "Warning", "Only observation for {} started".format(service)

    def stop_observation(self, config_file, abort=False, service=None):
        """
        Stop an observation

        :param str config_file: path to observation config file
        :param bool abort: whether to abort the observation
        :param str service: Which service to send start_observation to (default: all)
        :return: status, reply message
        """
        self.logger.info("Received stop_observation command with config file {}".format(config_file))
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

        # call stop_observation for all relevant services through their queues
        if service is None:
            for queue in self.all_queues:
                # in mixed mode, skip stopping offline_processing, unless abort is True
                if (self.mode == 'mixed') and (queue == self.offline_queue) and not abort:
                    self.logger.info("Skipping stopping offline processing in mixed mode")
                    continue
                queue.put({'command': 'stop_observation', 'obs_config': config})
            status = 'Success'
            reply = "Stopped observation"
        else:
            queue = self.get_queue(service)
            self.logger.info("Only stopping observation for {}".format(service))
            queue.put({'command': 'stop_observation', 'obs_config': config})
            status = 'Warning'
            reply = "Only stopped observation for {}".format(service)

        self.logger.info("Stopped observation")
        return status, reply

    def _load_yaml(self, config_file):
        """
        Load yaml file and convert to observation config

        :param str config_file: Path to yaml file
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

        :param str config_file: Path to parset file
        :return: observation configuration
        """
        self.logger.info("Loading parset {}".format(config_file))
        if not os.path.isfile(config_file):
            self.logger.error("Parset not found: {}".format(config_file))
            # no parset - do not process this observation
            return {'proctrigger': False}

        # Read raw parset
        with open(config_file) as f:
            parset = f.read().strip()
        # Convert to dict
        config = util.parse_parset(parset)
        return config

    def _switch_cmd(self, command):
        """
        Check status of LOFAR trigger system / VOEvent generator, or enable/disable them

        :param str command: command to run
        :return: status, reply
        """
        if command.startswith('lofar_'):
            service = self.threads['lofar_trigger']
            name = 'LOFAR triggering'
            queue = self.lofar_trigger_queue
        elif command.startswith('voevent_'):
            service = self.threads['voevent_generator']
            name = 'VOEvent generator'
            queue = self.voevent_generator_queue
        else:
            self.logger.info("Unknown command: {}".format(command))
            return 'Error', "Failed: Unknown command {}".format(command)

        if self.hostname != MASTER:
            return 'Error', "Failed: should run on master node"

        # check if service is running
        if (service is None) or (not service.is_alive()):
            return 'Error', f"{name} service is not running"

        # send command to service
        queue.put(command)
        # retrieve reply
        try:
            status, reply = self.control_queue.get(timeout=self.control_timeout)
        except Empty:
            return 'Error', f"Command sent to service, but no reply received within {self.control_timeout} seconds"

        return status, reply


def main():
    """
    Run DARC Master
    """
    parser = argparse.ArgumentParser(description="DARC master service")
    parser.add_argument('--config', default=CONFIG_FILE, help="Path to config file, loaded from $HOME/darc/config.yaml "
                                                              "if it exists, else default provided with package."
                                                              "Current default: %(default)s")
    args = parser.parse_args()

    master = DARCMaster(args.config)
    master.run()
