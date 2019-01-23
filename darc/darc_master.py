#!/usr/bin/env python
#
# DARC master process
# Controls all services

import sys
import ast
import yaml
import logging
import logging.handlers
from queue import Queue
import threading
import socket
from time import sleep

from darc.definitions import *
from darc.amber_listener import AMBERListener
from darc.amber_triggering import AMBERTriggering
#from darc.voevent_generator import VOEventGenerator


class DARCMasterException(Exception):
    pass


class DARCMaster(object):
    def __init__(self):
        """
        Setup queues, config, logging
        """
        # setup queues
        self.amber_listener_queue = Queue()
        self.voevent_queue = Queue()

        # Initalize services
        self.amber_listener_stop = threading.Event()
        self.amber_listener = AMBERListener(self.amber_listener_stop)
        self.amber_triggering_stop = threading.Event()
        self.amber_triggering = AMBERTriggering(self.amber_triggering_stop)

        # Load config file
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['darc_master']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~')}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        handler = logging.handlers.WatchedFileHandler(self.log_file)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        self.logger.info('Initalized')

    def start(self):
        """
        Initalize the socket and listen for message
        """
        # setup listening socket
        try:
            command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            command_socket.bind((self.host, self.port))
        except socket.error as e:
            self.logger.error("Failed to create socket: {}".format(e))
            raise DARCMasterException("Failed to setup command socket")

        # wait for commands
        command_socket.listen(5)
        self.logger.info("Waiting for commands")

        while True:
            try:
                client, adr = command_socket.accept()
            except Exception as e:
                self.logger.error('Caught exception while waiting for command: {}', e)
                raise DARCMasterException('Caught exception while waiting for command: {}', e)

            raw_message = client.recv(1024)
            self.logger.info("Received message: {}".format(raw_message))
            try:
                status = self.parse_message(raw_message)
            except Exception as e:
                status = "Caught exception: {}".format(e)
            try:
                client.sendall(status)
            except socket.error as e:
                self.logger.error("Failed to send reply: {}".format(e))
            client.close()

    def parse_message(self, raw_message):
        """
        Parse raw received message
        :param raw_message: message as single string
        :return: status
        """
        #
        try:
            message = ast.literal_eval(raw_message)
        except Exception as e:
            self.logger.error("Failed to parse command message: {}".format(e))
            return "Failed - cannot parse message"

        try:
            service = message['service']
        except KeyError as e:
            self.logger.error("Service missing from command message: {}".format(e))
            return "Failed - no service specified"

        try:
            command = message['command']
        except KeyError as e:
            self.logger.error("Command missing from command message: {}".format(e))
            return "Failed - no command specified"

        payload = message.get('payload', None)
        status = self.process_message(service, command, payload)

        return status

    def process_message(self, service, command, payload):
        """
        :param service: service to interact with
        :param command: command to run
        :param payload: payload for command
        :return: status
        """
        status = "Success"
        if command.lower() == 'start':
            self.start_service(service)
        elif command.lower() == 'stop':
            self.stop_service(service)
        elif command.lower() == 'restart':
            self.stop_service(service)
            self.start_service(service)
        else:
            self.logger.error('Received unknown command: {}'.format(command))
            status = 'Unknown command: {}'.format(command)

        return status

    def start_service(self, service):
        """
        :param service: service to start
        :return: status
        """

        # settings for specific services
        if service == 'amber_listener':
            thread = self.amber_listener
            event = self.amber_listener_stop
            source_queue = None
            target_queue = self.amber_listener_queue

        elif service == 'amber_triggering':
            thread = self.amber_triggering
            event = self.amber_triggering_stop
            source_queue = self.amber_listener_queue
            target_queue = self.voevent_queue

        else:
            status = "Unknown service: {}".format(service)
            self.logger.error(status)
            return status

        # start the specified service
        self.logger.info("Starting service {}".format(service))
        # check if already running
        if thread.isAlive():
            status = "Service already running: {}".format(service)
            self.logger.warning(status)
        else:
            # set event to allow running
            event.clear()
            # set queues
            if source_queue:
                thread.set_source_queue(source_queue)
            if target_queue:
                thread.set_target_queue(target_queue)
            # start
            thread.start()
            # check status
            if not thread.isAlive():
                status = "Failed to start service: {}".format(service)
                self.logger.error(status)
            else:
                status = "Started service: {}".format(service)
                self.logger.info(status)

        return status

        ## start VO Event generator
        #voevent_generator = VOEventGenerator()
        ## set source queue for events
        #voevent_generator.set_source_queue(self.voevent_queue)
        #voevent_generator_thread = threading.Thread(target=voevent_generator.start)
        #voevent_generator_thread.daemon = True
        #voevent_generator_thread.start()

    def stop_service(self, service):
        """
        :param service: service to stop
        :return: status
        """

        self.logger.info("Stopping service {}".format(service))
        if service == 'amber_listener':
            if not self.amber_listener.isAlive():
                status = "Service not running: {}".format(service)
                self.logger.warning(status)
            else:
                self.amber_listener_stop.set()
                sleep(1)
                if self.amber_listener.isAlive():
                    status = "Failed to stop: {}".format(service)
                    self.logger.error(status)
                else:
                    status = "Service stopped: {}".format(service)
                    self.logger.info(status)

        elif service == 'amber_triggering':
            if not self.amber_triggering_thread_running:
                status = "Service not running: {}".format(service)
                self.logger.warning(status)
            else:
                self.amber_triggering_thread_stop.set()
                sleep(1)
                if self.amber_triggering_thread.isAlive():
                    status = "Failed to stop: {}".format(service)
                    self.logger.error(status)
                else:
                    self.amber_triggering_thread_running = False
                    status = "Service stopped: {}".format(service)
                    self.logger.info(status)
        else:
            status = "Unknown service: {}".format(service)
            self.logger.error(status)

        return status

    def restart_service(self, service):
        self.stop_service(service)
        self.start_service(service)


def main():
    master = DARCMaster()
    master.start()

