#!/usr/bin/env python3
#
# DARC base class

import os
import socket
import multiprocessing as mp
import yaml
from queue import Empty

from darc.logger import get_logger
from darc.definitions import CONFIG_FILE, MASTER, WORKERS


class DARCBase(mp.Process):
    """
    DARC Base class

    Provides common methods to services
    """

    def __init__(self, source_queue, target_queue=None, second_target_queue=None,
                 control_queue=None, config_file=CONFIG_FILE,
                 no_logger=False):
        """
        :param Queue source_queue: Input queue
        :param Queue target_queue: Output queue
        :param Queue second_target_queue: second output queue
        :param Queue control_queue: Control queue
        :param str config_file: Path to config file
        :param bool no_logger: Skip logger initialization
        """
        super(DARCBase, self).__init__()
        self.stop_event = mp.Event()

        self.source_queue = source_queue
        self.target_queue = target_queue
        self.second_target_queue = second_target_queue
        self.control_queue = control_queue

        # set names for config and logger
        self.module_name = type(self).__module__.split('.')[-1]
        self.log_name = type(self).__name__

        # load config
        self.config_file = config_file
        self.load_config()

        if not no_logger:
            # setup logger
            self.logger = get_logger(self.module_name, self.log_file)
            self.logger.info("{} initialized".format(self.log_name))
        else:
            self.logger = None

        # set host type
        hostname = socket.gethostname()
        if hostname == MASTER:
            self.host_type = 'master'
        elif hostname in WORKERS:
            self.host_type = 'worker'
        else:
            if not no_logger:
                self.logger.warning("Running on unknown host")
            self.host_type = None

    def load_config(self):
        """
        Load config file
        """
        with open(self.config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)[self.module_name]

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

    def stop(self, abort=False):
        """
        Stop this service

        :param bool abort: Whether to abort running observation
        """
        self.logger.info("Stopping {}".format(self.log_name))
        # run cleanup, avoid issue if abort keyword does not exist
        try:
            self.cleanup(abort=abort)
        except TypeError:
            self.cleanup()
        self.stop_event.set()

    def run(self):
        """
        Main loop

        Receive commands on input queue, calls self.start_observation, self.stop_observation,
        else self.process_command
        """
        # check queues
        try:
            self.logger.info("Starting {}".format(self.log_name))
            while not self.stop_event.is_set():
                # read from queue
                try:
                    command = self.source_queue.get(timeout=.1)
                except Empty:
                    continue
                # command received, process it
                if isinstance(command, str) and command == 'stop':
                    self.stop()
                elif isinstance(command, str) and command == 'abort':
                    self.stop(abort=True)
                elif command['command'] == "start_observation":
                    self.logger.info("Starting observation")
                    try:
                        if 'reload_conf' in command.keys():
                            self.start_observation(command['obs_config'], reload=command['reload_conf'])
                        else:
                            self.start_observation(command['obs_config'])

                    except Exception as e:
                        self.logger.error("Failed to start observation: {}: {}".format(type(e), e))
                elif command['command'] == "stop_observation":
                    self.logger.info("Stopping observation")
                    if 'obs_config' in command.keys():
                        self.stop_observation(obs_config=command['obs_config'])
                    else:
                        self.stop_observation()
                else:
                    self.process_command(command)
        # EOFError can occur due to usage of queues
        # Can be ignored
        except EOFError:
            pass
        except Exception as e:
            self.logger.error("Caught exception in main loop: {}: {}".format(type(e), e))
            self.stop()

    def start_observation(self, *args, reload=True, **kwargs):
        """
        Start observation. By default only (re)loads config file.

        :param list args: start_observation arguments
        :param bool reload: reload service settings (default: True)
        :param dict kwargs: start_observation keyword arguments
        """
        if reload:
            self.load_config()

    def stop_observation(self, *args, **kwargs):
        """
        Stop observation stub, should be overridden by subclass if commands need to be executed at
        observation stop

        :param list args: stop_observation arguments
        :param dict kwargs: stop_observation keyword arguments
        """
        pass

    def cleanup(self, abort=False):
        """
        Stub for commands to run upon service stop, defaults to self.stop_observation

        :param bool abort: Whether to abort running observation
        """
        # run stop observation, avoid issue if abort keyword does not exist
        try:
            self.stop_observation(abort=abort)
        except TypeError:
            self.stop_observation()

    def get_attribute(self, command):
        """
        Get attribute as given in input command

        :param dict command: Command received over queue
        """
        try:
            value = getattr(self, command['attribute'])
        except KeyError:
            self.logger.error("Missing 'attribute' key from command")
            status = 'Error'
            reply = 'missing attribute key from command'
        except AttributeError:
            status = 'Error'
            reply = f"No such attribute: {command['attribute']}"
        else:
            status = 'Success'
            reply = f"{{'{self.log_name}.{command['attribute']}': {value}}}"

        if self.control_queue is not None:
            self.control_queue.put([status, reply])
        else:
            self.logger.error("Cannot send reply: no control queue set")

    def process_command(self, command, *args, **kwargs):
        """
        Process command from queue, other than start_observation and stop_observation.
        By default only provides get_attribute command

        :param dict command: Input command
        :param list args: process command arguments
        :param dict kwargs: process command keyword arguments
        """
        if isinstance(command, dict) and command['command'] == 'get_attr':
            self.get_attribute(command)
        else:
            raise NotImplementedError(f"Unknown command: {command['command']}")
