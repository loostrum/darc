#!/usr/bin/env python3
#
# Website

import os
import logging
import yaml
import multiprocessing as mp
import threading
from queue import Empty
import socket
from textwrap import dedent
from astropy.time import Time

from darc.definitions import CONFIG_FILE, MASTER, WORKERS
from darc import util
from darc.control import send_command
from darc.logger import get_logger


class StatusWebsiteException(Exception):
    pass


class StatusWebsite(mp.Process):
    """
    Generate a HTML page with the status of each service
    across the ARTS cluster at regular intervals
    """

    def __init__(self, source_queue, *args, config_file=CONFIG_FILE, control_queue=None, **kwargs):
        """
        :param Queue source_queue: Input queue
        :param str config_file: Path to config file
        :param Queue control_queue: Control queue of parent Process
        """
        super(StatusWebsite, self).__init__()
        self.stop_event = mp.Event()
        self.source_queue = source_queue
        self.control_queue = control_queue

        # load config, including master for list of services
        with open(config_file, 'r') as f:
            config_all = yaml.load(f, Loader=yaml.SafeLoader)

        config = config_all['status_website']
        config_master = config_all['darc_master']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # set services
        if config_master['mode'] == 'real-time':
            self.services_master = config_master['services_master_rt']
            self.services_worker = config_master['services_worker_rt']
        elif config_master['mode'] == 'mixed':
            self.services_master = config_master['services_master_mix']
            self.services_worker = config_master['services_worker_mix']
        else:
            self.services_master = config_master['services_master_off']
            self.services_worker = config_master['services_worker_off']

        # store all node names
        self.all_nodes = [MASTER] + WORKERS

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        self.command_checker = None

        # reduce logging from status check commands
        logging.getLogger('darc.control').setLevel(logging.ERROR)

        # create website directory
        try:
            util.makedirs(self.web_dir)
        except Exception as e:
            self.logger.error("Failed to create website directory: {}".format(e))
            raise StatusWebsiteException("Failed to create website directory: {}".format(e))

        self.logger.info('Status website initialized')

    def run(self):
        """
        Main loop:

        #. Get status of all services across all nodes
        #. Publish HTML page with statuses
        #. Generate offline page upon exit
        """
        # run command checker
        self.command_checker = threading.Thread(target=self.check_command)
        self.command_checker.daemon = True
        self.command_checker.start()

        while not self.stop_event.is_set():
            self.logger.info("Getting status of all services")
            # get status all nodes
            statuses = {}
            for node in self.all_nodes:
                statuses[node] = {}
                try:
                    status = send_command(self.timeout, 'all', 'status', host=node)
                except Exception as e:
                    status = None
                    self.logger.error("Failed to get {} status: {}".format(node, e))
                statuses[node] = status
                # break immediately if stop event is set
                if self.stop_event.is_set():
                    break
            self.logger.info("Publishing status")
            try:
                self.publish_status(statuses)
            except Exception as e:
                self.logger.error("Failed to publish status: {}".format(e))
            self.stop_event.wait(self.interval)
        # Create website for non-running status website
        self.make_offline_page()

    def _get_attribute(self, command):
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
            reply = f"{{'{type(self).__name__}.{command['attribute']}': {value}}}"

        if self.control_queue is not None:
            self.control_queue.put([status, reply])
        else:
            self.logger.error("Cannot send reply: no control queue set")

    def check_command(self):
        """
        Check if this service should execute a command
        """
        while not self.stop_event.is_set():
            try:
                command = self.source_queue.get(timeout=1)
            except Empty:
                continue
            if isinstance(command, str) and command == 'stop':
                self.stop()
            elif isinstance(command, dict) and command['command'] == 'get_attr':
                self._get_attribute(command)
            else:
                self.logger.warning(f"Ignoring unknown command: {command['command']}")

    def stop(self):
        """
        Stop this service
        """
        self.stop_event.set()

    def publish_status(self, statuses):
        """
        Publish status as simple html webpage

        :param dict statuses: Status of each service across all nodes
        """

        header, footer = self.get_template()
        webpage = header
        # add update time
        webpage += "<tr><td>Last update:</td><td colspan=2>{}</td></tr>".format(Time.now())

        # stopped running
        # add status of each node
        for node in self.all_nodes:
            # If node has no status, set to unknown
            if statuses[node] is None:
                webpage += "<tr><td style='background-color:{}'>{}</td></tr>".format(self.colour_unknown, node.upper())
                continue

            # check node type
            if node == MASTER:
                services = self.services_master
            elif node in WORKERS:
                services = self.services_worker
            else:
                self.logger.error("Failed to determine whether node {} is worker or master:".format(node))
                continue

            # first check if all ok to determine host colour
            colour_node = self.colour_good

            for service in services:
                try:
                    status = statuses[node]['message'][service]
                except KeyError:
                    self.logger.error("Failed to get status of {} for {}".format(node, service))
                    status = 'error'

                if not status == 'running':
                    colour_node = self.colour_bad
                    break
            # add node name
            webpage += "<tr><td style='background-color:{}'>{}</td>".format(colour_node, node.upper())

            # add status of each service
            for service in services:
                try:
                    status = statuses[node]['message'][service]
                except KeyError:
                    self.logger.error("Failed to get status of {} for {}".format(node, service))
                    status = 'error'

                # get proper name of service
                try:
                    name = self.service_to_name[service]
                except KeyError:
                    name = "No name found for {}".format(service)
                # get color based on status
                if status == 'running':
                    colour_service = self.colour_good
                elif status == 'stopped':
                    colour_service = self.colour_bad
                else:
                    colour_service = self.colour_unknown
                webpage += "<td style='background-color:{}'>{}</td>".format(colour_service, name)
            webpage += "</tr>\n"

        webpage += footer

        web_file = os.path.join(self.web_dir, 'index.html')
        with open(web_file, 'w') as f:
            f.write(webpage)

    @staticmethod
    def get_template():
        """
        Return HTML template

        Split into header and footer

        :return: header, footer
        """

        header = dedent("""
                        <html>
                        <head>
                        <title>DARC status</title>
                        <meta http-equiv="refresh" content="10">
                        </head>
                        <body style='font-size: 10pt;
                                     line-height: 1em;
                                     font-family: arial'>


                        <p>
                        <table style="width:50%;text-align:left">
                        """)

        footer = dedent("""
                        </table>
                        </p>
                        </body>
                        </html>
                        """)

        return header, footer

    def make_offline_page(self):
        """
        Create page for when status website is offline
        """
        web_file = os.path.join(self.web_dir, 'index.html')
        webpage = dedent("""
                         <html><head>
                         <title>DARC status</title>
                         <meta http-equiv="refresh" content="10">
                         </head><body>
                         <p>
                         <h1>Status website offline</h1>
                         </p>
                         </body>
                         </html>
                          """)

        with open(web_file, 'w') as f:
            f.write(webpage)
        return
