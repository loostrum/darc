#!/usr/bin/env python3
#
# Website

import os
import yaml
import threading
import socket
from textwrap import dedent
from astropy.time import Time

from darc.definitions import CONFIG_FILE, MASTER, WORKERS
from darc import util
from darc.logger import get_logger
from darc.control import send_command


class StatusWebsiteException(Exception):
    pass


class StatusWebsite(threading.Thread):
    """
    Generate a HTML page with the status of each service
    across the ARTS cluster at regular intervals
    """

    def __init__(self):
        """
        """
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.daemon = True

        # load config, including master for list of services
        with open(CONFIG_FILE, 'r') as f:
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

        self.logger.info('Status website initialized')

        # create website directory
        try:
            util.makedirs(self.web_dir)
        except Exception as e:
            self.logger.error("Failed to create website directory: {}".format(e))
            raise StatusWebsiteException("Failed to create website directory: {}".format(e))

    def run(self):
        """
        Main loop:

        #. Get status of all services across all nodes
        #. Publish HTML page with statuses
        #. Generate offline page upon exit
        """
        while not self.stop_event.is_set():
            self.logger.info("Getting status of all services")
            # get status all nodes
            statuses = {}
            for node in self.all_nodes:
                statuses[node] = {}
                # self.logger.info("Getting {} status".format(node))
                try:
                    status = send_command(10, 'all', 'status', host=node)
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
