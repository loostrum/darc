#!/usr/bin/env python
#
# Website 

import errno
import yaml
import threading
import socket
from textwrap import dedent
from astropy.time import Time

from darc.definitions import *
from darc.logger import get_logger
from darc.control import send_command


class StatusWebsiteException(Exception):
    pass


class StatusWebsite(threading.Thread):
    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self.daemon = True

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['status_website']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # store all node names
        self.all_nodes = [MASTER] + WORKERS

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        self.logger.info('Status website initialized')

        # create website directory
        try:
            os.makedirs(self.web_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                self.logger.error("Failed to create website directory: {}".format(e))
                raise StatusWebsiteException("Failed to create website directory: {}".format(e))

    def run(self):
        """
        """
        while not self.stop_event.is_set():
            self.logger.info("Getting status of all services")
            # get status all nodes
            statuses = {}
            for node in self.all_nodes:
                statuses[node] = {}
                self.logger.info("Getting {} status".format(node))
                try:
                    status = send_command(10, 'all', 'status', host=node)
                except Exception as e:
                    status = None
                    self.logger.error("Failed to get {} status: {}".format(node, e))
                statuses[node] = status
            self.logger.info("Publishing status")
            try:
                self.publish_status(statuses)
            except Exception as e:
                self.logger.error("Failed to publish status: {}".format(e))
            self.stop_event.wait(self.interval)

    def publish_status(self, statuses):
        """
        Publish status as simple html webpage
        """ 

        header, footer = self.get_template()
        webpage = header
        # add update time
        webpage += "<tr><td>Last update:</td><td colspan=2>{}</td></tr>".format(Time.now())

        # stopped running
        # add status of each node
        for node in self.all_nodes:
            if statuses[node] is None:
                webpage += "<tr><td style='background-color:{}'>{}</td></tr>".format(self.colour_unknown, node.upper())
            else:
                # first check if all ok
                colour_node = self.colour_good
                for service, status in statuses[node]['message'].items():
                    if not status == 'running':
                        colour_node = self.colour_bad
                        break
                # add node name
                webpage += "<tr><td style='background-color:{}'>{}</td>".format(colour_node, node.upper())
                for service, status in statuses[node]['message'].items():
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

    def get_template(self):
        """
        Return the HTML template
        """

        header = dedent("""<html>
                        <head><title>DARC status</title></head>
                        <body style='font-size: 10pt; 
                                     line-height: 1em; 
                                     font-family: arial'>


                        <p>
                        <table style="width:50%;text-align:left">
                        """)

        footer = dedent("""</table>
                        </p>
                        </body>
                        </html>
                        """)
        
        return header, footer
