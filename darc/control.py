#!/usr/bin/env python

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
import yaml
import logging

from darc.definitions import *
from darc.server import Server



def start_service(service):
    """
        Start the supplied service
    """
    server = Server(mode=service)
    server.run()


def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

    # Check available services in config
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.load(f)
    services = config.keys()

    # Parse arguments
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument('--service', type=str, help="Which service to start, available services: {}".format(' '.join(services)), required=True)

    args = parser.parse_args()

    # Check if service is valid
    if not args.service in services:
        logging.error("Service not found: {}".format(service))
        sys.exit()

    # Start the service
    start_service(args.service)
