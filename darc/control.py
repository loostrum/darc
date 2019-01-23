#!/usr/bin/env python

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
import yaml
import logging
import socket

from darc.definitions import *

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


def send_command(timeout, service, command, payload=None):
    """

    :param service: Service to send command to
    :param command: Which command to send
    :param payload: Payload for command (optional)
    :return:
    """
    # define message as literal python dict
    if payload:
        message = "{{'service':'{}', 'command':'{}', 'payload':'{}'}}".format(service, command, payload)
    else:
        message = "{{'service':'{}', 'command':'{}'}}".format(service, command)
    # read port from config
    with open(CONFIG_FILE, 'r') as f:
        master_config = yaml.load(f)['darc_master']
    port = master_config['port']
    # connect to master
    try:
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.settimeout(timeout)
        master_socket.connect(("localhost", port))
    except socket.error as e:
        logging.error("Failed to connect DARC master: {}".format(e))
        sys.exit(1)
    # send message
    master_socket.sendall(message)
    logging.info("Command send successfully")
    # receive reply
    try:
        reply = master_socket.recv(1024)
    except socket.timeout:
        logging.error("Did not receive reply before timeout")
    else:
        logging.info("Status: {}".format(reply))
    # close connection
    master_socket.close()


def main():
    # Check available services in config
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.load(f)['darc_master']
    services = config['services']

    # Parse arguments
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument('--service', type=str, help="Which service to interact with, "
                        " available services: {}".format(', '.join(services)))
    parser.add_argument('--timeout', type=int, default=10, help="Timeout for sending command "
                        "(Default: %(default)ss)")
    parser.add_argument('--cmd', type=str, help="Command to send to service")

    args = parser.parse_args()

    # Check arguments
    if not args.cmd:
        logging.error("Add command to execute, e.g. \"darc --service amber_listener status\"")
        sys.exit(1)
    if args.cmd.lower() == "status" and not args.service:
        args.service = "all"
    elif not args.service:
        logging.error("Argument --service is required unless status command is given")
        sys.exit(1)
    elif args.service not in services:
        logging.error("Service not found: {}".format(args.service))
        sys.exit(1)

    send_command(args.timeout, args.service, args.cmd)
