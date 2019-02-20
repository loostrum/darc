#!/usr/bin/env python

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
import yaml
import logging
import socket

from darc.definitions import *

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


def send_command(timeout, service, command, payload=None, host='localhost'):
    """
    :param timeout: Timeout for reply in seconds
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
        master_socket.connect((host, port))
    except socket.error as e:
        logging.error("Failed to connect DARC master: {}".format(e))
        sys.exit(1)
    # send message
    master_socket.sendall(message)
    logging.info("Command sent successfully")
    # receive reply unless stop_all was sent
    if not command == 'stop_all':
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
                        " available services: {}, or all".format(', '.join(services)))
    parser.add_argument('--timeout', type=int, default=10, help="Timeout for sending command "
                        "(Default: %(default)ss)")
    parser.add_argument('--cmd', type=str, help="Command to send to service")

    args = parser.parse_args()

    # Check arguments
    if not args.cmd:
        logging.error("Add command to execute, e.g. \"darc --service amber_listener --cmd status\"")
        sys.exit(1)
    elif not args.service and args.cmd != 'stop_master':
        logging.error("Argument --service is required unless calling stop_all")
        sys.exit(1)
    elif args.service not in services and args.service != 'all' and args.cmd != 'stop_master':
        logging.error("Service not found: {}".format(args.service))
        sys.exit(1)

    send_command(args.timeout, args.service, args.cmd)
