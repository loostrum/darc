#!/usr/bin/env python

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
import yaml
import ast
import logging
import socket

from darc.definitions import *

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def send_command(timeout, service, command, payload=None, host='localhost'):
    """
    :param timeout: Timeout for reply in seconds
    :param service: Service to send command to
    :param command: Which command to send
    :param payload: Payload for command (optional)
    :param host: Hostname (default: localhost)
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
        logging.error("Failed to connect to DARC master: {}".format(e))
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
            reply = None
        else:
            try:
                reply = ast.literal_eval(reply)
            except:
                logging.error("Failed to parse message: {}".format(reply))
            else:
                if isinstance(reply, dict):
                    for key, value in reply.items():
                        logging.info("{}: {}".format(key, value))
                else:
                    logging.info(reply)
        # close connection
        master_socket.close()
    return reply


def main():
    # Check available services in config
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.load(f)['darc_master']
    services = config['services_master'] + config['services_worker']
    commands = config['commands']

    # Parse arguments
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument('--service', type=str, help="Which service to interact with, "
                        " available services: {}, or all".format(', '.join(services)))
    parser.add_argument('--timeout', type=int, default=10, help="Timeout for sending command "
                        "(Default: %(default)ss)")
    parser.add_argument('--host', type=str, default='localhost', help="Host to send command to "
                       "(Default: %(default)s)")
    parser.add_argument('--parset', type=str, default=None, help="Observation parset (takes precedence over --config)")
    parser.add_argument('--config', type=str, default=None, help="Node observation config")

    parser.add_argument('cmd', type=str, help="Command to execute, available commands: {}".format(', '.join(commands)))
    args = parser.parse_args()

    # Check arguments
    if not args.cmd:
        logging.error("Add command to execute, e.g. \"darc --service amber_listener --cmd status\"")
        sys.exit(1)
    elif not args.service and args.cmd not in ['stop_master', 'start_observation']:
        logging.error("Argument --service is required unless calling stop_master or start_observation")
        sys.exit(1)

    # Get payload
    if args.parset:
        payload = args.parset
    elif args.config:
        payload = args.config
    else:
        payload = None

    send_command(args.timeout, args.service, args.cmd, host=args.host, payload=payload)
