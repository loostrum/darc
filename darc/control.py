#!/usr/bin/env python

import os
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
import yaml
import ast
import logging
import socket
import subprocess

from darc.definitions import CONFIG_FILE

logging.basicConfig(format='%(message)s', level=logging.DEBUG, stream=sys.stdout)


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
        master_config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']
    port = master_config['port']
    # connect to master
    try:
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.settimeout(timeout)
        master_socket.connect((host, port))
    except socket.error as e:
        logging.error("Failed to connect to DARC master: {}".format(e))
        return None
    # send message
    master_socket.sendall(message.encode())
    logging.info("Command sent successfully")
    reply = None
    # receive reply unless stop_all was sent
    if not command == 'stop_all':
        try:
            reply = master_socket.recv(1024).decode()
        except socket.timeout:
            logging.error("Did not receive reply before timeout")
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
        config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']
    if config['real_time']:
        services = config['services_master_rt'] + config['services_worker_rt']
    else:
        services = config['services_master_off'] + config['services_worker_off']
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

    parser.add_argument('cmd', type=str, default=None, help="Command to execute, available commands: {}".format(', '.join(commands)))
    args = parser.parse_args()

    # Check arguments
    if not args.cmd:
        logging.error("Add command to execute e.g. \"darc --service amber_listener status\"")
        sys.exit(1)
    elif not args.service and args.cmd not in ['stop_master', 'start_observation', 'stop_observation', 'edit']:
        logging.error("Argument --service is required unless calling stop_master or start/stop observation")
        sys.exit(1)

    # If command is edit, open confing in an editor
    if args.cmd == 'edit':
        with open(CONFIG_FILE, 'r') as f:
            master_config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']
        default_editor = master_config['editor']
        editor = os.environ.get('EDITOR', default_editor)
        ret = subprocess.Popen([editor, CONFIG_FILE]).wait()
        if ret != 0:
            logging.error("Editor did not exit properly")
        else:
            logging.info("Reload config with 'darc reload' to apply new settings")
        sys.exit(ret)

    # Get payload
    if args.parset:
        payload = args.parset
    elif args.config:
        payload = args.config
    else:
        payload = None

    if not send_command(args.timeout, args.service, args.cmd, host=args.host, payload=payload):
        sys.exit(1)
