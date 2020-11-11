#!/usr/bin/env python3

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


def send_command(timeout, service, command, payload=None, host='localhost', port=None):
    """
    Send a command to the DARC master service

    :param float timeout: Timeout for reply in seconds
    :param str service: Service to send command to
    :param str command: Which command to send
    :param str payload: Payload for command (optional)
    :param str host: Hostname to connect to (default: localhost)
    :param int port: Port to connect to (default: get from DARC config file)
    :return: reply from DARC master
    """
    # define message as literal python dict
    if payload:
        message = "{{'service':'{}', 'command':'{}', 'payload':'{}'}}".format(service, command, payload)
    else:
        message = "{{'service':'{}', 'command':'{}'}}".format(service, command)
    if port is None:
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
            except Exception as e:
                logging.error("Failed to parse message (): {}".format(e, reply))
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
    """
    DARC command line interface

    This function is called by the darc executable

    Run darc --help for usage
    """
    # Check available services in config
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']
    if config['mode'] == 'real-time':
        services = config['services_master_rt'] + config['services_worker_rt']
    elif config['mode'] == 'mixed':
        services = config['services_master_mix'] + config['services_worker_mix']
    else:
        services = config['services_master_off'] + config['services_worker_off']
    master_commands = config['master_commands']
    service_commands = config['service_commands']
    commands = list(set(master_commands + service_commands))

    # Parse arguments
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument('--service', type=str, help="Which service to interact with, "
                        " available services: {}, or all".format(', '.join(services)))
    parser.add_argument('--timeout', type=int, default=10, help="Timeout for sending command "
                        "(Default: %(default)ss)")
    parser.add_argument('--host', type=str, default='localhost', help="Host to send command to "
                        "(Default: %(default)s)")
    parser.add_argument('--port', type=int, help="Port DARC listens to "
                        "(Default: determine from DARC config file)")
    parser.add_argument('--parset', type=str, help="Observation parset (takes precedence over --config)")
    parser.add_argument('--config', type=str, help="Node observation config")

    parser.add_argument('cmd', type=str, nargs='+', help="Command to execute. When using get_attr, add space "
                                                         "followed by attribute. Available commands: "
                                                         "{}".format(', '.join(commands)))
    args = parser.parse_args()

    # Check arguments
    if not args.cmd:
        logging.error("Add command to execute e.g. \"darc --service amber_listener status\"")
        sys.exit(1)
    cmd = args.cmd[0]

    try:
        attr = args.cmd[1]
    except IndexError:
        attr = None

    if cmd not in commands:
        logging.error("Unknown command: {}. Run darc -h to see available commands".format(cmd))
        sys.exit(1)
    elif not args.service and cmd not in master_commands:
        logging.error("Argument --service is required for given command")
        sys.exit(1)

    # add attribute to command if get_attr is called
    if attr is not None:
        if cmd == 'get_attr':
            cmd += f" {attr}"
        else:
            logging.error("Attribute can only be provided when using get_attr command")
            sys.exit(1)

    # If command is edit, open config in an editor
    if cmd == 'edit':
        with open(CONFIG_FILE, 'r') as f:
            master_config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']
        default_editor = master_config['editor']
        editor = os.environ.get('EDITOR', default_editor)
        ret = subprocess.Popen([editor, CONFIG_FILE]).wait()
        if ret != 0:
            logging.error("Editor did not exit properly")
        else:
            logging.info("Restart services to apply new settings, or run 'darc reload' to reload the master config.\n"
                         "WARNING: Restarting services aborts any running observation.\n"
                         "For services without queue server (i.e. all except LOFARTrigger and VOEventGenerator),\n"
                         "the config is automatically reloaded at the start of each observation.")
        sys.exit(ret)

    # Get payload
    if args.parset:
        payload = args.parset
    elif args.config:
        payload = args.config
    else:
        payload = None

    if not send_command(args.timeout, args.service, cmd, host=args.host, port=args.port, payload=payload):
        sys.exit(1)
