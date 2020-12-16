#!/usr/bin/env python3

import os
import sys
import logging
import logging.handlers
from copy import copy
from darc import util


def get_logger(name, log_file, level=logging.INFO):
    """
    Create logger

    :param str name: name to use in log prints
    :param str log_file: Path to log file
    :param int level: log level (default: logging.INFO)
    :return: logger
    """
    # create log dir
    log_dir = os.path.dirname(os.path.realpath(log_file))
    try:
        util.makedirs(log_dir)
    except Exception as e:
        sys.stderr.write("Could not create log directory: {}\n".format(e))
    # setup logger
    logger = logging.getLogger(name)
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(level)
    # remove any old handlers
    for h in copy(logger.handlers):
        h.close()
        logger.removeHandler(h)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def get_queue_logger_listener(queue, log_file):
    """
    Create thread that logs message from a queue
    :param Queue queue: Log queue
    :param str log_file: Path to log file
    :return: QueueListener thread
    """
    # create log dir
    log_dir = os.path.dirname(os.path.realpath(log_file))
    try:
        util.makedirs(log_dir)
    except Exception as e:
        sys.stderr.write("Could not create log directory: {}\n".format(e))
    # setup handler
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s: %(message)s')
    handler.setFormatter(formatter)
    listener = logging.handlers.QueueListener(queue, handler)
    return listener


def get_queue_logger(name, queue, level=logging.INFO):
    """
    Create logger that puts messages on a queue to be handled by a separate listener
    :param str name: name to use in log prints
    :param Queue queue: Log queue
    :param int level: log level (default: logging.INFO)
    :return: logger
    """
    # setup logger
    logger = logging.getLogger(name)
    handler = logging.handlers.QueueHandler(queue)
    logger.setLevel(level)
    # remove any old handlers
    for h in copy(logger.handlers):
        h.close()
        logger.removeHandler(h)
    logger.addHandler(handler)
    return logger
