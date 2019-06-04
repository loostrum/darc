#!/usr/bin/env python

import os
import sys
import logging
import logging.handlers
from copy import copy
from darc import util


def get_logger(name, log_file, level=logging.DEBUG):
    """
    Create logger

    :param log_file: Path to log file
    :param level: log level (default: DEBUG)
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
    formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(module)s: %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(level)
    # remove any old handlers
    for h in copy(logger.handlers):
        h.close()
        logger.removeHandler(h)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
