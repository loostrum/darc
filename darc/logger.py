#!/usr/bin/env python

import logging
import logging.handlers
from copy import copy


def get_logger(name, log_file, level=logging.DEBUG):
    """
    Create logger

    :param log_file: Path to log file
    :param level: log level (default: DEBUG)
    :return: logger
    """
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
