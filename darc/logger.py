#!/usr/bin/env python

import logging


def get_logger(name, log_file, level=logging.DEBUG):
    """
    Create logger

    :param log_file: Path to log file
    :param level: log level (default: DEBUG)
    :return: logger
    """
    # setup logger
    logger = logging.getLogger(name)
    logger.basicConfig(format='%(asctime)s.%(levelname)s.%(module)s: %(message)s',
                       level=level,
                       filename=log_file)

    return logger
