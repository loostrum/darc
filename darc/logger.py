#!/usr/bin/env python

import logging
import logging.handlers


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
    logger.addHandler(handler)

    return logger
