#!/usr/bin/env python
#
# OfflineProcessing end to end test
# 
# 

import numpy as np
import logging
import threading

from darc.offline_processing import OfflineProcessing

def run_processing(**config):
    # config for worker should contain:
    # ntabs 
    # mode (IAB/TAB)
    # output_dir (/data2/output/<date>/<datetimesource)
    # endtime (UTC)
    # beam (CB)
    # amber_dir
    # duration
    # master_dir

    event = threading.Event()
    proc = OfflineProcessing(event)
    # override logger (first initalized message still goes to normal logger)
    logging.basicConfig(format='%(asctime)s.%(levelname)s.%(module)s: %(message)s', level='DEBUG')
    logger = logging.getLogger()
    proc.logger = logger

    # start worker observation 
    try:
        proc._start_observation_worker(config)
        pass
    except Exception as e:
        logger.error("Offline processing failed: {}".format(e))
        return
    logger.info("Offline processing done")


if __name__ == '__main__':
    conf = {'ntabs': 12, 'beam': 0, 'mode': 'TAB', 'amber_dir': '/dev/null',
            'output_dir': '/dev/null', 'duration': 61.44, 
            'endtime': "2019-01-01 09:00:00", 'master_dir': '/dev/null'}

    run_processing(**conf)
