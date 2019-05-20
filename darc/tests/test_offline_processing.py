#!/usr/bin/env python
#
# OfflineProcessing end to end test
# 
# 
import os
import sys
import logging
import threading

import numpy as np
from astropy.time import Time

from darc.offline_processing import OfflineProcessing

def run_processing(**config):
    # config for worker should contain:
    # ntabs 
    # mode (IAB/TAB)
    # output_dir (/data2/output/<date>/<datetimesource)
    # startpacket
    # beam (CB)
    # amber_dir
    # duration
    # result_dir

    logging.basicConfig(format='%(asctime)s.%(levelname)s.%(module)s: %(message)s', level='DEBUG')
    logger = logging.getLogger()

    # Create result dir (normally done in run method, but that is skipped)
    try:
        os.makedirs(config['result_dir'])
    except OSError as e:
        logger.error('Cannot create result dir {}: {}'.format(config['result_dir'], e))
        sys.exit(1)

    event = threading.Event()
    proc = OfflineProcessing(event)
    # override logger (first initalized message still goes to normal logger)
    proc.logger = logger

    # override config
    proc.config['nfreq_plot'] = 32
    proc.config['snrmin_processing'] = 10
    proc.config['snrmin_processing_local'] = 5
    proc.config['dmmin'] = 20
    proc.config['dmmax'] = 5000
    # Add offline processing config to obs config (normally done in run method, but that is skipped)
    config.update(proc.config)

    # start worker observation 
    try:
        proc._start_observation_worker(config)
    except Exception as e:
        logger.error("Unhandled exception in offline processing: {}".format(e))
        return


if __name__ == '__main__':
    output_dir = '/tank/users/oostrum/iquv/B0531/output_I'
    amber_dir = os.path.join(output_dir, 'amber')
    result_dir = os.path.join(output_dir, 'results')

    duration = 300.032
    startpacket = int((Time.now().unix - duration) * 781250)
    #endtime = Time.now().datetime.strftime('%Y-%m-%d %H:%M:%S')

    conf = {'ntabs': 12, 'beam': 0, 'mode': 'TAB', 'amber_dir': amber_dir,
            'output_dir': output_dir, 'duration': 300.032, 
            'startpacket': startpacket, 'result_dir': result_dir}

    run_processing(**conf)
