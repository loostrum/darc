#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
from time import sleep
import numpy as np
from astropy.time import Time
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from darc.amber_clustering import AMBERClustering


class TestAMBERClustering(unittest.TestCase):

    def test_clusters_without_thresholds(self):
        """
        Test AMBER clustering without applying thresholds
        """

        # create queues
        in_queue = mp.Queue()
        out_queue = mp.Queue()
        # init AMBER Clustering
        clustering = AMBERClustering()
        # set the queues
        clustering.set_source_queue(in_queue)
        clustering.set_target_queue(out_queue)
        # remove tresholds
        clustering.age_max = np.inf
        clustering.dm_min = 0
        clustering.dm_max = np.inf
        clustering.snr_min = 0

        # start the clustering
        clustering.start()

        # load triggers to put on queue
        nline_to_check = 50
        trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'CB00_step1.trigger')
        with open(trigger_file, 'r') as f:
            triggers = f.readlines()
        triggers = [line.strip() for line in triggers]
        if len(triggers) > nline_to_check:
            triggers = triggers[:nline_to_check]

        # start observation
        utc_start = Time.now()
        obs_config = {'startpacket': int(utc_start.unix*781250), 'min_freq': 1219.70092773,
                      'network_port_event_i': 30000}
        in_queue.put({'command': 'start_observation', 'obs_config': obs_config})

        # put triggers on queue
        for trigger in triggers:
            in_queue.put({'command': 'trigger', 'trigger': trigger})
        # get output
        sleep(clustering.interval + 5)
        output = []
        while True:
            try:
                output.append(out_queue.get(timeout=5))
            except Empty:
                break
        if not output:
            self.fail("No clusters received")

        # stop clustering
        clustering.stop()

        expected_output = [{'stokes': 'I', 'dm': 56.6, 'port': 30000, 'beam': 0, 'width': 1.0, 'window_size': 1.024,
                            'snr': 18.4666, 'time': 0.0244941},
                           {'stokes': 'I', 'dm': 80.0, 'port': 30000, 'beam': 2, 'width': 1000.0, 'window_size': 1.024,
                            'snr': 10.3451, 'time': 0.90112},
                           {'stokes': 'I', 'dm': 47.8, 'port': 30000, 'beam': 3, 'width': 1000.0, 'window_size': 1.024,
                            'snr': 10.8202, 'time': 2.94912},
                           {'stokes': 'I', 'dm': 41.6, 'port': 30000, 'beam': 3, 'width': 1000.0, 'window_size': 1.024,
                            'snr': 14.372, 'time': 0.90112},
                           {'stokes': 'I', 'dm': 15.8, 'port': 30000, 'beam': 4, 'width': 1000.0, 'window_size': 1.024,
                            'snr': 15.7447, 'time': 2.94912}]

        # test all clusters are there
        self.assertEqual(len(output), len(expected_output))
        # test clusters are equal
        for ind, expected_cluster in enumerate(expected_output):
            cluster = output[ind]['trigger']
            # remove utc_start because that cannot be controlled yet
            del cluster['utc_start']
            self.assertDictEqual(cluster, expected_cluster)

    def test_clusters_with_thresholds(self):
        """
        Test AMBER clustering with applying thresholds
        """

        # create queues
        in_queue = mp.Queue()
        out_queue = mp.Queue()
        # init AMBER Clustering
        clustering = AMBERClustering()
        # set the queues
        clustering.set_source_queue(in_queue)
        clustering.set_target_queue(out_queue)

        # set max age to -inf, so zero triggers are approved
        clustering.age_max = -np.inf

        # start the clustering
        clustering.start()

        # load triggers to put on queue
        nline_to_check = 50
        trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'CB00_step1.trigger')
        # check the output is correct, i.e. equal to input
        line = 0
        with open(trigger_file, 'r') as f:
            triggers = f.readlines()
        if len(triggers) > nline_to_check:
            triggers = triggers[:nline_to_check]
        triggers = [line.strip() for line in triggers]

        # start observation
        obs_config = {'startpacket': int(Time.now().unix*781250), 'min_freq': 1219.70092773,
                      'network_port_event_i': 30000}
        in_queue.put({'command': 'start_observation', 'obs_config': obs_config})

        # put triggers on queue
        for trigger in triggers:
            in_queue.put({'command': 'trigger', 'trigger': trigger})
        # get output
        sleep(clustering.interval + 5)
        try:
            output = out_queue.get(timeout=5)
        except Empty:
            output = []

        # stop clustering
        clustering.stop()

        # with thresholds, none of the triggers are ok
        self.assertEqual(output, [])


if __name__ == '__main__':
    unittest.main()
