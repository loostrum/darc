#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
import threading
from time import sleep
import numpy as np
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
        # create stop event for clustering
        stop_event = threading.Event()
        # init AMBER Clustering
        clustering = AMBERClustering(stop_event)
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

        # put triggers on queue
        for trigger in triggers:
            in_queue.put(trigger)
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

        # stop the clustering
        stop_event.set()

        expected_output = [{'stokes': 'I', 'dm': 56.6, 'beam': 1.0, 'width': 1.0, 'snr': 18.4666, 'time': 0.0244941},
                            {'stokes': 'I', 'dm': 80.0, 'beam': 1.0, 'width': 1000.0, 'snr': 10.3451, 'time': 0.90112},
                            {'stokes': 'I', 'dm': 47.8, 'beam': 1.0, 'width': 1000.0, 'snr': 10.8202, 'time': 2.94912},
                            {'stokes': 'I', 'dm': 41.6, 'beam': 1.0, 'width': 1000.0, 'snr': 14.372, 'time': 0.90112},
                            {'stokes': 'I', 'dm': 15.8, 'beam': 1.0, 'width': 1000.0, 'snr': 15.7447, 'time': 2.94912}]

        # test all clusters are there
        self.assertEqual(len(output), len(expected_output))
        # test clusters are equal
        for ind, expected_cluster in enumerate(expected_output):
            cluster = output[ind]
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
        # create stop event for clustering
        stop_event = threading.Event()
        # init AMBER Clustering
        clustering = AMBERClustering(stop_event)
        # set the queues
        clustering.set_source_queue(in_queue)
        clustering.set_target_queue(out_queue)

        # set max age to -1, so zero trigger are approved
        clustering.age_max = -1

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

        # put triggers on queue
        for trigger in triggers:
            in_queue.put(trigger)
        # get output
        sleep(clustering.interval + 5)
        try:
            output = out_queue.get(timeout=5)
        except Empty:
            output = []

        # stop the clustering
        stop_event.set()

        # with thresholds, none of the triggers are ok
        self.assertEqual(output, [])


if __name__ == '__main__':
    unittest.main()
