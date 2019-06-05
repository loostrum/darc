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
        try:
            output = out_queue.get(timeout=5)
        except Empty:
            self.fail("No clusters received")

        # stop the clustering
        stop_event.set()

        expected_output = {'clusters': np.array([[1.84666e+01, 5.66000e+01, 2.44941e-02, 1.00000e+00],
                            [1.03451e+01, 8.00000e+01, 9.01120e-01, 1.00000e+03],
                            [1.08202e+01, 4.78000e+01, 2.94912e+00, 1.00000e+03],
                            [1.43720e+01, 4.16000e+01, 9.01120e-01, 1.00000e+03],
                            [1.57447e+01, 1.58000e+01, 2.94912e+00, 1.00000e+03]]), 
                            'columns': {'SNR': 0, 'DM': 1, 'time': 2, 'integration_step': 3}}
        
        # test columns are equal
        self.assertDictEqual(output['columns'], expected_output['columns'])
        # test clusters are equal
        np.testing.assert_array_equal(output['clusters'], expected_output['clusters'])

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
