#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
import threading
from time import sleep
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from darc.amber_listener import AMBERListener


class TestAMBERListener(unittest.TestCase):

    def test_trigger_set(self):
        """
        Check whether a set of triggers + header is put on the queue correctly
        """

        # create input queue
        obs_queue = mp.Queue()
        # create output queue
        amber_queue = mp.Queue()
        # init AMBER Listener
        listener = AMBERListener()
        # set the queues
        listener.set_source_queue(obs_queue)
        listener.set_target_queue(amber_queue)
        # start the listener
        listener.start()
        # start observation
        amber_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amber.conf')
        obs_config = {'amber_dir': os.path.dirname(os.path.abspath(__file__)),
                      'beam': 0,
                      'amber_config': amber_conf_file}
        command = {'command': 'start_observation', 'obs_config': obs_config}
        obs_queue.put(command)

        output = []
        # read queue until empty
        i = 0
        while True:
            try:
                raw_trigger = amber_queue.get(timeout=2)
            except Empty:
                break
            output.append(raw_trigger['trigger'])

        # stop the observation
        obs_queue.put({'command': 'stop_observation'})

        # stop the listener
        listener.stop()

        # check if there is any output at all
        self.assertTrue(len(output) > 0)

        # check the output is correct, i.e. equal to input
        # load all trigger files
        all_triggers = []
        for step in [1, 2, 3]:
            trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'CB00_step{}.trigger'.format(step))
            with open(trigger_file, 'r') as f:
                triggers = f.readlines()
            triggers = [line.strip() for line in triggers]
            all_triggers.extend(triggers)

        # sort input and output by last element (S/N)
        output.sort()
        all_triggers.sort()
        self.assertEqual(output, all_triggers)


if __name__ == '__main__':
    unittest.main()
