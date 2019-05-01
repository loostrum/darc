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

        # create output queue
        queue = mp.Queue()
        # create stop event for listener
        stop_event = threading.Event()
        # init AMBER Listener
        listener = AMBERListener(stop_event)
        # set the queue
        listener.set_target_queue(queue)
        # start the listener
        listener.start()
        # send triggers to network port (ToDo: implement sending in python)
        nline_to_send = 50
        trigger_file = 'CB00_step1.trigger'
        cmd = "head -n {} {} | nc localhost {}".format(nline_to_send, trigger_file, listener.port)
        os.system(cmd)
        # check they arrived at output queue
        sleep(5)
        try:
            output = listener.queue.get(timeout=5)
        except Empty:
            output = []

        # stop the listener
        stop_event.set()

        # check if there is any output at all
        self.assertTrue(len(output) > 0)

        # check the output is correct, i.e. equal to input
        line = 0
        with open(trigger_file, 'r') as f:
            triggers = f.readlines()
        if len(triggers) > nline_to_send:
            triggers = triggers[:nline_to_send]
        triggers = [line.strip() for line in triggers]

        self.assertEqual(output, triggers)

if __name__ == '__main__':
    unittest.main()
