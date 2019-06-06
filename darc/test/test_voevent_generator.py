#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
import threading
from time import sleep

from darc.voevent_generator import VOEventGenerator, VOEventQueueServer


class TestVOEventGenerator(unittest.TestCase):

    def test_generate_VOEvent(self):
        """
        Test that the VOEvent generator converts a trigger into a VOEvent
        """

        # create stop event for generator
        stop_event = threading.Event()
        # init VOEvent Generator
        generator = VOEventGenerator(stop_event)
        # overwrite server location
        generator.server_host = 'localhost'
        # start the generator
        generator.start()
        # create a trigger
        trigger = {'dm': 56.791, 'dm_err': .2, 'width': 2.5, 'snr': 25, 'flux': 0.5,
                   'ra': 83.63322083333333, 'dec': 22.01446111111111, 'ymw16': 0,
                   'semiMaj': 15., 'semiMin': 15., 'name': 'B0531+21',
                   'importance': 0.1, 'utc': '2019-01-01-18:00:00.0'}

        sleep(1)

        # get the queue
        VOEventQueueServer.register('get_queue')
        queue_server = VOEventQueueServer(address=(generator.server_host, generator.server_port), 
                                          authkey=generator.server_auth.encode())
        queue_server.connect()
        queue = queue_server.get_queue()
        # send the trigger
        queue.put(trigger)
        # wait and stop
        sleep(5)
        stop_event.set()
        # check the output file
        filename = os.path.join(generator.voevent_dir, "{}.xml".format(trigger['utc']))

        self.assertTrue(os.path.isfile(filename))

        # remove output file
        os.remove(filename)


if __name__ == '__main__':
    unittest.main()
