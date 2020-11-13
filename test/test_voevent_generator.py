#!/usr/bin/env python3

import os
import unittest
from time import sleep
import multiprocessing as mp

from darc import VOEventGenerator, VOEventQueueServer


class TestVOEventGenerator(unittest.TestCase):

    def test_generate_VOEvent(self):
        """
        Test that the VOEvent generator converts a trigger into a VOEvent
        """
        # init VOEvent Generator
        control_queue = mp.Queue()
        generator = VOEventGenerator(control_queue)
        # overwrite server location
        generator.server_host = 'localhost'
        # events are only generated if send = True
        generator.send_events = True
        # start the generator
        generator.start()
        sleep(1)
        # create two triggers, generator should pick highest S/N
        # define utc of trigger with highest S/N, which is also in the VOEvent filename
        trigger_utc = '2019-01-02T18:00:00.0'
        triggers = [{'dm': 56.791, 'dm_err': .2, 'width': 2.5, 'snr': 10, 'flux': 0.5,
                     'ra': 83.63322083333333, 'dec': 22.01446111111111, 'ymw16': 0,
                     'semiMaj': 15., 'semiMin': 15., 'name': 'B0531+21', 'cb': 0,
                     'importance': 0.1, 'utc': '2019-01-01T18:00:00.0', 'test': True},
                    {'dm': 56.791, 'dm_err': .2, 'width': 2.5, 'snr': 50, 'flux': 0.5,
                     'ra': 83.63322083333333, 'dec': 22.01446111111111, 'ymw16': 0,
                     'semiMaj': 15., 'semiMin': 15., 'name': 'B0531+21', 'cb': 17,
                     'importance': 0.1, 'utc': trigger_utc, 'test': True}]

        # get the queue
        VOEventQueueServer.register('get_queue')
        queue_server = VOEventQueueServer(address=(generator.server_host, generator.server_port),
                                          authkey=generator.server_auth.encode())
        queue_server.connect()
        queue = queue_server.get_queue()
        # send the triggers
        for trigger in triggers:
            queue.put(trigger)
        # wait and stop
        sleep(5)
        control_queue.put('stop')
        generator.join()
        # check the output file
        filename = os.path.join(generator.voevent_dir, "{}.xml".format(trigger_utc))

        self.assertTrue(os.path.isfile(filename))

        # remove output file
        os.remove(filename)


if __name__ == '__main__':
    unittest.main()
