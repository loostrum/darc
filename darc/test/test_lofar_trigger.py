#!/usr/bin/env python

import os
import unittest
from time import sleep
import socket
import struct
import errno

from darc.lofar_trigger import LOFARTrigger, LOFARTriggerQueueServer


class TestLOFARTrigger(unittest.TestCase):


    def test_lofar_trigger(self):
        """
        Test whether the LOFAR Trigger module converts and incoming trigger to a struct

        and sends it
        """
        # init LOFAR trigger
        lofar_trigger = LOFARTrigger()
        # overwrite server location
        lofar_trigger.server_host = 'localhost'
        # overwrite lofar server location
        lofar_trigger.lofar_host = 'localhost'
        # set event seding to true
        lofar_trigger.send_events = True
        # start the service
        lofar_trigger.start()
        sleep(1)

        # open a listening socket for events
        timeout = 5
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", lofar_trigger.lofar_port))
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))

        # create two triggers, lofar_trigger should pick highest S/N
        triggers = [{'dm': 56.791, 'utc': '2019-01-02T18:00:00.0', 'nu_GHz': 1.37,
                     'snr': 20, 'cb': 0, 'test': True},
                    {'dm': 60.123, 'utc': '2019-01-02T18:00:00.0', 'nu_GHz': 1.37,
                     'snr': 15, 'cb': 17, 'test': True}]
        # expected output
        fmt = '>cciHHc'
        # \x99, \xA0, tstop_s, tstop_ms, dm*10, test_flag)
        tstop_s = 1546452008
        tstop_ms = 288
        dm_int = 568
        expected_event = (b'\x99', b'\xA0', tstop_s, tstop_ms, dm_int, b'\x54')


        # get the queue
        LOFARTriggerQueueServer.register('get_queue')
        queue_server = LOFARTriggerQueueServer(address=(lofar_trigger.server_host,
                                                        lofar_trigger.server_port), 
                                               authkey=lofar_trigger.server_auth.encode())
        queue_server.connect()
        queue = queue_server.get_queue()
        # send the triggers
        for trigger in triggers:
            queue.put(trigger)
        try:
            msg, adr = sock.recvfrom(1024)
        except socket.timeout:
            self.fail("Did not receive event within {} seconds".format(timeout))

        # close the socket
        sock.close()

        # unpack the LOFAR trigger
        lofar_event = struct.unpack(fmt, msg)

        # stop lofar trigger module
        lofar_trigger.stop()

        self.assertTupleEqual(lofar_event, expected_event)



if __name__ == '__main__':
    unittest.main()
