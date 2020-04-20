#!/usr/bin/env python

import os
import sys
import unittest
import multiprocessing as mp
from textwrap import dedent
import errno
from time import sleep
from astropy.time import Time, TimeDelta
import socket
from queue import Empty

from darc.dada_trigger import DADATrigger


class TestDADATrigger(unittest.TestCase):

    def get_trigger(self, stokes, min_window_size, delay_end):
        """
        Generate a trigger dict
        :param: stokes: I or IQUV
        :return: trigger (dict), event (str)
        """

        utc_start = Time("2019-01-01 12:00:00")
        time = 38.249

        # trigger
        dm = 56.791
        trigger = {'dm': dm, 'snr': 15.2, 'width': 2, 'beam': 22, 'time': time,
                   'utc_start': utc_start, 'stokes': stokes}

        # event parameters
        # dm delay is less than the minimum trigger duration, so start/end time do not need to take DM into account
        shift = 2.048
        event_start_full = utc_start + TimeDelta(time, format='sec') - TimeDelta(shift, format='sec')
        event_end_full = event_start_full + TimeDelta(min_window_size + delay_end + shift, format='sec')

        event_start, event_start_frac = event_start_full.iso.split('.')
        event_end, event_end_frac = event_end_full.iso.split('.')

        event_info = trigger.copy()
        event_info['utc_start'] = trigger['utc_start'].iso.replace(' ', '-')
        event_info['event_start'] = event_start.replace(' ', '-')
        event_info['event_start_frac'] = event_start_frac
        event_info['event_end'] = event_end.replace(' ', '-')
        event_info['event_end_frac'] = event_end_frac

        event = dedent("""\
                       N_EVENTS 1
                       {utc_start}
                       {event_start} {event_start_frac} {event_end} {event_end_frac} {dm} {snr} {width} {beam}
                       """.format(**event_info))
        return trigger, event

    def test_triggers(self):
        """
        Generate a stokes I and IQUV event
        """
        timeout = 5

        # create input queue
        queue = mp.Queue()
        # init DADA Trigger
        dadatrigger = DADATrigger()
        # set the queue
        dadatrigger.set_source_queue(queue)
        # start dadatrigger
        dadatrigger.start()

        # get trigger dict and event
        trigger_i, event_i = self.get_trigger('I', dadatrigger.min_window_size, dadatrigger.delay_end)
        trigger_iquv, event_iquv = self.get_trigger('IQUV', dadatrigger.min_window_size, dadatrigger.delay_end)
        # open a listening socket for stokes I events
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", dadatrigger.port_i))
            sock.listen(5)
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))

        # send the stokes I trigger
        queue.put({'command': 'trigger', 'trigger': [trigger_i]})
        try:
            client, adr = sock.accept()
        except socket.timeout:
            self.fail("Did not receive event within {} seconds".format(timeout))

        # receive event. Work around bug on MAC
        if sys.platform == 'Darwin':
            received = False
            while not received:
                try:
                    out_event = client.recv(1024).decode()
                    received = True
                except socket.error as e:
                    if e.errno == errno.EAGAIN:
                        sleep(.1)
                    else:
                        raise
        else:
            out_event = client.recv(1024).decode()
        # close the socket
        sock.close()

        in_event_split = event_i.strip().split('\n')
        out_event_split = out_event.strip().split('\n')
        self.assertListEqual(in_event_split, out_event_split)

        # open a listening socket for stokes IQUV events
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", dadatrigger.port_iquv))
            sock.listen(5)
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))
        # send the stokes IQUV trigger
        queue.put({'command': 'trigger', 'trigger': [trigger_iquv]})
        try:
            client, adr = sock.accept()
        except socket.timeout:
            self.fail("Did not receive event within {} seconds".format(timeout))

        # receive event. Work around bug on MAC
        if sys.platform == 'Darwin':
            received = False
            while not received:
                try:
                    out_event = client.recv(1024).decode()
                    received = True
                except socket.error as e:
                    if e.errno == errno.EAGAIN:
                        sleep(.1)
                    else:
                        raise
        else:
            out_event = client.recv(1024).decode()
        # close the socket
        sock.close()

        in_event_split = event_iquv.strip().split('\n')
        out_event_split = out_event.strip().split('\n')
        self.assertListEqual(in_event_split, out_event_split)

        # stop dadatrigger
        dadatrigger.stop()


if __name__ == '__main__':
    unittest.main()
