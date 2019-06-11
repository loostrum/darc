#!/usr/bin/env python

import os
import sys
import unittest
import multiprocessing as mp
from textwrap import dedent
from time import sleep
from astropy.time import Time, TimeDelta
import socket
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from darc.dada_trigger import DADATrigger


class TestDADATrigger(unittest.TestCase):

    @staticmethod
    def get_trigger(window_size, stokes):
        """
        Generate a trigger dict
        :param: window_size: event duration in seconds
        :param: stokes: I or IQUV
        :return: trigger (dict), event (str)
        """

        utc_start = Time("2019-01-01 12:00:00")
        utc_start_str = utc_start.iso
        time = 38.249

        # set network port (values from arts_survey_control.conf)
        if stokes == 'I':
            port = 30000
        elif stokes == 'IQUV':
            port = 30001
        else:
            self.fail("Unknown stokes mode: {}".format(stokes))

        # trigger
        trigger = {'dm': 56.791, 'snr': 15.2, 'width': 2, 'beam': 22, 'time': time,
                   'window_size': 10.24, 'utc_start': utc_start, 'stokes': stokes, 'port': port}

        # event parameters
        event_start_full = utc_start + TimeDelta(time, format='sec') - TimeDelta(window_size/2, format='sec')
        event_end_full = event_start_full + TimeDelta(window_size, format='sec')

        event_start, event_start_frac = event_start_full.iso.split('.')
        #event_start_frac = '.' + event_start_frac
        event_end, event_end_frac = event_end_full.iso.split('.')
        #event_end_frac = '.' + event_end_frac

        event_info = trigger.copy()
        event_info['event_start'] = event_start
        event_info['event_start_frac'] = event_start_frac
        event_info['event_end'] = event_end
        event_info['event_end_frac'] = event_end_frac
        
        event = dedent("""\
                       N_EVENTS 1
                       {utc_start}
                       {event_start} {event_start_frac} {event_end} {event_end_frac} {snr} {dm} {width} {beam}
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
        trigger_i, event_i = self.get_trigger(dadatrigger.window_size_i, stokes='I')
        trigger_iquv, event_iquv = self.get_trigger(dadatrigger.window_size_iquv, stokes='IQUV')
        # open a listening socket for stokes I events
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", trigger_i['port']))
            sock.listen(5)
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))

        # send the stokes I trigger
        queue.put({'command': 'trigger', 'trigger': trigger_i})
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
            sock.bind(("", trigger_iquv['port']))
            sock.listen(5)
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))
        # send the stokes IQUV trigger
        queue.put({'command': 'trigger', 'trigger': trigger_iquv})
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
