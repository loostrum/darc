#!/usr/bin/env python

import os
import unittest
from time import sleep
import socket
import struct
import errno
import multiprocessing as mp

import numpy as np
from astropy.time import Time

from darc.amber_clustering import AMBERClustering
from darc.lofar_trigger import LOFARTrigger, LOFARTriggerQueueServer
from darc import util


class TestLOFARTrigger(unittest.TestCase):

    def test_lofar_trigger(self):
        """
        Test whether the LOFAR Trigger module converts and incoming trigger to a struct

        and sends it
        """
        # init LOFAR trigger
        lofar_trigger = LOFARTrigger()
        # overwrite server location and port
        lofar_trigger.server_host = 'localhost'
        lofar_trigger.server_port = 9999
        # overwrite lofar server location
        lofar_trigger.lofar_host = 'localhost'
        # set event sending to true
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
                     'snr': 20, 'cb': 0, 'name': 'candidate', 'test': True},
                    {'dm': 60.123, 'utc': '2019-01-02T18:00:00.0', 'nu_GHz': 1.37,
                     'snr': 15, 'cb': 17, 'name': 'candidate', 'test': True}]
        # expected output
        fmt = '>cciHHc'
        # \x99, \xA0, tstop_s, tstop_ms, dm*10, test_flag)
        tstop_s = 1546452008
        tstop_ms = 265
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
        # receive the LOFAR event
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

    def test_internal_trigger(self):
        """
        Test LOFAR triggering through AMBERClustering module
        """

        # init LOFAR trigger
        lofar_trigger = LOFARTrigger()
        # overwrite server location and port
        lofar_trigger.server_host = 'localhost'
        lofar_trigger.server_port = 9990
        # overwrite lofar server location
        lofar_trigger.lofar_host = 'localhost'
        # set event sending to true
        lofar_trigger.send_events = True
        # start the service
        lofar_trigger.start()
        sleep(1)

        # init amber clustering
        # do not connect to VOEvent server nor LOFAR trigger system upon init
        clustering = AMBERClustering(connect_vo=False, connect_lofar=False)
        clustering.set_source_queue(mp.Queue())
        clustering.set_target_queue(mp.Queue())
        clustering.sb_filter = False
        clustering.start()

        # get the LOFAR trigger queue to connect to AMBERClustering
        LOFARTriggerQueueServer.register('get_queue')
        queue_server = LOFARTriggerQueueServer(address=(lofar_trigger.server_host,
                                                        lofar_trigger.server_port),
                                               authkey=lofar_trigger.server_auth.encode())
        queue_server.connect()
        queue = queue_server.get_queue()

        # connect AMBERClustering to LOFARTrigger
        clustering.lofar_queue = queue
        clustering.have_lofar = True

        # overwrite source list location
        clustering.source_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'source_list.yaml')

        # open a listening socket for LOFAR events
        timeout = 5
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", lofar_trigger.lofar_port))
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))

        # run three observations: galactic source, extragalactic source, new source
        # YMW16 galactic DM = 291
        # galactic source in source list: DM = 10
        # extragalactic source in source list: DM = 1000
        # trigger DMs should be within dm_range (in config.yaml) of these values
        galactic = {'source': 'galactic', 'snr': 100, 'dm': 11.5, 'downsamp': 10, 'time': 5.0}
        extragalactic = {'source': 'extragalactic', 'snr': 100, 'dm': 999.5, 'downsamp': 10, 'time': 5.0}
        new = {'source': 'new', 'snr': 100, 'dm': 2500.5, 'downsamp': 10, 'time': 5.0}
        for i, trigger in enumerate([galactic, extragalactic, new]):
            beam = 0
            parset_dict = {'task.source.name': trigger['source'],
                           'task.beamSet.0.compoundBeam.{}.phaseCenter'.format(beam): '[83.633deg, 22.0144deg]',
                           'task.directionReferenceFrame': 'J2000'}
            # encode parset
            parset_str = ''
            for k, v in parset_dict.items():
                parset_str += '{}={}\n'.format(k, v)
            parset_enc = util.encode_parset(parset_str)

            utc_start = Time.now()
            obs_config = {'startpacket': int(utc_start.unix * 781250), 'min_freq': 1219.70092773,
                          'beam': beam, 'parset': parset_enc, 'datetimesource': '2020-01-01T00:00:00.FAKE'}
            # start observation
            clustering.source_queue.put({'command': 'start_observation', 'obs_config': obs_config})
            sleep(.1)

            # put header and trigger on the input queue
            amber_trigger = ['# beam_id batch_id sample_id integration_step compacted_integration_steps time DM_id DM compacted_DMs SNR',
                             '35 0 0 {downsamp} 1 {time} 1 {dm} 1 {snr}'.format(**trigger)]
            for t in amber_trigger:
                clustering.source_queue.put({'command': 'trigger', 'trigger': t})

            # receive the LOFAR event
            try:
                msg, adr = sock.recvfrom(1024)
            except socket.timeout:
                self.fail("Did not receive event within {} seconds".format(timeout))

            # compare to expected event
            # time is the LOFAR buffer freeze time, which is already tested by test_lofar_trigger
            # here, just check the DM to verify the correct trigger was received
            # event format is
            # \x99, \xA0, tstop_s, tstop_ms, dm*10, test_flag)
            fmt = '>cciHHc'
            dm_int = struct.unpack(fmt, msg)[4]
            # convert input DM to same format
            # input DM is source DM for known sources
            if trigger != new:
                # read the source DM as read by AMBERClustering
                input_dm, src_type, src_name = clustering._get_source()
            else:
                # the input DM is the trigger DM
                input_dm = trigger['dm']
            # convert to integer of 10 times DM, as used by LOFAR
            input_dm = int(np.round(10 * input_dm))
            self.assertEqual(input_dm, dm_int)

            # stop observation
            clustering.source_queue.put({'command': 'stop_observation'})

            sleep(.5)

        # close the socket
        sock.close()


if __name__ == '__main__':
    unittest.main()
