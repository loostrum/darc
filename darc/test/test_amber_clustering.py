#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
from time import sleep
from astropy.time import Time
from queue import Empty

from darc.amber_clustering import AMBERClustering
from darc.voevent_generator import VOEventGenerator, VOEventQueueServer

from darc import util


class TestAMBERClustering(unittest.TestCase):

    def test_clustering_iquv(self):
        """
        Test AMBER clustering without applying thresholds
        """

        # create queues
        in_queue = mp.Queue()
        out_queue = mp.Queue()
        # init AMBER Clustering
        clustering = AMBERClustering(connect_vo=False)
        # set the queues
        clustering.set_source_queue(in_queue)
        clustering.set_target_queue(out_queue)

        # start the clustering
        clustering.start()

        # overwrite source list location
        clustering.source_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'source_list.yaml')

        # load triggers to put on queue
        nline_to_check = 100
        trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'CB00_step1.trigger')
        with open(trigger_file, 'r') as f:
            triggers = f.readlines()
        triggers = [line.strip() for line in triggers]
        if len(triggers) > nline_to_check:
            triggers = triggers[:nline_to_check]

        # start observation
        # create input parset with required keys
        # source has to match trigger file DMs
        beam = 0
        parset_dict = {'task.source.name': 'B0531+21',
                       'task.beamSet.0.compoundBeam.{}.phaseCenter'.format(beam): '[83.633deg, 22.0144deg]',
                       'task.directionReferenceFrame': 'J2000'}
        # encode parset
        parset_str = ''
        for k, v in parset_dict.items():
            parset_str += '{}={}\n'.format(k, v)
        parset_enc = util.encode_parset(parset_str)

        utc_start = Time.now()
        obs_config = {'startpacket': int(utc_start.unix*781250), 'min_freq': 1219.70092773,
                      'beam': beam, 'parset': parset_enc}
        in_queue.put({'command': 'start_observation', 'obs_config': obs_config})

        # put triggers on queue
        for trigger in triggers:
            in_queue.put({'command': 'trigger', 'trigger': trigger})
        # get output
        sleep(clustering.interval + 5)
        output = []
        while True:
            try:
                output.extend(out_queue.get(timeout=5)['trigger'])
            except Empty:
                break
        if not output:
            self.fail("No clusters received")

        # stop clustering
        in_queue.put({'command': 'stop_observation'})
        clustering.stop()

        expected_output = [{'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 18.4666, 'time': 0.0244941},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 3, 'width': 1000, 'snr': 10.8202, 'time': 2.94912},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 29.6098, 'time': 3.46857},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 25.0833, 'time': 4.58277},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 9, 'width': 1000, 'snr': 11.173, 'time': 4.99712},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 10, 'width': 1000, 'snr': 15.1677, 'time': 6.02112},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 11.3797, 'time': 8.33061},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 10.4506, 'time': 10.0863},
                           {'stokes': 'IQUV', 'dm': 56.8, 'beam': 0, 'width': 1, 'snr': 19.8565, 'time': 11.0317}]

        self.assertEqual(len(output), len(expected_output))
        # test clusters are equal
        for ind, expected_cluster in enumerate(expected_output):
            cluster = output[ind]
            # remove utc_start because that cannot be controlled yet
            del cluster['utc_start']
            self.assertDictEqual(cluster, expected_cluster)

    def test_clustering_lofar(self):
        """
        Test clustering and sending LOFAR trigger to VOEvent Generator
        """
        port = 52000
        auth = 'AAAA'

        # init VOEvent Generator
        generator = VOEventGenerator()
        # ensure it does not send events
        generator.send_events = False
        # overwrite port and auth
        generator.server_port = port
        generator.server_auth = auth
        # start the generator
        generator.start()

        # init AMBER clustering
        # create queues
        in_queue = mp.Queue()
        out_queue = mp.Queue()
        # setup clustering
        clustering = AMBERClustering()
        # set the queues
        clustering.set_source_queue(in_queue)
        clustering.set_target_queue(out_queue)
        # overwrite VO connector

        def connector():
            # Load VO server settings
            VOEventQueueServer.register('get_queue')
            server = VOEventQueueServer(address=('localhost', port), authkey=auth.encode())
            server.connect()
            return server.get_queue()
        clustering.voevent_connector = connector

        # start the clustering
        clustering.start()

        # overwrite source list location
        clustering.source_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'source_list.yaml')

        # start observation
        # create input parset with required keys
        beam = 0
        parset_dict = {'task.source.name': 'B0531+21',
                       'task.beamSet.0.compoundBeam.{}.phaseCenter'.format(beam): '[83.633deg, 22.0144deg]',
                       'task.directionReferenceFrame': 'J2000'}
        # encode parset
        parset_str = ''
        for k, v in parset_dict.items():
            parset_str += '{}={}\n'.format(k, v)
        parset_enc = util.encode_parset(parset_str)

        utc_start = Time('2019-01-01T12:00:00', scale='utc', format='isot')
        obs_config = {'startpacket': int(utc_start.unix * 781250), 'min_freq': 1219.70092773,
                      'beam': beam, 'parset': parset_enc}
        in_queue.put({'command': 'start_observation', 'obs_config': obs_config})

        # put trigger on queue that passes LOFAR thresholds
        triggers = ['# beam_id batch_id sample_id integration_step compacted_integration_steps '
                    'time DM_id DM compacted_DMs SNR',
                    '0 0 299 1 2 0.0244941 283 56.6 10 100']
        for trigger in triggers:
            in_queue.put({'command': 'trigger', 'trigger': trigger})

        # wait, the stop observation
        sleep(clustering.interval + 5)
        in_queue.put({'command': 'stop_observation'})
        clustering.stop()

        # stop voevent generator
        generator.stop()
        filename = os.path.join(generator.voevent_dir, '2019-01-01T12:00:00.024.xml')
        self.assertTrue(os.path.isfile(filename))
        # remove output file
        os.remove(filename)


if __name__ == '__main__':
    unittest.main()
