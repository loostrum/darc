#!/usr/bin/env python3

import os
import unittest
from unittest import mock
import multiprocessing as mp
from time import sleep
from astropy.time import Time
from queue import Empty

from darc import AMBERClustering
from darc import util
from darc.definitions import TIME_UNIT


class TestAMBERClustering(unittest.TestCase):

    # set YMW16 (which is not installed in CI) to give high DM value,
    # avoiding that new source triggering finds something in this test
    @mock.patch('darc.amber_clustering.util.get_ymw16',
                return_value=10000.)
    def test_clustering_iquv(self, *mocks):
        """
        Test AMBER clustering without applying thresholds, for a known source
        """

        # create queues
        in_queue = mp.Queue()
        out_queue = mp.Queue()
        # init AMBER Clustering
        clustering = AMBERClustering(connect_vo=False, connect_lofar=False,
                                     source_queue=in_queue, target_queue=out_queue)
        # ensure clustering settings are the same as when the below expected output was calculated
        clustering.clustering_window = 1.0
        clustering.dm_range = 10
        clustering.snr_min_global = 10
        clustering.sb_filter = False
        clustering.interval = 0.5
        clustering.thresh_iquv['interval'] = 0.0  # always send IQUV triggers

        # overwrite source list location
        clustering.source_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'source_list.yaml')

        # start the clustering
        clustering.start()

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
        # use known source triggering
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
        obs_config = {'startpacket': int(utc_start.unix * TIME_UNIT), 'min_freq': 1219.70092773,
                      'beam': beam, 'parset': parset_enc, 'datetimesource': '2020-01-01T00:00:00.B0531+21'}
        in_queue.put({'command': 'start_observation', 'obs_config': obs_config, 'reload_conf': False})

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
        in_queue.put('stop')

        expected_keys = ('stokes', 'dm', 'beam', 'width', 'snr', 'time', 'utc_start')

        # the exact output is not always the same because it depends on wall time intervals between triggers,
        # so check if the result is in the expected range
        self.assertTrue(len(output) > 5)
        self.assertTrue(len(output) < 20)
        # test all expected keys are present in all clusters
        for cluster in output:
            for key in expected_keys:
                self.assertTrue(key in cluster)


if __name__ == '__main__':
    unittest.main()
