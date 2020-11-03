#!/usr/bin/env python3

import logging
import unittest
import multiprocessing as mp
import socket
from astropy.time import Time, TimeDelta
from darc import ProcessorMaster

# disable debug log messages from matplotlib
logging.getLogger('matplotlib').setLevel(logging.ERROR)


@unittest.skipUnless(socket.gethostname() in ('arts041', 'zeus'), "Test can only run on arts041 or zeus")
class TestProcessorMaster(unittest.TestCase):
    def test_processor_master(self):
        if socket.gethostname() == 'zeus':
            result_dir = '/data/arts/darc/results'
        else:
            self.fail("Test not yet implemented on arts041")

        tstart = Time.now()
        duration = TimeDelta(5, format='sec')

        parset = {'task.duration': duration,
                  'task.startTime': tstart.isot,
                  'task.stopTime': (tstart + duration).isot,
                  'task.taskID': '001122',
                  'task.beamSet.0.compoundBeam.0.phaseCenter': '[293.94876deg, 16.27778deg]',
                  'task.directionReferenceFrame': 'J2000'}

        obs_config = {'beams': [0],
                      'date': '20200101',
                      'datetimesource': '2020-01-01-00:00:00.FAKE',
                      'result_dir': result_dir,
                      'parset': parset}

        # init processor master
        self.processor = ProcessorMaster()
        self.processor.set_source_queue(mp.Queue())
        self.processor.start()

        # start observation
        self.processor.start_observation(obs_config, reload=False)


if __name__ == '__main__':
    unittest.main()
