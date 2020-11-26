#!/usr/bin/env python3

import os
import logging
import unittest
import multiprocessing as mp
import shutil
import socket
from astropy.time import Time, TimeDelta
import yaml

from darc import ProcessorMaster
from darc import util
from darc.definitions import TIME_UNIT

# disable debug log messages from matplotlib
logging.getLogger('matplotlib').setLevel(logging.ERROR)


@unittest.skipUnless(socket.gethostname() in ('arts041', 'zeus'), "Test can only run on arts041 or zeus")
class TestProcessorMaster(unittest.TestCase):
    def setUp(self):
        if socket.gethostname() == 'zeus':
            self.result_dir = '/data/arts/darc/central_dir_processor_master'
        else:
            self.result_dir = '/tank/users/arts/darc_automated_testing/processor_master'

        self.processor_queue = mp.Queue()
        self.processor = ProcessorMaster(self.processor_queue)

        tstart = Time.now()
        duration = TimeDelta(5, format='sec')

        parset = {'task.duration': str(duration.sec),
                  'task.startTime': tstart.isot,
                  'task.stopTime': (tstart + duration).isot,
                  'task.source.name': 'FAKE',
                  'task.taskID': '001122',
                  'task.beamSet.0.compoundBeam.0.phaseCenter': '[293.94876deg, 16.27778deg]',
                  'task.directionReferenceFrame': 'HADEC',
                  'task.telescopes': '[RT2, RT3, RT4, RT5, RT6, RT7, RT8, RT9]'}

        self.obs_config = {'beams': [0],
                           'home': os.path.expanduser('~'),
                           'freq': 1370,
                           'startpacket': int(tstart.unix * TIME_UNIT),
                           'date': '20200101',
                           'datetimesource': '2020-01-01-00:00:00.FAKE',
                           'result_dir': self.result_dir,
                           'parset': parset}

        # output directory
        output_dir = os.path.join(self.result_dir, self.obs_config['date'], self.obs_config['datetimesource'])
        # web directory
        webdir = '{home}/public_html/darc/{webdir}/{date}/{datetimesource}'.format(home=os.path.expanduser('~'),
                                                                                   webdir=self.processor.webdir,
                                                                                   **self.obs_config)
        # remove existing output
        for d in (output_dir, webdir):
            try:
                shutil.rmtree(d)
            except FileNotFoundError:
                pass
        util.makedirs(output_dir)

        # create empty PDF
        open(os.path.join(output_dir, 'CB00.pdf'), 'w').close()

        # create trigger overview
        # classifier cands must be > 0 to test processing of PDF attachments
        trigger_results = {'ncand_raw': 1000,
                           'ncand_post_clustering': 100,
                           'ncand_post_thresholds': 10,
                           'ncand_post_classifier': 1}

        with open(os.path.join(output_dir, 'CB00_summary.yaml'), 'w') as f:
            yaml.dump(trigger_results, f, default_flow_style=False)

        # create file with trigger list
        with open(os.path.join(output_dir, 'CB00_triggers.txt'), 'w') as f:
            f.write('#cb snr dm time downsamp sb p\n')
            f.write('00 10.00 20.00 30.0000 50 35 1.00\n')

    def test_processor_master(self):
        # override result dir
        self.processor.result_dir = self.result_dir

        # start processor
        self.processor.start()

        # start observation
        self.processor_queue.put({'command': 'start_observation', 'obs_config': self.obs_config, 'reload': False})

        # wait until processor is done
        self.processor.join()


if __name__ == '__main__':
    unittest.main()
