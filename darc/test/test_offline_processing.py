#!/usr/bin/env python
#
# OfflineProcessing end to end test
# 
# 
import os
import logging
import shutil
import unittest
import yaml
import socket

from astropy.time import Time

from darc.offline_processing import OfflineProcessing
from darc import util

TESTDIR = os.path.dirname(os.path.abspath(__file__))


class TestOfflineProcessing(unittest.TestCase):

    def gen_config(self, sb=False):
        if socket.gethostname() != 'arts041':
            self.skipTest("Can only run offline processing tests on arts041")

        if sb:
            output_dir = '/tank/users/oostrum/test_darc/output'
            min_freq = 1219.70092773
            freq = 1370
        else:
            output_dir = '/tank/users/oostrum/iquv/B0531/output_I'
            min_freq = 1249.70092773
            freq = 1400
        # SB AMBER
        amber_dir = os.path.join(output_dir, 'amber')
        result_dir = os.path.join(output_dir, 'results')

        duration = 300.032
        startpacket = int((Time.now().unix - duration) * 781250)
        # endtime = Time.now().datetime.strftime('%Y-%m-%d %H:%M:%S')

        config = {'ntabs': 12, 'nsynbeams': 71, 'beam': 0, 'mode': 'TAB',
                  'amber_dir': amber_dir, 'output_dir': output_dir,
                  'duration': 300.032, 'startpacket': startpacket, 
                  'result_dir': result_dir, 'min_freq': min_freq,
                  'datetimesource': '2019-01-01-00:00:00.FAKE',
                  'freq': freq}
        return config

    def test_worker_processing_tab(self):
        # config for worker should contain:
        # ntabs
        # nsynbeams
        # mode (IAB/TAB)
        # output_dir (/data2/output/<date>/<datetimesource)
        # startpacket
        # beam (CB)
        # amber_dir
        # duration
        # result_dir

        # config
        config = self.gen_config(sb=False)

        logging.basicConfig(format='%(asctime)s.%(levelname)s.%(module)s: %(message)s', level='DEBUG')
        logger = logging.getLogger()


        # output file names
        fname_yaml = "CB00_summary.yaml"
        fname_txt = "CB00_triggers.txt"
        fname_txt_in = "CB00_triggers_tab.txt" 
        fname_pdf = "CB00_candidates_summary.pdf"

        # set expected output for yaml
        expected_output_yaml = {'ncand_abovethresh': 75, 'ncand_classifier': 75, 'ncand_raw': 5923,
                                'ncand_skipped': 1339, 'ncand_trigger': 1414}
        # read expected output for txt
        with open(os.path.join(TESTDIR, fname_txt_in)) as f:
            expected_output_txt = f.read().strip().split()

        # Create result dir (normally done in run method, but that is skipped)
        try:
            util.makedirs(config['result_dir'])
        except Exception as e:
            self.fail('Cannot create result dir {}: {}'.format(config['result_dir'], e))
        else:
            proc = OfflineProcessing()
            # override logger (first initalized message still goes to normal logger)
            proc.logger = logger
            # override sb processing mode
            proc.process_sb = False

            # override config
            proc.config['nfreq_plot'] = 32
            proc.config['snrmin_processing'] = 10
            proc.config['snrmin_processing_local'] = 5
            proc.config['dmmin'] = 20
            proc.config['dmmax'] = 5000
            proc.config['pthresh_dmtime'] = 0
            # Add offline processing config to obs config (normally done in run method, but that is skipped)
            fullconfig = proc.config.copy()
            fullconfig.update(config)

            # run worker observation
            try:
                proc._start_observation_worker(fullconfig)
            except Exception as e:
                self.fail("Unhandled exception in offline processing: {}".format(e))

        # check if output files exist
        for fname in [fname_yaml, fname_txt, fname_pdf]:
            self.assertTrue(os.path.isfile(os.path.join(config['result_dir'], fname)))

        # for the yaml and txt, verify the content
        with open(os.path.join(config['result_dir'], fname_yaml)) as f:
            output_yaml = yaml.load(f, Loader=yaml.SafeLoader)
        self.assertDictEqual(expected_output_yaml, output_yaml)

        with open(os.path.join(config['result_dir'], fname_txt)) as f:
            output_txt = f.read().strip().split()
        self.assertListEqual(expected_output_txt, output_txt)

        # remove the results dir if it exists
        shutil.rmtree(config['result_dir'], ignore_errors=True)

    def test_worker_processing_sb(self):
        # config for worker should contain:
        # ntabs
        # nsynbeams
        # mode (IAB/TAB)
        # output_dir (/data2/output/<date>/<datetimesource)
        # startpacket
        # beam (CB)
        # amber_dir
        # duration
        # result_dir

        # config
        config = self.gen_config(sb=True)

        logging.basicConfig(format='%(asctime)s.%(levelname)s.%(module)s: %(message)s', level='DEBUG')
        logger = logging.getLogger()


        # output file names
        fname_yaml = "CB00_summary.yaml"
        fname_txt = "CB00_triggers.txt"
        fname_txt_in = "CB00_triggers_sb.txt"
        fname_pdf = "CB00_candidates_summary.pdf"

        # set expected output for yaml
        expected_output_yaml = {'ncand_abovethresh': 104, 'ncand_classifier': 89, 'ncand_raw': 2048,
                                'ncand_skipped': 109, 'ncand_trigger': 213}
        # read expected output for txt
        with open(os.path.join(TESTDIR, fname_txt_in)) as f:
            expected_output_txt = f.read().strip().split()

        # Create result dir (normally done in run method, but that is skipped)
        try:
            util.makedirs(config['result_dir'])
        except Exception as e:
            self.fail('Cannot create result dir {}: {}'.format(config['result_dir'], e))
        else:
            proc = OfflineProcessing()
            # override logger (first initalized message still goes to normal logger)
            proc.logger = logger
            # override sb processing mode
            proc.process_sb = True

            # override config
            proc.config['nfreq_plot'] = 32
            proc.config['snrmin_processing'] = 10
            proc.config['snrmin_processing_local'] = 5
            proc.config['dmmin'] = 20
            proc.config['dmmax'] = 5000
            proc.config['pthresh_dmtime'] = 0
            # Add offline processing config to obs config (normally done in run method, but that is skipped)
            fullconfig = proc.config.copy()
            fullconfig.update(config)

            # run worker observation
            try:
                proc._start_observation_worker(fullconfig)
            except Exception as e:
                self.fail("Unhandled exception in offline processing: {}".format(e))

        # check if output files exist
        for fname in [fname_yaml, fname_txt, fname_pdf]:
            self.assertTrue(os.path.isfile(os.path.join(config['result_dir'], fname)))

        # for the yaml and txt, verify the content
        with open(os.path.join(config['result_dir'], fname_yaml)) as f:
            output_yaml = yaml.load(f, Loader=yaml.SafeLoader)
        self.assertDictEqual(expected_output_yaml, output_yaml)

        with open(os.path.join(config['result_dir'], fname_txt)) as f:
            output_txt = f.read().strip().split()
        self.assertListEqual(expected_output_txt, output_txt)

        # remove the results dir if it exists
        shutil.rmtree(config['result_dir'], ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

