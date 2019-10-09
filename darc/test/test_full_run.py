#!/usr/bin/env python3

import os
import numpy as np
import glob
import unittest
import socket
from shutil import which
import multiprocessing as mp
import threading
from time import sleep
import yaml
from astropy.time import Time, TimeDelta
from queue import Empty

try:
    import psrdada
except ImportError:
    psrdada = None

from darc.darc_master import DARCMaster
from darc.control import send_command
from darc import util


@unittest.skip("This test is not fully implemented yet")
# only run this test if this script is run directly, _not_ in automated testing (pytest etc)
@unittest.skipUnless(__name__ == '__main__', "Skipping full test run in automated testing")
# skip if not running on arts041
@unittest.skipUnless(socket.gethostname() == 'arts041', "Test can only run on arts041")
# Skip if psrdada not available
@unittest.skipIf(psrdada is None or which('dada_db') is None, "psrdada not available")
class TestFullRun(unittest.TestCase):

    def setUp(self):
        """
        Set up the pipeline and observation software
        """
        # observation settings
        files = glob.glob('/tank/data/sky/B0329+54/2019-06-25-11:17:00.B0329+54/dada/*.dada')
        self.assertTrue(len(files) > 0)
        # FOR NOW ONE FILE
        files = [files[0]]
        self.settings = {'resolution': 1536*12500*12, 'nbuf': 5, 'key_i': 'aaaa',
                         'hdr_size': 4096, 'dada_files': files, 'nreader': 1}

        # store custom config file
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),  'config.yaml')
        with open(self.config_file, 'r') as f:
            self.master_config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']

        # create buffers
        # stokes I
        os.system('dada_db -d -k {key_i} 2>/dev/null ; dada_db -k {key_i} -a {hdr_size} '
                  '-b {resolution} -n {nbuf} -r {nreader}'.format(**self.settings))

        # start GPU pipeline
        # <<AMBER>>

        # start DARC master
        master = DARCMaster(config_file=self.config_file)
        self.t_darc_service = threading.Thread(target=master.run, name='darc_service', daemon=True)
        self.t_darc_service.start()
        # start DARC services
        send_command(timeout=5, service='all', command='start', port=self.master_config['port'])

        # initalize writer thread
        self.t_disk_to_db = threading.Thread(target=self.writer, name='disk_to_db', daemon=True)

    def tearDown(self):
        """
        Clean up after pipeline run
        """
        # remove buffers
        os.system('dada_db -d -k {key_i}'.format(**self.settings))
        # stop DARC master (also stops services)
        send_command(timeout=5, service=None, command='stop_master', port=self.master_config['port'])
        self.t_darc_service.join()

    def writer(self):
        """
        Write data from disk into ringbuffer
        """
        # connect to the buffer
        print("Writer start")
        dada_writer = psrdada.Writer()
        hex_key = int('0x{key_i}'.format(**self.settings), 16)
        dada_writer.connect(hex_key)

        # loop over dada files
        for n, fname in enumerate(self.settings['dada_files']):
            # open file
            with open(fname, 'rb') as f:
                # read the header of the first file
                if n == 0:
                    # read header, strip empty bytes, convert to string, remove last newline
                    raw_hdr = f.read(self.settings['hdr_size']).rstrip(b'\x00').decode().strip()
                    # convert to dict by splitting on newline, then whitespace
                    hdr = dict([line.split(maxsplit=1) for line in raw_hdr.split('\n')])
                    dada_writer.setHeader(hdr)
                else:
                    # skip header
                    f.seek(self.settings['hdr_size'])
                # read data
                while True:
                    data = np.fromfile(f, count=self.settings['resolution'], dtype='uint8')
                    # if at end of file, mark end of data
                    if len(data) == 0:
                        break
                    else:
                        # write page into buffer
                        page = dada_writer.getNextPage()
                        page = np.asarray(page)
                        page[:] = data[:]
                        dada_writer.markFilled()

        # all pages done, mark EoD
        dada_writer.getNextPage()
        dada_writer.markEndOfData()
        # disconnect buffer
        dada_writer.disconnect()
        print("Writer done")

    def test_full_run(self):
        """
        Do a full pipeline run
        """

        # start DARC observation 5s in the future
        tstart = Time.now() + TimeDelta(5, format='sec')

        amber_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amber.conf')
        obs_config = {'amber_dir': os.path.dirname(os.path.abspath(__file__)),
                     'beam': 0,
                     'amber_config': amber_conf_file}
        send_command(timeout=5, service=None, command='start_observation',
                     payload=obs_config, port=self.master_config['port'])

        # start writer at start time
        util.sleepuntil_utc(tstart)
        self.t_disk_to_db.start()

        # Now AMBER should start producing triggers

        # Listen for any IQUV triggers in thread

        # wait for the writer to finish
        self.t_disk_to_db.join()


        # start observation

        ## stop the observation
        #obs_queue.put({'command': 'stop_observation'})
        #sleep(1)


if __name__ == '__main__':
    unittest.main()
