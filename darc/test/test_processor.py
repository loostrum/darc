#!/usr/bin/env python

import os
import glob
import unittest
import subprocess
import multiprocessing as mp
import socket
from shutil import which
from time import sleep
from queue import Empty
import numpy as np

from darc.processor import Processor
from darc import util


# skip if not running on arts041 or zeus
@unittest.skipUnless(socket.gethostname() in ('arts041', 'zeus'), "Test can only run on arts041 or zeus")
# Skip if psrdada not available
@unittest.skipIf(which('dada_db') is None, "psrdada not available")
class TestProcessor(unittest.TestCase):

    def setUp(self):
        if socket.gethostname() == 'zeus':
            self.dada_files = glob.glob('/data/arts/data/dada/*.dada')
            self.dada_files.sort()
            amber_dir = '/data/arts/darc/output/amber'
            amber_conf_dir = '/data/arts/darc/amber_conf'
            amber_conf_file = '/data/arts/darc/amber.conf'
            sb_table = '/data/arts/darc/sbtable-sc4-12tabs-71sbs.txt'
        else:
            self.skipTest("Test not supported yet on arts041")

        self.processes = {}

        # extract PSRDADA header
        self.header = self.get_psrdada_header(self.dada_files[0])

        # add general settings
        self.header['nreader'] = 1
        self.header['nbuffer'] = 5
        self.header['key_i'] = '5000'
        self.header['beam'] = 0
        self.header['nbatch'] = int(float(self.header['SCANLEN']) / 1.024)
        self.header['amber_dir'] = amber_dir
        self.header['amber_conf_dir'] = amber_conf_dir
        self.header['amber_conf_file'] = amber_conf_file
        self.header['sb_table'] = sb_table
        self.header['parset'] = 'parset'
        self.header['datetimesource'] = '2019-01-01-00:00:00.FAKE'
        self.header['freq'] = int(np.round(float(self.header['FREQ'])))

        # create ringbuffer
        self.create_ringbuffer()

        # create process to load data to buffer (but do not run it yet)
        self.diskdb_proc = self.diskdb_command()

        # create process to run amber
        self.amber_proc = self.amber_command()

    def tearDown(self):
        # remove ringbuffers
        for key in ('key_i', ):
            cmd = f'dada_db -d -k {self.header[key]}'
            os.system(cmd)

    @staticmethod
    def get_psrdada_header(fname):
        # load a typical amount of bytes from the file and look for header size keyword
        nbyte = 1
        raw_header = ''
        with open(fname, 'r') as f:
            while True:
                raw_header = raw_header + f.read(nbyte)
                header = [line.strip().split(maxsplit=1) for line in raw_header.split('\n')]
                header = np.array(header)
                try:
                    key_index = np.where(header == 'HDR_SIZE')[0]
                    hdr_size = header[key_index, 1][0].astype(int)
                except (IndexError, ValueError):
                    if nbyte > 1e6:
                        raise ValueError("Key HDR_SIZE not found in first MB of file")
                    nbyte += 4096
                else:
                    break
        # load the full header with known size
        with open(fname, 'r') as f:
            header = f.read(hdr_size)
        # convert to dict, skipping empty lines and zero padding at the end
        header = dict([line.strip().split(maxsplit=1) for line in header.split('\n') if line][:-1])
        return header

    def create_ringbuffer(self):
        # run ringbuffer
        cmd = 'dada_db -a {HDR_SIZE} -b {RESOLUTION} -k {key_i} -n {nbuffer} -r {nreader}'.format(**self.header)
        os.system(cmd)

    def diskdb_command(self):
        cmd = 'dada_diskdb -k {key_i} '.format(**self.header)
        for fname in self.dada_files:
            cmd += f' -f {fname}'
        proc = mp.Process(target=os.system, args=(cmd, ))
        return proc

    def amber_command(self):
        # load amber config file
        with open(self.header['amber_conf_file']) as f:
            amber_conf = util.parse_parset(f.read())
        print(amber_conf)
        amber_step1 = "taskset -c 3 amber -sync -print -opencl_platform 0 -opencl_device 0 " \
                      "-device_name ARTS_step1_81.92us_{freq}MHz " \
                      "-padding_file {amber_conf_dir}/padding.conf " \
                      "-zapped_channels {amber_conf_dir}/zapped_channels_{freq}.conf " \
                      "-integration_steps {amber_conf_dir}/integration_steps_x1.conf " \
                      "-subband_dedispersion " \
                      "-dedispersion_stepone_file {amber_conf_dir}/dedispersion_stepone.conf " \
                      "-dedispersion_steptwo_file {amber_conf_dir}/dedispersion_steptwo.conf " \
                      "-integration_file {amber_conf_dir}/integration.conf " \
                      "-snr_file {amber_conf_dir}/snr.conf " \
                      "-dms 32 -dm_first 0 -dm_step .2 -subbands 32 -subbanding_dms 64 " \
                      "-subbanding_dm_first 0 -subbanding_dm_step 6.4 -snr_sc -nsigma 3.00 " \
                      "-downsampling_configuration {amber_conf_dir}/downsampling.conf " \
                      "-downsampling_factor 1 -rfim  -time_domain_sigma_cut -frequency_domain_sigma_cut " \
                      "-time_domain_sigma_cut_steps {amber_conf_dir}/tdsc_steps.conf" \
                      " -time_domain_sigma_cut_configuration {amber_conf_dir}/tdsc.conf " \
                      "-frequency_domain_sigma_cut_steps {amber_conf_dir}/fdsc_steps.conf " \
                      "-frequency_domain_sigma_cut_configuration {amber_conf_dir}/fdsc.conf " \
                      "-nr_bins 48  -threshold 8.0 " \
                      "-output {amber_dir}/CB{beam:02d}_step1 " \
                      "-beams 12 -synthesized_beams 71 -synthesized_beams_chunk 4 " \
                      "-dada -dada_key {key_i} -batches {nbatch} " \
                      "-compact_results " \
                      "-synthesized_beams_file {sb_table}".format(**self.header)
        proc = mp.Process(target=os.system, args=(amber_step1,))
        print(amber_step1)
        return proc

    def test_processor(self):
        # read data into buffer
        self.diskdb_proc.start()
        # run amber
        self.amber_proc.start()
        # wait until reader is done
        self.diskdb_proc.join()
        # wait until amber is done
        self.amber_proc.join()


if __name__ == '__main__':
    unittest.main()
