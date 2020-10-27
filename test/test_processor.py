#!/usr/bin/env python3

import os
import glob
import unittest
import ast
from time import sleep
import threading
import multiprocessing as mp
import socket
from shutil import which, rmtree
import numpy as np
from astropy.time import Time, TimeDelta

from darc import Processor, ProcessorManager, AMBERListener
from darc import util


# An simple idling thread to test the thread scavenger
class Idling(threading.Thread):

    def __init__(self):
        super(Idling, self).__init__()
        self.event = mp.Event()

    def run(self):
        while not self.event.is_set():
            self.event.wait(.1)

    def stop_observation(self):
        self.event.set()


class TestProcessorManager(unittest.TestCase):

    def test_scavenger(self):
        # initialize the processor manager
        manager = ProcessorManager()
        # set the scavenger interval
        manager.scavenger_interval = 0.1
        # the thread scavenger should now be running
        self.assertTrue(manager.scavenger.is_alive())

        # create a thread that idles forever
        thread = Idling()
        thread.start()
        # add the thread to the manager observation list
        manager.observations['0'] = thread

        # the scavenger should not remove either thread
        sleep(manager.scavenger_interval)
        self.assertTrue(thread.is_alive())

        # now stop one thread
        thread.event.set()
        # the scavenger should remove this thread
        sleep(manager.scavenger_interval)
        self.assertTrue(not thread.is_alive())
        # it should also be removed from the observation list
        self.assertTrue('0' not in manager.observations.keys())

        # start a new thread
        thread = Idling()
        thread.start()
        # add the thread to the manager observation list
        manager.observations['0'] = thread

        # stop the manager, which should also stop the thread
        manager.stop()
        self.assertTrue(not thread.is_alive())


# skip if not running on arts041 or zeus
@unittest.skipUnless(socket.gethostname() in ('arts041', 'zeus'), "Test can only run on arts041 or zeus")
# Skip if psrdada not available
@unittest.skipIf(which('dada_db') is None, "psrdada not available")
class TestProcessor(unittest.TestCase):

    def setUp(self):
        if socket.gethostname() == 'zeus':
            self.dada_files = glob.glob('/data/arts/data/dada/*.dada')[:1]
            self.dada_files.sort()
            log_dir = '/data/arts/darc/output/log'
            filterbank_dir = '/data/arts/darc/output/filterbank'
            amber_dir = '/data/arts/darc/output/amber'
            amber_conf_dir = '/data/arts/darc/amber_conf'
            amber_conf_file = '/data/arts/darc/amber.conf'
            sb_table = '/data/arts/darc/sbtable-sc4-12tabs-71sbs.txt'
        else:
            self.skipTest("Test not supported yet on arts041")

        # ensure we start clean
        for d in (log_dir, amber_dir, filterbank_dir):
            try:
                rmtree(d)
            except FileNotFoundError:
                pass
            util.makedirs(d)

        self.processes = {}

        # extract PSRDADA header
        self.header = self.get_psrdada_header(self.dada_files[0])

        # add general settings
        self.header['nreader'] = 2
        self.header['nbuffer'] = 5
        self.header['key_i'] = '5000'
        self.header['beam'] = 0
        self.header['ntab'] = 12
        self.header['nsb'] = 71
        # self.header['nbatch'] = int(float(self.header['SCANLEN']) / 1.024)
        self.header['nbatch'] = 10
        self.header['log_dir'] = log_dir
        self.header['filterbank_dir'] = filterbank_dir
        self.header['amber_dir'] = amber_dir
        self.header['amber_conf_dir'] = amber_conf_dir
        self.header['amber_config'] = amber_conf_file
        self.header['sb_table'] = sb_table
        self.header['datetimesource'] = '2020-01-01-00:00:00.FAKE'
        self.header['freq'] = int(np.round(float(self.header['FREQ'])))
        self.header['snrmin'] = 8
        self.header['min_freq'] = 1220.0
        self.header['tstart'] = Time.now() + TimeDelta(5, format='sec')

        # add encoded parset
        parset = """
        task.duration = {SCANLEN}
        task.starttime = {tstart}
        """.format(**self.header)
        self.header['parset'] = util.encode_parset(parset)

        # create ringbuffer
        self.create_ringbuffer()

        # create processes for the different pipeline steps
        self.diskdb_proc = self.diskdb_command()
        self.amber_proc = self.amber_command()
        self.dadafilterbank_proc = self.dadafilterbank_command()

        # start all except data reader
        self.dadafilterbank_proc.start()
        self.amber_proc.start()

        # open processor config file
        # self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_processor.yaml')
        # with open(self.config_file, 'r') as f:
        #     self.config = yaml.load(f, Loader=yaml.SafeLoader)

        # initialize AMBERListener, used for feeding triggers to Processor
        self.amber_listener = AMBERListener()
        self.amber_listener.set_source_queue(mp.Queue())
        self.amber_listener.set_target_queue(mp.Queue())
        self.amber_listener.start()

        # initialize Processor, connect input queue to output of AMBERListener
        self.processor = Processor()
        self.processor.set_source_queue(self.amber_listener.target_queue)

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
        with open(self.header['amber_config']) as f:
            amber_conf = util.parse_parset(f.read())
        # extract step1 settings and add to a full config dict
        fullconfig = self.header.copy()
        for key, value in amber_conf.items():
            # some values are lists, interpret these
            if value.startswith('['):
                value = ast.literal_eval(value)
            if isinstance(value, list):
                # extract 1st item
                fullconfig[key] = value[0]
            else:
                fullconfig[key] = value
        # add freq to device name
        fullconfig['device_name'] = fullconfig['device_name'].format(**self.header)
        amber_step1 = "taskset -c 3 amber -sync -print -opencl_platform {opencl_platform} " \
                      "-opencl_device {opencl_device} " \
                      "-device_name {device_name} " \
                      "-padding_file {amber_conf_dir}/padding.conf " \
                      "-zapped_channels {amber_conf_dir}/zapped_channels_{freq}.conf " \
                      "-integration_steps {amber_conf_dir}/{integration_file} " \
                      "-subband_dedispersion " \
                      "-dedispersion_stepone_file {amber_conf_dir}/dedispersion_stepone.conf " \
                      "-dedispersion_steptwo_file {amber_conf_dir}/dedispersion_steptwo.conf " \
                      "-integration_file {amber_conf_dir}/integration.conf " \
                      "-snr_file {amber_conf_dir}/snr.conf " \
                      "-dms {num_dm} -dm_first {dm_first} -dm_step {dm_step} -subbands {subbands} " \
                      "-subbanding_dms {subbanding_dms} -subbanding_dm_first {subbanding_dm_first} " \
                      "-subbanding_dm_step {subbanding_dm_step} -snr_sc -nsigma {snr_nsigma} " \
                      "-downsampling_configuration {amber_conf_dir}/downsampling.conf " \
                      "-downsampling_factor {downsamp} -rfim -time_domain_sigma_cut -frequency_domain_sigma_cut " \
                      "-time_domain_sigma_cut_steps {amber_conf_dir}/tdsc_steps.conf" \
                      " -time_domain_sigma_cut_configuration {amber_conf_dir}/tdsc.conf " \
                      "-frequency_domain_sigma_cut_steps {amber_conf_dir}/fdsc_steps.conf " \
                      "-frequency_domain_sigma_cut_configuration {amber_conf_dir}/fdsc.conf " \
                      "-nr_bins {fdsc_nbins} -threshold {snrmin} " \
                      "-output {amber_dir}/CB{beam:02d}_step1 " \
                      "-beams {ntab} -synthesized_beams {nsb} -synthesized_beams_chunk {nsynbeams_chunk} " \
                      "-dada -dada_key {key_i} -batches {nbatch} {extra_flags}" \
                      "-synthesized_beams_file {sb_table}".format(**fullconfig)
        proc = mp.Process(target=os.system, args=(amber_step1,))
        return proc

    def dadafilterbank_command(self):
        cmd = 'dadafilterbank -l {log_dir}/dadafilterbank.log -k {key_i} ' \
              '-n {filterbank_dir}/CB{beam:02d}'.format(**self.header)
        proc = mp.Process(target=os.system, args=(cmd, ))
        return proc

    def test_processor_obs(self):
        # set processor settings
        self.processor.interval = 1.0
        # start processor
        self.processor.start()

        # start amber listener and processor
        self.amber_listener.start_observation(obs_config=self.header, reload=False)
        self.processor.start_observation(obs_config=self.header, reload=False)

        # at start time, read data into buffer, other processes are already set up and waiting for data
        util.sleepuntil_utc(self.header['tstart'])
        self.diskdb_proc.start()

        # wait until processes are done
        for proc in (self.diskdb_proc, self.amber_proc, self.dadafilterbank_proc):
            proc.join()

        # stop observation
        self.amber_listener.stop_observation()
        self.processor.stop_observation()

        # stop services
        self.amber_listener.stop()
        self.processor.stop()


if __name__ == '__main__':
    unittest.main()
