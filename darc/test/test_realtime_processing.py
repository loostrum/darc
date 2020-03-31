#!/usr/bin/env python3

import os
import sys
import errno
import numpy as np
import glob
import unittest
import socket
from shutil import which, rmtree
import multiprocessing as mp
import threading
from queue import Empty
from time import sleep
import yaml
from astropy.time import Time, TimeDelta

try:
    import psrdada
except ImportError:
    psrdada = None

from darc.darc_master import DARCMaster
from darc.control import send_command
from darc.amber_listener import AMBERListener
from darc.amber_clustering import AMBERClustering
from darc.dada_trigger import DADATrigger
from darc import util


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
        print("Setup")
        # observation settings
        files = glob.glob('/tank/data/sky/B1933+16/20200211_dump/dada/*.dada')
        self.assertTrue(len(files) > 0)
        output_dir = '/tank/users/oostrum/test_full_run'
        amber_dir = os.path.join(output_dir, 'amber')
        amber_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amber.conf')
        # ensure we start clean
        rmtree(amber_dir)
        util.makedirs(amber_dir)

        # load the encoded parset
        with open('/tank/data/sky/B1933+16/20200211_dump/parset', 'r') as f:
            parset = f.read().strip()

        # nreader: one for each programme readding from the buffer, i.e. 3x AMBER
        self.settings = {'resolution': 1536*12500*12, 'nbuf': 5, 'key_i': 'aaaa',
                         'hdr_size': 40960, 'dada_files': files, 'nreader': 3,
                         'freq': 1370, 'amber_dir': amber_dir, 'nbatch': len(files)*10,
                         'beam': 0, 'amber_config': amber_conf_file, 'min_freq': 1219.70092773,
                         'parset': parset}

        # store custom config file
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
        with open(self.config_file, 'r') as f:
            self.master_config = yaml.load(f, Loader=yaml.SafeLoader)['darc_master']

        # create buffers
        # stokes I
        print("Creating buffers")
        os.system('dada_db -d -k {key_i} 2>/dev/null ; dada_db -k {key_i} -a {hdr_size} '
                  '-b {resolution} -n {nbuf} -r {nreader}'.format(**self.settings))

        # init GPU pipeline thread
        self.t_amber = threading.Thread(target=self.amber, name='AMBER', daemon=True)

        # start DARC master
        #master = DARCMaster(config_file=self.config_file)
        #self.t_darc_service = threading.Thread(target=master.run, name='darc_service', daemon=True)
        #print("Starting DARC")
        #self.t_darc_service.start()
        ## start DARC services
        #send_command(timeout=5, service='all', command='start', port=self.master_config['port'])

        # init amber listener thread
        self.listener = AMBERListener()
        self.listener.set_source_queue(mp.Queue())
        self.listener.set_target_queue(mp.Queue())
        self.listener.start()

        # init amber clustering thread
        # do not connect to VOEvent server nor LOFAR trigger system
        self.clustering = AMBERClustering(connect_vo=False, connect_lofar=False)
        self.clustering.set_source_queue(self.listener.target_queue)
        self.clustering.set_target_queue(mp.Queue())
        self.clustering.start()

        # init DADA trigger thread
        self.dadatrigger = DADATrigger()
        self.dadatrigger.set_source_queue(self.clustering.target_queue)
        self.dadatrigger.start()

        # initalize writer thread
        self.t_disk_to_db = threading.Thread(target=self.writer, name='disk_to_db', daemon=True)

        # initalize output listener thread
        self.event_queue = mp.Queue()
        self.t_dbevent = threading.Thread(target=self.dbevent, name='dbevent', daemon=True,
                                          args=(self.event_queue,))

    def tearDown(self):
        """
        Clean up after pipeline run
        """
        print("teardown")
        # remove buffers
        print("Removing buffers")
        os.system('dada_db -d -k {key_i}'.format(**self.settings))
        # stop DARC master (also stops services)
        #print("Stopping DARC")
        #send_command(timeout=5, service=None, command='stop_master', port=self.master_config['port'])
        #self.t_darc_service.join()
        self.listener.stop()
        self.clustering.stop()
        self.dadatrigger.stop()

    def writer(self):
        """
        Write data from disk into ringbuffer
        """
        print("Starting writer")
        # connect to the buffer
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
                    hdr = dict([line.split(maxsplit=1) for line in raw_hdr.split('\n') if line ])
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

    def amber(self):
        """
        Run GPU pipeline
        """
        print("Starting AMBER")
        # ugly and hardcoded for now
        # should add proper amber.conf file and generate from there
        amber_step1 = "taskset -c 3 amber -sync -print -opencl_platform 0 -opencl_device 1 " \
                      "-device_name ARTS_step1_81.92us_{freq}MHz " \
                      "-padding_file /home/arts/.controller/amber_conf/padding.conf " \
                      "-zapped_channels /home/arts/.controller/amber_conf/zapped_channels_{freq}.conf " \
                      "-integration_steps /home/arts/.controller/amber_conf/integration_steps_x1.conf " \
                      "-subband_dedispersion " \
                      "-dedispersion_stepone_file /home/arts/.controller/amber_conf/dedispersion_stepone.conf " \
                      "-dedispersion_steptwo_file /home/arts/.controller/amber_conf/dedispersion_steptwo.conf " \
                      "-integration_file /home/arts/.controller/amber_conf/integration.conf " \
                      "-snr_file /home/arts/.controller/amber_conf/snr.conf " \
                      "-dms 32 -dm_first 0 -dm_step .2 -subbands 32 -subbanding_dms 64 " \
                      "-subbanding_dm_first 0 -subbanding_dm_step 6.4 -snr_sc -nsigma 3.00 " \
                      "-max_std_file /home/arts/.controller/amber_conf/max_std.conf " \
                      "-mom_stepone_file /home/arts/.controller/amber_conf/mom_stepone.conf " \
                      "-mom_steptwo_file /home/arts/.controller/amber_conf/mom_steptwo.conf " \
                      "-downsampling_configuration /home/arts/.controller/amber_conf/downsampling.conf " \
                      "-downsampling_factor 1 -rfim  -time_domain_sigma_cut -frequency_domain_sigma_cut " \
                      "-time_domain_sigma_cut_steps /home/arts/.controller/amber_conf/tdsc_steps.conf" \
                      " -time_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/tdsc.conf " \
                      "-frequency_domain_sigma_cut_steps /home/arts/.controller/amber_conf/fdsc_steps.conf " \
                      "-frequency_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/fdsc.conf " \
                      "-nr_bins 48  -threshold 8.0 " \
                      "-output {amber_dir}/CB{beam:02d}_step1 " \
                      "-beams 12 -synthesized_beams 71 -synthesized_beams_chunk 4 " \
                      "-dada -dada_key {key_i} -batches {nbatch} " \
                      "-compact_results " \
                      "-synthesized_beams_file /home/arts/.controller/synthesized_beam_tables/" \
                      "sbtable-sc4-12tabs-71sbs.txt".format(**self.settings)

        amber_step2 = "taskset -c 15 amber -sync -print -opencl_platform 0 -opencl_device 2 " \
                      "-device_name ARTS_step2_81.92us_{freq}MHz " \
                      "-padding_file /home/arts/.controller/amber_conf/padding.conf " \
                      "-zapped_channels /home/arts/.controller/amber_conf/zapped_channels_{freq}.conf " \
                      "-integration_steps /home/arts/.controller/amber_conf/integration_steps_x1.conf " \
                      "-subband_dedispersion " \
                      "-dedispersion_stepone_file /home/arts/.controller/amber_conf/dedispersion_stepone.conf " \
                      "-dedispersion_steptwo_file /home/arts/.controller/amber_conf/dedispersion_steptwo.conf " \
                      "-integration_file /home/arts/.controller/amber_conf/integration.conf " \
                      "-snr_file /home/arts/.controller/amber_conf/snr.conf " \
                      "-dms 32 -dm_first 0 -dm_step .2 -subbands 32 -subbanding_dms 64 " \
                      "-subbanding_dm_first 409.6 -subbanding_dm_step 6.4 -snr_sc -nsigma 3.00 " \
                      "-max_std_file /home/arts/.controller/amber_conf/max_std.conf " \
                      "-mom_stepone_file /home/arts/.controller/amber_conf/mom_stepone.conf " \
                      "-mom_steptwo_file /home/arts/.controller/amber_conf/mom_steptwo.conf  " \
                      "-downsampling_configuration /home/arts/.controller/amber_conf/downsampling.conf " \
                      "-downsampling_factor 1 -rfim  -time_domain_sigma_cut -frequency_domain_sigma_cut " \
                      "-time_domain_sigma_cut_steps /home/arts/.controller/amber_conf/tdsc_steps.conf " \
                      "-time_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/tdsc.conf " \
                      "-frequency_domain_sigma_cut_steps /home/arts/.controller/amber_conf/fdsc_steps.conf " \
                      "-frequency_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/fdsc.conf " \
                      "-nr_bins 48  -threshold 8.0 " \
                      "-output {amber_dir}/CB{beam:02d}_step2 " \
                      "-beams 12 -synthesized_beams 71 -synthesized_beams_chunk 4 " \
                      "-dada -dada_key {key_i} -batches {nbatch} " \
                      "-compact_results " \
                      "-synthesized_beams_file /home/arts/.controller/synthesized_beam_tables/" \
                      "sbtable-sc4-12tabs-71sbs.txt".format(**self.settings)

        amber_step3 = "taskset -c 16 amber -sync -print -opencl_platform 0 -opencl_device 3 " \
                      "-device_name ARTS_step3_nodownsamp_81.92us_{freq}MHz " \
                      "-padding_file /home/arts/.controller/amber_conf/padding.conf " \
                      "-zapped_channels /home/arts/.controller/amber_conf/zapped_channels_{freq}.conf " \
                      "-integration_steps /home/arts/.controller/amber_conf/integration_steps_x1.conf " \
                      "-subband_dedispersion " \
                      "-dedispersion_stepone_file /home/arts/.controller/amber_conf/dedispersion_stepone.conf " \
                      "-dedispersion_steptwo_file /home/arts/.controller/amber_conf/dedispersion_steptwo.conf " \
                      "-integration_file /home/arts/.controller/amber_conf/integration.conf " \
                      "-snr_file /home/arts/.controller/amber_conf/snr.conf " \
                      "-dms 16 -dm_first 0 -dm_step 2.5 -subbands 32 -subbanding_dms 64 " \
                      "-subbanding_dm_first 819.2 -subbanding_dm_step 40.0 -snr_sc -nsigma 3.00 " \
                      "-max_std_file /home/arts/.controller/amber_conf/max_std.conf " \
                      "-mom_stepone_file /home/arts/.controller/amber_conf/mom_stepone.conf " \
                      "-mom_steptwo_file /home/arts/.controller/amber_conf/mom_steptwo.conf " \
                      "-downsampling_configuration /home/arts/.controller/amber_conf/downsampling.conf " \
                      "-downsampling_factor 1 -rfim  -time_domain_sigma_cut -frequency_domain_sigma_cut " \
                      "-time_domain_sigma_cut_steps /home/arts/.controller/amber_conf/tdsc_steps.conf " \
                      "-time_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/tdsc.conf " \
                      "-frequency_domain_sigma_cut_steps /home/arts/.controller/amber_conf/fdsc_steps.conf " \
                      "-frequency_domain_sigma_cut_configuration /home/arts/.controller/amber_conf/fdsc.conf " \
                      "-nr_bins 48  -threshold 8.0 " \
                      "-output {amber_dir}/CB{beam:02d}_step3 " \
                      "-beams 12 -synthesized_beams 71 -synthesized_beams_chunk 4 " \
                      "-dada -dada_key {key_i} -batches {nbatch} " \
                      "-compact_results " \
                      "-synthesized_beams_file /home/arts/.controller/synthesized_beam_tables/" \
                      "sbtable-sc4-12tabs-71sbs.txt".format(**self.settings)

        threads = []
        # start all steps
        for i, step in enumerate([amber_step1, amber_step2, amber_step3]):
            print("Starting AMBER step {}".format(i+1))
            thread = threading.Thread(target=os.system, args=(step,),
                                      name='AMBER_step{}'.format(i+1), daemon=True)
            thread.start()
            sleep(.1)
            threads.append(thread)
        # wait for all to finish
        for thread in threads:
            thread.join()
        print("AMBER done")

    def dbevent(self, queue):
        """
        Listen for IQUV events on dada_dbevent port
        :param queue: queue where output is put
        """
        # open a listening socket for stokes IQUV events
        port = self.dadatrigger.port_iquv
        # should receive first trigger within a minute
        timeout = 60
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", port))
            sock.listen(5)
            sock.settimeout(timeout)
        except socket.error as e:
            self.fail("Failed to set up listening socket for events: {}".format(e))
        # accept connection
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
            event = client.recv(1024).decode()
        # close the socket
        sock.close()
        # store the event. Cannot use return in a thread
        queue.put(event)

    def test_full_run(self):
        """
        Do a full pipeline run
        """

        # Buffers are already set up
        # start AMBER
        self.t_amber.start()
        # Give it some time to start
        sleep(1)

        # start DARC observation 5s in the future
        tstart = Time.now() + TimeDelta(5, format='sec')
        # add start time to config
        self.settings['startpacket'] = tstart.unix * 781250

        # Todo: ensure DARC waits for start time

        # start observation
        cmd = {'command': 'start_observation', 'obs_config': self.settings}
        self.listener.source_queue.put(cmd)
        self.clustering.source_queue.put(cmd)

        #print("Sending start_observation")
        #send_command(timeout=5, service=None, command='start_observation',
        #             payload=obs_config, port=self.master_config['port'])

        # start writer at start time
        util.sleepuntil_utc(tstart)
        self.t_disk_to_db.start()

        # Now AMBER should start producing triggers
        # Listen for any IQUV triggers in thread
        self.t_dbevent.start()

        # wait for the writer to finish
        self.t_disk_to_db.join()
        # AMBER may still be processing, wait for that to finish too
        self.t_amber.join()

        # stop the observation
        self.listener.source_queue.put({'command': 'stop_observation'})
        self.clustering.source_queue.put({'command': 'stop_observation'})

        # verify an IQUV event was received
        try:
            event = self.event_queue.get(timeout=1)
        except Empty:
            event = None

        # event looks like (depending on start time):
        # N_EVENTS 2
        # 2019-10-14-13:14:26.016
        # 2019-10-14-13:14:37 137 2019-10-14-13:14:39 185 12.3749 26.4 50.0 21
        # 2019-10-14-13:14:37 849 2019-10-14-13:14:39 897 14.1344 31.8 50.0 21
        self.assertTrue(event is not None)
        print("Received event:\n{}".format(event))


if __name__ == '__main__':
    unittest.main()
