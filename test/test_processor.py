#!/usr/bin/env python3

import os
import glob
import logging
import ctypes
import unittest
import ast
from time import sleep
import threading
import multiprocessing as mp
from queue import Empty
import socket
from shutil import which, rmtree, copyfile
import numpy as np
from astropy.time import Time, TimeDelta
import h5py

from darc import Processor, ProcessorManager, AMBERListener
from darc.processor import Clustering, Extractor, Classifier, Visualizer
from darc import util
from darc.definitions import TIME_UNIT

# disable debug log messages from matplotlib
logging.getLogger('matplotlib').setLevel(logging.ERROR)


# An simple idling thread to test the thread scavenger
class Idling(threading.Thread):

    def __init__(self):
        super(Idling, self).__init__()
        self.event = mp.Event()

    def run(self):
        while not self.event.is_set():
            self.event.wait(.1)

    def stop_observation(self, abort=False):
        self.event.set()


class TestProcessorManager(unittest.TestCase):

    def test_scavenger(self):
        # initialize the processor manager
        manager = ProcessorManager()
        # set the scavenger interval
        manager.scavenger_interval = 0.1
        # the manager needs a queue before it can be started
        queue = mp.Queue()
        manager.set_source_queue(queue)

        # create a thread that idles forever
        thread = Idling()
        thread.name = 'obs'
        thread.start()
        # add the thread to the manager observation list
        manager.observations['0'] = thread

        manager.start()
        # give it some time to start
        sleep(.2)

        # the scavenger should not remove the thread
        sleep(manager.scavenger_interval)
        self.assertTrue(thread.is_alive())

        # now stop thread
        thread.event.set()
        # manager should remove thread, but how to check in Process setup?

        # stop the manager
        queue.put('stop')
        manager.join()


# skip if not running on arts041 or zeus
@unittest.skipUnless(socket.gethostname() in ('arts041', 'zeus'), "Test can only run on arts041 or zeus")
# Skip if psrdada not available
@unittest.skipIf(which('dada_db') is None, "psrdada not available")
class TestProcessor(unittest.TestCase):

    def setUp(self):
        if socket.gethostname() == 'zeus':
            self.dada_files = glob.glob('/data/arts/data/dada/*.dada')[:1]
            self.dada_files.sort()
            output_dir = '/data/arts/darc/output'
            log_dir = f'{output_dir}/log'
            filterbank_dir = f'{output_dir}/filterbank'
            amber_dir = f'{output_dir}/amber'
            amber_conf_dir = '/data/arts/darc/amber_conf'
            amber_conf_file = '/data/arts/darc/amber.conf'
            sb_table = '/data/arts/darc/sbtable-sc4-12tabs-71sbs.txt'
        else:
            self.dada_files = glob.glob('/tank/data/sky/B1933+16/20200211_dump/dada/*.dada')[:1]
            self.dada_files.sort()
            user = os.getlogin()
            if user == 'oostrum':
                main_dir = '/tank/users/oostrum/darc/automated_testing'
            elif user == 'arts':
                main_dir = '/tank/users/arts/darc_automated_testing'
            else:
                self.skipTest(f"Cannot run test as user {user}")
            output_dir = f'{main_dir}/output'
            log_dir = f'{output_dir}/log'
            filterbank_dir = f'{output_dir}/filterbank'
            amber_dir = f'{output_dir}/amber'
            amber_conf_dir = f'{main_dir}/amber/amber_conf'
            amber_conf_file = f'{main_dir}/amber/amber.conf'
            sb_table = '/home/arts/.controller/synthesized_beam_tables/sbtable-sc4-12tabs-71sbs.txt'

        # ensure we start clean
        try:
            rmtree(output_dir)
        except FileNotFoundError:
            pass
        for d in (output_dir, log_dir, amber_dir, filterbank_dir):
            util.makedirs(d)

        self.processes = {}

        # extract PSRDADA header
        self.header = self.get_psrdada_header(self.dada_files[0])

        self.tstart = Time.now() + TimeDelta(5, format='sec')
        # add general settings
        self.header['nreader'] = 2
        self.header['nbuffer'] = 5
        self.header['key_i'] = '5000'
        self.header['beam'] = 0
        self.header['ntab'] = 12
        self.header['nsb'] = 71
        # self.header['nbatch'] = int(float(self.header['SCANLEN']) / 1.024)
        self.header['nbatch'] = 10
        self.header['duration'] = float(self.header['SCANLEN'])
        self.header['log_dir'] = log_dir
        self.header['output_dir'] = output_dir
        self.header['filterbank_dir'] = filterbank_dir
        self.header['amber_dir'] = amber_dir
        self.header['amber_conf_dir'] = amber_conf_dir
        self.header['amber_config'] = amber_conf_file
        self.header['sb_table'] = sb_table
        self.header['date'] = '20200101'
        self.header['datetimesource'] = '2020-01-01-00:00:00.FAKE'
        self.header['freq'] = int(np.round(float(self.header['FREQ'])))
        self.header['snrmin'] = 8
        self.header['min_freq'] = 1220.7
        self.header['startpacket'] = int(self.tstart.unix * TIME_UNIT)

        # add parset
        parset = {'task.duration': self.header['SCANLEN'],
                  'task.startTime': self.tstart.isot,
                  'task.taskID': '001122',
                  'task.beamSet.0.compoundBeam.0.phaseCenter': '[293.94876deg, 16.27778deg]',
                  'task.directionReferenceFrame': 'J2000'}
        self.header['parset'] = parset

        # create ringbuffer
        self.create_ringbuffer()

        # create processes for the different pipeline steps
        self.diskdb_proc = self.diskdb_command()
        self.amber_proc = self.amber_command()
        self.dadafilterbank_proc = self.dadafilterbank_command()

        # start all except data reader
        self.dadafilterbank_proc.start()
        self.amber_proc.start()

        # initialize AMBERListener, used for feeding triggers to Processor
        self.amber_listener = AMBERListener()
        self.amber_listener.set_source_queue(mp.Queue())
        self.amber_listener.set_target_queue(mp.Queue())
        self.amber_listener.start()

        # initialize Processor, connect input queue to output of AMBERListener
        self.processor = Processor()
        self.processor.logger.setLevel('DEBUG')
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
                      "-dada -dada_key {key_i} -batches {nbatch} {extra_flags} " \
                      "-synthesized_beams_file {sb_table}".format(**fullconfig)
        proc = mp.Process(target=os.system, args=(amber_step1,))
        return proc

    def dadafilterbank_command(self):
        cmd = 'dadafilterbank -l {log_dir}/dadafilterbank.log -k {key_i} ' \
              '-n {filterbank_dir}/CB{beam:02d}'.format(**self.header)
        proc = mp.Process(target=os.system, args=(cmd, ))
        return proc

    def test_processor_obs(self):
        # start processor
        self.processor.start()

        # start amber listener and processor
        self.amber_listener.start_observation(obs_config=self.header, reload=False)
        self.processor.start_observation(obs_config=self.header, reload=False)

        # at start time, read data into buffer, other processes are already set up and waiting for data
        util.sleepuntil_utc(self.tstart)
        self.diskdb_proc.start()

        # wait until processes are done
        for proc in (self.diskdb_proc, self.amber_proc, self.dadafilterbank_proc):
            proc.join()

        # stop observation
        self.amber_listener.source_queue.put({'command': 'stop_observation', 'obs_config': self.header})
        self.processor.source_queue.put({'command': 'stop_observation'})

        # stop services
        self.amber_listener.source_queue.put('stop')
        self.amber_listener.join()
        self.processor.source_queue.put('stop')
        self.processor.join()


@unittest.skipUnless(socket.gethostname() == 'zeus', "Test can only run on zeus")
class TestExtractor(unittest.TestCase):

    def setUp(self):
        self.output_dir = '/data/arts/darc/output'

        startpacket = Time.now().unix // TIME_UNIT
        obs_config = {'freq': 1370, 'min_freq': 1220.7, 'startpacket': startpacket,
                      'output_dir': self.output_dir, 'beam': 0}
        logger = logging.getLogger('test_extractor')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        self.ncand = mp.Value('i', 0)
        self.extractor = Extractor(obs_config, self.output_dir + '/triggers_realtime', logger, mp.Queue(), mp.Queue(),
                                   self.ncand)
        # set filterbank reader (normally done in run method)
        self.extractor.filterbank_reader = self.extractor.init_filterbank_reader()
        # ensure we start clean
        for fname in glob.glob(os.path.join(self.output_dir, 'data', '*.hdf5')):
            os.remove(fname)

    def test_extract(self):
        # parameters from an earlier amber run
        snr = 71.26
        dm = 159.8
        toa = 5.79174
        sb = 35
        downsamp = 100

        # run extractor
        self.extractor._extract(dm, snr, toa, downsamp, sb)
        # read output file name
        try:
            fname = self.extractor.output_queue.get(timeout=.1)
        except Empty:
            fname = None
        self.assertTrue(fname is not None)
        # check that the output file exists
        self.assertTrue(os.path.isfile(fname))
        self.assertTrue(self.ncand.value == 1)


@unittest.skip('Need to fix usage of mp.Array')
@unittest.skipUnless(socket.gethostname() == 'zeus', "Test can only run on zeus")
class TestClassifier(unittest.TestCase):

    def setUp(self):
        # path to test file
        fname_in = glob.glob('/data/arts/darc/output/triggers_realtime/data/*.hdf5')[0]
        self.fname = fname_in.replace('.hdf5', '_test.hdf5')
        # copy over for testing as not to overwrite the original
        copyfile(fname_in, self.fname)

        # initialize the classifier
        logger = logging.getLogger('test_classifier')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        self.candarray = mp.Array(ctypes.c_wchar_p, 1000)
        self.ncand = mp.Value('i', 0)
        self.classifier = Classifier(logger, mp.Queue(), self.candarray, self.ncand)

    def test_classify(self):
        # start the classifier
        self.classifier.start()
        # feed the file path
        self.classifier.input_queue.put(self.fname)
        # stop the classifier
        self.classifier.input_queue.put('stop')
        self.classifier.join()
        # read the output
        self.assertEqual(self.ncand.value, 1)
        fnames = []
        for i in range(self.ncand.value):
            fnames.append(self.candarray[i])

        self.assertTrue(fnames[0].endswith('.hdf5'))
        # read the probabilities
        with h5py.File(fnames[0], 'r') as f:
            self.assertTrue('prob_freqtime' in f.attrs.keys())
            self.assertTrue('prob_dmtime' in f.attrs.keys())

    def tearDown(self):
        # remove the test file
        os.remove(self.fname)


@unittest.skipUnless(socket.gethostname() == 'zeus', "Test can only run on zeus")
class TestVisualizer(unittest.TestCase):

    def setUp(self):
        self.output_dir = '/data/arts/darc/output/triggers_realtime'
        self.result_dir = '/data/arts/darc/output/central'
        # ensure we start clean
        try:
            rmtree(self.result_dir)
        except FileNotFoundError:
            pass
        util.makedirs(self.result_dir)
        for fname in glob.glob(os.path.join(self.output_dir, '*.pdf')):
            os.remove(fname)

    def test_visualize(self):
        files = glob.glob('/data/arts/darc/output/triggers_realtime/data/*.hdf5')
        logger = logging.getLogger('test_visualizer')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        parset = {'task.taskID': '001122',
                  'task.beamSet.0.compoundBeam.0.phaseCenter': '[293.94876deg, 16.27778deg]',
                  'task.directionReferenceFrame': 'J2000'}
        obs_config = {'date': '20200101',
                      'datetimesource': '2020-01-01-00:00:00.FAKE',
                      'min_freq': 1220.7,
                      'beam': 0,
                      'parset': parset}
        Visualizer(self.output_dir, self.result_dir, logger, obs_config, files)
        # verify the output files are present
        for key in ('freq_time', 'dm_time', '1d_time'):
            self.assertTrue(len(glob.glob(f'{self.output_dir}/*{key}*.pdf')) > 0)
        self.assertTrue(os.path.isfile(f'{self.result_dir}/CB{obs_config["beam"]:02d}.pdf'))


if __name__ == '__main__':
    unittest.main()
