#!/usr/bin/env python
#
# real-time data processor

import os
import threading
from time import sleep

import numpy as np
from single_pulse_ml import frbkeras, realtime_tools
import psrdada

from darc.base import DARCBase


class ProcessorException(Exception):
    pass


class Processor(DARCBase):
    """
    Process dada events
    """

    def __init__(self):
        super(Processor, self).__init__()
        self.needs_target_queue = True
        self.thread = None
        self.observation_running = False

        # intialise analysis tools
        self.rtproc = realtime_tools.RealtimeProc()
        self.model_freqtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_freqtime))
        self.model_dmtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_dmime))

        # For some reason, the model's first prediction takes a long time.
        # pre-empt this by classifying an array of zeros before looking at real data
        self.model_freqtime.predict(np.zeros([1, self.nfreq, self.ntime, 1]))
        self.model_dmtime.predict(np.zeros([1, self.ndm, self.ntime, 1]))

    # No commands other than start/stop observation
    def process_command(self, command):
        pass

    def start_observation(self, obs_config):
        """
        Start observation
        """
        if self.thread:
            # old obs thread exists, stop it
            self.stop_observation()
        self.observation_running = True
        self.thread = threading.Thread(target=self.read_and_process_data, args=[obs_config])
        self.thread.daemon = True
        self.thread.start()
        self.logger.info("Observation started")

    def stop_observation(self):
        """
        Stop observation
        """
        self.logger.info("Stopping observation")

    def read_and_process_data(self, obs_config):
        """
        Read ringbuffer pages and process them
        :param obs_config: observation config dict
        """
        # initialize reader
        reader = psrdada.Reader()
        # get dada key (hex)
        dada_key = int('0x{}'.format(obs_config['eventkey_i']), 16)
        # try to connect - keep trying in case this thread starts before buffers are created
        while self.observation_running:
            try:
                reader.connect(dada_key)
            except psrdada.exceptions.PSRDadaError as e:
                self.logger.error("Failed to connect to ringbuffer, will retry: {}".format(e))
                sleep(1)
            else:
                break

        # we are connected; start reading pages while the obs is running
        while self.observation_running:
            dada_header = None

            for page in reader:
                # read header
                if dada_header is None:
                    dada_header = realtime_tools.DadaHeader(reader.getHeader(), triger=True)
                self.logger.info('Received event with (DM, S/N, beam) = ({})'.format(dada_header.dm, dada_header.snr,
                                                                                     dada_header.beamno))
                data = np.asarray(page)

            self.logger.info("End of event")

        # observation is done, disconnect reader
        try:
            reader.disconnect()
        except psrdada.exceptions.PSRDadaError as e:
            self.logger.error("Failed to disconnect reader: {}".format(e))

    def lofar_trigger(self, trigger):
        """
        Contact VOEvent server to send LOFAR trigger
        """
        pass

    def iquv_trigger(self, trigger):
        """
        Trigger IQUV data dump
        """
        pass