#!/usr/bin/env python3

import os
import socket
from argparse import Namespace
import threading
from time import sleep
from queue import Empty
import multiprocessing as mp
import yaml
import numpy as np
import h5py

try:
    from tensorflow.keras.models import load_model
except ImportError:
    raise NotImplementedError("Older tensorflow/keras not supported yet")

from darc.definitions import CONFIG_FILE


class Classifier(threading.Thread):
    """
    Classify candidates from HDF5 files produced by Extractor
    """

    def __init__(self, logger, input_queue):
        """
        :param Logger logger: Processor logger object
        :param Queue input_queue: Input queue for triggers
        """
        super(Classifier, self).__init__()
        self.logger = logger
        self.input_queue = input_queue

        # load config
        self.config = self._load_config()

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.model_freqtime = None
        self.model_dmtime = None

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting classifier thread")

        self._init_models()

        while not self.stop_event.is_set():
            # read file paths from input queue
            try:
                fname = self.input_queue.get(timeout=.1)
            except Empty:
                self.input_empty = True
                continue
            else:
                self.input_empty = False
                # do classification
                self._classify(fname)
        # close the output file
        self.logger.info("Stopping classifier thread")

    def stop(self):
        """
        Stop this thread
        """
        # wait until the input queue is empty
        while not self.input_empty:
            sleep(1)
        # then stop
        self.stop_event.set()

    @staticmethod
    def _load_config():
        """
        Load configuration
        """
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['classifier']
        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.format(**kwargs)
            # replace any -1 by infinite
            elif value == -1:
                config[key] = np.inf
        # return as Namespace so the keys can be accessed as attributes
        return Namespace(**config)

    def _init_models(self):
        pass
        # intialise analysis tools
        # self.model_freqtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_freqtime))
        # self.model_dmtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_dmime))

        # For some reason, the model's first prediction takes a long time.
        # pre-empt this by classifying an array of zeros before looking at real data
        # self.model_freqtime.predict(np.zeros([1, self.nfreq, self.ntime, 1]))
        # self.model_dmtime.predict(np.zeros([1, self.ndm, self.ntime, 1]))

    def _classify(self, fname):
        """
        Classify a candidate

        :param str fname: Path to HDF5 file containing candiate data and metadata
        """
        self.logger.debug(f"Received classification request for {fname}")
