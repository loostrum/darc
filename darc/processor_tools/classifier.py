#!/usr/bin/env python3
import os
import socket
from argparse import Namespace
from time import sleep
from queue import Empty
import multiprocessing as mp
import threading
import yaml
import numpy as np
import h5py

from darc.definitions import CONFIG_FILE
from darc.logger import get_queue_logger

# silence the tensorflow logger
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


class Classifier(mp.Process):
    """
    Classify candidates from HDF5 files produced by Extractor
    """

    def __init__(self, log_queue, input_queue, conn, config_file=CONFIG_FILE):
        """
        :param Queue log_queue: Queue to use for logging
        :param Queue input_queue: Input queue for triggers
        :param Connection conn: Pipe connection to send output to
        :param str config_file: Path to config file
        """
        super(Classifier, self).__init__()
        module_name = type(self).__module__.split('.')[-1]
        self.logger = get_queue_logger(module_name, log_queue)
        self.input_queue = input_queue
        self.conn = conn

        # load config
        self.config_file = config_file
        self.config = self._load_config()

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.model_freqtime = None
        self.model_dmtime = None
        self.data_freqtime = None
        self.data_dmtime = None
        self.nfreq_data = None
        self.ndm_data = None
        self.ntime_data = None
        self.candidates_to_visualize = []
        self.tf = None

    def _load_tensorflow(self):
        """
        Load tensorflow into local namespace
        """
        # import tensorflow here as apparently it isn't fork-safe, and results
        # in a "Could not retrieve CUDA device count" error when
        # this Process is forked from another Process
        import tensorflow
        self.tf = tensorflow
        # set GPU visible to classifier
        os.environ['CUDA_VISIBLE_DEVICES'] = str(self.config.gpu)

        # set memory growth parameter to avoid allocating all GPU memory
        # only one GPU is visible, so always selecting first GPU is fine
        # this is only available on tensorflow >= 2.0
        if int(self.tf.__version__[0]) >= 2:
            gpu = self.tf.config.experimental.list_physical_devices('GPU')[0]
            self.tf.config.experimental.set_memory_growth(gpu, True)
            # also silence the logger even more
            self.tf.get_logger().setLevel('ERROR')
        else:
            # for TF 1.X, create a session with the required growth parameter
            tf_config = self.tf.ConfigProto()
            tf_config.gpu_options.allow_growth = True
            self.tf.Session(config=tf_config)

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting classifier thread")

        # initalize tensorflow and models
        self._load_tensorflow()
        self._init_models()

        do_stop = False
        while not self.stop_event.is_set():
            # read file paths from input queue
            try:
                fname = self.input_queue.get(timeout=.1)
            except Empty:
                self.input_empty = True
                if do_stop:
                    # run stop in a thread, so processing can continue
                    self.logger.debug("Running stop")
                    thread = threading.Thread(target=self.stop)
                    thread.daemon = True
                    thread.start()
                    # then set do_stop to false, so it is not run a second time
                    do_stop = False
                continue
            else:
                self.input_empty = False
                if fname == 'stop':
                    do_stop = True
                else:
                    # do classification
                    self._classify(fname)
        self.logger.info("Stopping classifier thread")
        # send list of candidates to visualize to parent process
        self.conn.send(self.candidates_to_visualize)
        self.conn.close()

    def stop(self):
        """
        Stop this thread
        """
        # wait until the input queue is empty
        if not self.input_empty:
            self.logger.debug("Classifier waiting to finish processing")
        while not self.input_empty:
            sleep(1)
        # then stop
        self.stop_event.set()

    def _load_config(self):
        """
        Load configuration
        """
        with open(self.config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['classifier']
        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.format(**kwargs)
            # replace any -1 by infinity
            elif value == -1:
                config[key] = np.inf

        # return as Namespace so the keys can be accessed as attributes
        return Namespace(**config)

    def _init_models(self):
        """
        Load the keras models
        """
        # intialise analysis tools
        self.model_freqtime = self.tf.keras.models.load_model(os.path.join(self.config.model_dir,
                                                                           self.config.model_freqtime))
        self.model_dmtime = self.tf.keras.models.load_model(os.path.join(self.config.model_dir,
                                                                         self.config.model_dmtime))

        # The model's first prediction takes longer
        # pre-empt this by classifying an array of zeros before looking at real data
        self.model_freqtime.predict(np.zeros([1, self.config.nfreq, self.config.ntime, 1]))
        self.model_dmtime.predict(np.zeros([1, self.config.ndm, self.config.ntime, 1]))

    def _classify(self, fname):
        """
        Classify a candidate

        :param str fname: Path to HDF5 file containing candidate data and metadata
        """
        # load data
        with h5py.File(fname, 'r') as f:
            self.data_freq_time = f['data_freq_time'][:]
            self.data_dm_time = f['data_dm_time'][:]

            self.nfreq_data = f.attrs['nfreq']
            self.ntime_data = f.attrs['ntime']
            self.ndm_data = f.attrs['ndm']

        # prepare data: verify shape and scale as needed
        # returns False if something failed
        if not self._prepare_data():
            return

        # classify
        prob_freqtime = self.model_freqtime.predict(self.data_freq_time)[0, 1]
        prob_dmtime = self.model_dmtime.predict(self.data_dm_time)[0, 1]
        self.logger.debug(f"Probabilities: freqtime={prob_freqtime:.2f}, dmtime={prob_dmtime:.2f}, "
                          f"fname={os.path.basename(fname)}")

        # append the probabilities to the file
        with h5py.File(fname, 'a') as f:
            f.attrs.create('prob_freqtime', data=prob_freqtime)
            f.attrs.create('prob_dmtime', data=prob_dmtime)

        # if the probabilities are above threshold, store the file path
        if (prob_freqtime > self.config.thresh_freqtime) and (prob_dmtime > self.config.thresh_dmtime):
            self.candidates_to_visualize.append(fname)

    def _prepare_data(self):
        """
        Verify data shape and downsampled as needed

        :return: success (bool)
        """
        # verify shapes and downsample if needed
        # frequency axis
        if self.nfreq_data != self.config.nfreq:
            modulo, remainder = divmod(self.nfreq_data, self.config.nfreq)
            if remainder != 0:
                self.logger.error(f"Data nfreq {self.nfreq_data} must be multiple of model nfreq {self.config.nfreq}")
                return False
            # reshape the frequency axis
            self.logger.debug(f"Reshaping freq from {self.nfreq_data} to {self.config.nfreq}")
            self.data_freq_time = self.data_freq_time.reshape(self.config.nfreq, modulo, -1).mean(axis=1)

        # dm axis
        if self.ndm_data != self.config.ndm:
            modulo, remainder = divmod(self.ndm_data, self.config.ndm)
            if remainder != 0:
                self.logger.error(f"Data ndm {self.ndm_data} must be multiple of model ndm {self.config.ndm}")
                return False
            # reshape the dm axis
            self.logger.debug(f"Reshaping dm from {self.ndm_data} to {self.config.ndm}")
            self.data_dm_time = self.data_dm_time.reshape(self.config.dm, modulo, -1).mean(axis=1)

        # time axis
        if self.ntime_data != self.config.ntime:
            modulo, remainder = divmod(self.ntime_data, self.config.ntime)
            if remainder != 0:
                self.logger.error(f"Data ntime {self.ntime_data} must be multiple of model ntime {self.config.ntime}")
                return False
            # reshape the time axis of both data_freq_time and data_dm_time
            self.logger.debug(f"Reshaping time from {self.ntime_data} to {self.config.ntime}")
            self.data_freq_time = self.data_freq_time.reshape(self.config.nfreq,
                                                              self.config.ntime, modulo).mean(axis=2)
            self.data_dm_time = self.data_dm_time.reshape(self.config.ndm,
                                                          self.config.ntime, modulo).mean(axis=2)

        # scale data and add required axis for classifier
        self.data_freq_time -= np.median(self.data_freq_time)
        # silence the potential runtime warning due to divide-by-zero
        with np.errstate(invalid='ignore'):
            self.data_freq_time /= np.std(self.data_freq_time)
        self.data_freq_time[np.isnan(self.data_freq_time)] = 0.
        self.data_freq_time = self.data_freq_time[None, ..., None]

        self.data_dm_time -= np.median(self.data_dm_time)
        self.data_dm_time /= np.std(self.data_dm_time)
        self.data_dm_time = self.data_dm_time[None, ..., None]

        return True
