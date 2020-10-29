#!/usr/bin/env python3
#
# real-time data processor

import os
import socket
import copy
from argparse import Namespace
import threading
import multiprocessing as mp
from queue import Empty
from time import sleep
import yaml
import numpy as np
import astropy.units as u
from astropy.time import Time
import h5py

from darc import DARCBase
from darc import util
from darc.definitions import CONFIG_FILE, BANDWIDTH, TSAMP, NCHAN, TIME_UNIT
from darc.external import tools
from darc.processor_tools import ARTSFilterbankReader


class ProcessorException(Exception):
    pass


class ProcessorManager(DARCBase):
    """
    Control logic for running several Processor instances, one per observation
    """

    def __init__(self):
        """
        """
        super(ProcessorManager, self).__init__()

        self.observations = {}
        self.current_observation = None

        # create a thread scavenger
        self.scavenger = threading.Thread(target=self.thread_scavenger, name='scavenger')
        self.scavenger.start()

    def thread_scavenger(self):
        """
        Remove any finished threads at regular intervals
        """
        while not self.stop_event.is_set():
            for taskid, thread in self.observations.copy().items():
                if not thread.is_alive():
                    # if the thread is dead, remove it from the list
                    self.observations.pop(taskid)
                    self.logger.info(f"Scavenging thread of taskid {taskid}")

    def cleanup(self):
        """
        Upon stop of the manager, abort any remaining observations
        """
        # loop over dictionary items. Use copy to avoid changing dict in loop
        for taskid, obs in self.observations.copy().items():
            self.logger.info(f"Aborting observation with taskid {taskid}")
            obs.stop_observation(abort=True)
            obs.join()

    def start_observation(self, obs_config, reload=True):
        """
        Initialize a Processor and call its start_observation
        """
        if reload:
            self.load_config()

        # add parset to obs config
        obs_config['parset'] = self._load_parset(obs_config)
        # get task ID
        taskid = obs_config['parset']['task.taskID']

        self.logger.info(f"Starting observation with task ID {taskid}")

        # refuse to do anything if an observation with this task ID already exists
        if taskid in self.observations.keys():
            self.logger.error(f"Failed to start observation: task ID {taskid} already exists")
            return

        # initialize a Processor for this observation
        proc = Processor()
        proc.name = taskid
        # create a source queue
        proc.set_source_queue(mp.Queue())
        proc.start()
        # start the observation and store thread
        proc.start_observation(obs_config, reload)
        self.observations[taskid] = proc
        self.current_observation = proc
        return

    def stop_observation(self, obs_config):
        """
        Stop observation with task ID as given in parset

        :param dict obs_config: Observation config
        """
        # load the parset
        parset = self._load_parset(obs_config)
        # get task ID
        taskid = parset['task.taskID']
        # check if an observation with this task ID exists
        if taskid not in self.observations.keys():
            self.logger.error("Failed to stop observation: no such task ID {taskid}")

        # signal the processor of this observation to stop
        # this also calls its stop_observation method
        self.observations[taskid].stop()

    def process_command(self, command):
        """
        Forward any data from the input queue to the running observation
        """
        if self.current_observation is not None:
            self.current_observation.source_queue.put(command)
        else:
            self.logger.error("Data received but no observation is running - ignoring")
        return

    def _load_parset(self, obs_config):
        """
        Load the observation parset

        :param dict obs_config: Observation config
        :return: parset as dict
        """
        try:
            # encoded parset is already in config on master node
            # decode the parset
            raw_parset = util.decode_parset(obs_config['parset'])
            # convert to dict and store
            parset = util.parse_parset(raw_parset)
        except KeyError:
            self.logger.info("Observation parset not found in input config, looking for master parset")
            # Load the parset from the master parset file
            master_config_file = os.path.join(obs_config['master_dir'], 'parset', 'darc_master.parset')
            try:
                # Read raw config
                with open(master_config_file) as f:
                    master_config = f.read().strip()
                # Convert to dict
                master_config = util.parse_parset(master_config)
                # extract obs parset and decode
                raw_parset = util.decode_parset(master_config['parset'])
                parset = util.parse_parset(raw_parset)
            except Exception as e:
                self.logger.warning(
                    "Failed to load parset from master config file {}, "
                    "setting parset to None: {}".format(master_config_file, e))
                parset = None

        return parset


class Processor(DARCBase):
    """
    Real-time processing of candidates

    #. Clustering + thresholding
    #. Extract data from filterbank
    #. Run classifier

    After observation finishes, results are sent to the master node
    """

    def __init__(self):
        """
        """
        super(Processor, self).__init__()
        self.observation_running = False
        self.threads = {}
        self.amber_triggers = []
        self.hdr_mapping = {}
        self.obs_config = None

        # create queues
        self.clustering_queue = mp.Queue()
        self.extractor_queue = mp.Queue()
        self.classifier_queue = mp.Queue()
        self.all_queues = (self.clustering_queue, self.extractor_queue, self.classifier_queue)

        # intialise analysis tools
        # self.rtproc = realtime_tools.RealtimeProc()
        # self.model_freqtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_freqtime))
        # self.model_dmtime = frbkeras.load_model(os.path.join(self.model_dir, self.model_name_dmime))

        # For some reason, the model's first prediction takes a long time.
        # pre-empt this by classifying an array of zeros before looking at real data
        # self.model_freqtime.predict(np.zeros([1, self.nfreq, self.ntime, 1]))
        # self.model_dmtime.predict(np.zeros([1, self.ndm, self.ntime, 1]))

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: Command to process
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                self.amber_triggers.append(command['trigger'])
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def start_observation(self, obs_config, reload=True):
        """
        Parse obs config and start listening for amber triggers on queue

        :param dict obs_config: Observation configuration
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

        # clean any old triggers
        self.amber_triggers = []
        # set config
        self.obs_config = obs_config

        # create output dir
        output_dir = os.path.join('{output_dir}'.format(**obs_config), self.output_dir)
        try:
            util.makedirs(output_dir)
        except Exception as e:
            self.logger.error(f"Failed to create output directory {output_dir}: {e}")
            raise ProcessorException(f"Failed to create output directory {output_dir}: {e}")
        # run in output dir
        os.chdir(output_dir)

        self.observation_running = True  # this must be set before starting the processing thread

        # start processing
        thread = threading.Thread(target=self._read_and_process_data, name='processing')
        thread.daemon = True
        thread.start()
        self.threads['processing'] = thread

        # start clustering
        thread = Clustering(obs_config, self.logger, self.clustering_queue, self.extractor_queue)
        thread.name = 'clustering'
        thread.daemon = True
        thread.start()
        self.threads['clustering'] = thread

        # start extractor(s)
        self.logger.info(f"Starting {self.num_extractor} data extractors")
        for i in range(self.num_extractor):
            thread = Extractor(obs_config, self.logger, self.extractor_queue, self.classifier_queue)
            thread.name = f'extractor_{i}'
            thread.daemon = True
            thread.start()
            self.threads[f'extractor_{i}'] = thread

        self.logger.info("Observation started")

    def stop_observation(self, abort=False):
        """
        Stop observation

        :param bool abort: Whether or not to abort the observation
        """
        # set running to false
        self.observation_running = False
        # clear triggers
        self.amber_triggers = []
        # clear header
        self.hdr_mapping = {}
        # clear config
        self.obs_config = None
        # if abort, clear all queues
        if abort:
            for queue in self.all_queues:
                util.clear_queue(queue)
        # clear processing thread
        try:
            self.threads['processing'].join()
        except KeyError:
            # there was no processing thread
            pass
        # signal clustering to stop
        try:
            self.threads['clustering'].stop()
            self.threads['clustering'].join()
        except KeyError:
            pass
        # signal extractor(s) to stop
        for i in range(self.num_extractor):
            try:
                self.threads[f'extractor_{i}'].stop()
                self.threads[f'extractor_{i}'].join()
            except KeyError:
                pass
        self.threads = {}

    def _read_and_process_data(self):
        """
        Process incoming AMBER triggers
        """
        # main loop
        while self.observation_running and not self.stop_event.is_set():
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                triggers = self.amber_triggers

                self.amber_triggers = []
                # check for header (always, because it is received once for every amber instance)
                if not self.hdr_mapping:
                    for trigger in triggers:
                        if trigger.startswith('#'):
                            # read header, remove comment symbol
                            header = trigger.split()[1:]
                            self.logger.info("Received header: {}".format(header))
                            # Check if all required params are present and create mapping to col index
                            keys = ['beam_id', 'integration_step', 'time', 'DM', 'SNR']
                            for key in keys:
                                try:
                                    self.hdr_mapping[key] = header.index(key)
                                except ValueError:
                                    self.logger.error("Key missing from clusters header: {}".format(key))
                                    self.hdr_mapping = {}
                                    return

                # header should be present now
                if not self.hdr_mapping:
                    self.logger.error("First clusters received but header not found")
                    continue

                # remove headers from triggers (i.e. any trigger starting with #)
                triggers = [trigger for trigger in triggers if not trigger.startswith('#')]

                # triggers is empty if only header was received
                if not triggers:
                    self.logger.info("Only header received - Canceling processing")
                    continue

                # split strings and convert to numpy array
                try:
                    triggers = np.array(list(map(lambda val: val.split(), triggers)), dtype=float)
                except Exception as e:
                    self.logger.error("Failed to process triggers: {}".format(e))
                    continue

                # pick columns to feed to clustering algorithm
                triggers_for_clustering = triggers[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'],
                                                       self.hdr_mapping['time'], self.hdr_mapping['integration_step'],
                                                       self.hdr_mapping['beam_id'])]

                # put triggers on clustering queue
                self.threads['clustering'].input_queue.put(triggers_for_clustering)
            sleep(self.interval)


class Clustering(threading.Thread):
    """
    Clustering and thresholding of AMBER triggers
    """

    def __init__(self, obs_config, logger, input_queue, output_queue):
        """
        :param dict obs_config: Observation settings
        :param Logger logger: Processor logger object
        :param Queue input_queue: Input queue for triggers
        :param Queue output_queue: Output queue for clusters
        """
        super(Clustering, self).__init__()
        self.logger = logger
        self.obs_config = obs_config
        self.input_queue = input_queue
        self.output_queue = output_queue

        # set system parameters
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq'] * u.MHz + 0.5 * BANDWIDTH).to(u.GHz).value
        self.sys_params = {'dt': dt, 'delta_nu_MHz': chan_width, 'nu_GHz': cent_freq}

        # load config
        self.config = self._load_config()

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.output_file_handle = None

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting clustering thread")
        # open the output file (line-buffered)
        self.output_file_handle = open(self.config.output_file, 'w', buffering=1)
        # write header
        self.output_file_handle.write("#snr dm time downsamp sb\n")
        while not self.stop_event.is_set():
            # read triggers from input queue
            try:
                triggers = self.input_queue.get(timeout=.1)
            except Empty:
                self.input_empty = True
                continue
            else:
                self.input_empty = False
                # do clustering
                self._cluster(triggers)
        # close the output file
        self.output_file_handle.close()
        self.logger.info("Stopping clustering thread")

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
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['clustering']
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

    def _cluster(self, triggers):
        """
        Execute trigger clustering

        :param np.ndarray triggers: Input triggers, columns: DM, S/N, time,
                              integration_step, sb
        """
        # input columns are DM, SNR, time, integration step, SB
        # run clustering
        # ignored column is indices of kept events in original triggers
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _, ncand_per_cluster = \
            tools.get_triggers(triggers,
                               dm_min=self.config.dm_min, dm_max=self.config.dm_max,
                               sig_thresh=self.config.snr_min_clustering, t_window=self.config.clustering_window,
                               read_beam=True, return_clustcounts=True, sb_filter=self.config.sb_filter,
                               sb_filter_period_min=self.config.sb_filter_period_min,
                               sb_filter_period_max=self.config.sb_filter_period_max,
                               **self.sys_params)

        # apply S/N and width threshold to clusters
        mask = (np.array(cluster_downsamp) <= self.config.width_max) & (np.array(cluster_snr) >= self.config.snr_min)
        cluster_snr = np.array(cluster_snr)[mask]
        cluster_dm = np.array(cluster_dm)[mask]
        cluster_time = np.array(cluster_time)[mask]
        cluster_downsamp = np.array(cluster_downsamp)[mask].astype(int)
        cluster_sb = np.array(cluster_sb)[mask].astype(int)
        ncluster = len(cluster_snr)

        self.logger.info(f"Clustered {len(triggers)} triggers into {ncluster} clusters")

        # put the clusters on the output queue for further analysis
        # note the for-loop is effectively skipped if ncluster is zero
        for ind in range(ncluster):
            self.output_queue.put([cluster_dm[ind], cluster_snr[ind], cluster_time[ind], cluster_downsamp[ind],
                                   cluster_sb[ind]])
            # write the cluster info to the output file (different order to remain compatible with old files)
            self.output_file_handle.write(f"{cluster_snr[ind]:.2f} {cluster_dm[ind]:.2f} {cluster_time[ind]:.3f} "
                                          f"{cluster_downsamp[ind]:.0f} {cluster_sb[ind]:.0f}\n")
        return


class Extractor(threading.Thread):
    """
    Extract data from filterbank files
    """

    def __init__(self, obs_config, logger, input_queue, output_queue):
        """
        :param dict obs_config: Observation settings
        :param Logger logger: Processor logger object
        :param Queue input_queue: Input queue for clusters
        :param Queue output_queue: Output queue for classifier
        """
        super(Extractor, self).__init__()
        self.logger = logger
        self.obs_config = obs_config
        self.input_queue = input_queue
        self.output_queue = output_queue

        # load config
        self.config = self._load_config()

        # create directory for output data
        # this thread runs in output directory, so relative to current directory is fine
        util.makedirs('data')

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.filterbank_reader = None
        self.rfi_mask = np.array([], dtype=int)

        self.data = None
        self.data_dm_time = None

    def run(self):
        """
        Main loop
        """
        self.logger.info("Starting extractor thread")

        # initialize filterbank reader
        self.filterbank_reader = self.init_filterbank_reader()

        # load AMBER RFI mask
        freq = self.obs_config['freq']
        try:
            # note that filterbank stores highest-freq first, but AMBER masks use lowest-freq first
            self.rfi_mask = self.filterbank_reader.nfreq - \
                np.loadtxt(self.config.rfi_mask.replace('FREQ', str(freq))).astype(int)
        except OSError:
            self.logger.warning(f"No AMBER RFI mask found for {freq} MHz, not applying mask")
            self.rfi_mask = np.array([], dtype=int)

        while not self.stop_event.is_set():
            # read parameters of a trigger from input queue
            try:
                params = self.input_queue.get(timeout=1)
            except Empty:
                self.input_empty = True
                continue
            else:
                self.input_empty = False
                # do extraction
                self._extract(*params)
        self.logger.info("Stopping extractor thread")

    def stop(self):
        """
        Stop this thread
        """
        # wait until the input queue is empty
        if not self.input_empty:
            self.logger.debug("Extractor received stop but input queue not empty; waiting")
        while not self.input_empty:
            sleep(1)  # do not use event.wait here, as the whole point is to not stop until processing is done
        # then stop
        self.stop_event.set()

    @staticmethod
    def _load_config():
        """
        Load configuration
        """
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['extractor']
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

    def init_filterbank_reader(self):
        """
        Initialize the ARTS filterbank reader
        """
        # get file name from obs config
        fname = os.path.join(self.obs_config['output_dir'], 'filterbank', "CB{cb:02d}_{tab:02d}.fil")
        # wait until the filterbank files exist
        while not os.path.isfile(fname.format(cb=self.obs_config['beam'], tab=0)) and not self.stop_event.is_set():
            self.stop_event.wait(.1)
        return ARTSFilterbankReader(fname, self.obs_config['beam'])

    def _extract(self, dm, snr, toa, downsamp, sb):
        """
        Execute data extraction

        :param float dm: Dispersion Measure (pc/cc)
        :param float snr: AMBER S/N
        :param float toa: Arrival time at top of band (s)
        :param int downsamp: Downsampling factor
        :param int sb: Synthesized beam index
        """
        # keep track of the time taken to process this candidate
        timer_start = Time.now()

        # add units
        dm = dm * u.pc / u.cm**3
        toa = toa * u.s

        # If the intra-channel smearing timescale is several samples, downsample
        # by at most 1/4th of that factor
        chan_width = BANDWIDTH / float(NCHAN)
        t_smear = util.dm_to_smearing(dm, self.obs_config['freq'] * u.MHz, chan_width)
        predownsamp = max(1, int(t_smear.to(u.s).value / self.filterbank_reader.tsamp / 4))
        # calculate remaining downsampling to do after dedispersion
        postdownsamp = max(1, downsamp // predownsamp)
        # total downsampling factor (can be slightly different from original downsampling)
        self.logger.debug(f"Original dowsamp: {downsamp}, new downsamp: {predownsamp * postdownsamp}")
        downsamp_effective = predownsamp * postdownsamp
        tsamp_effective = self.filterbank_reader.tsamp * downsamp_effective

        # calculate start/end bin we need to load from filterbank
        # DM delay in units of samples
        dm_delay = util.dm_to_delay(dm, self.obs_config['min_freq'] * u.MHz,
                                    self.obs_config['min_freq'] * u.MHz + BANDWIDTH)
        sample_delay = dm_delay.to(u.s).value // self.filterbank_reader.tsamp
        # number of input bins to store in final data
        ntime = self.config.ntime * downsamp_effective
        start_bin = int(toa.to(u.s).value // self.filterbank_reader.tsamp - .5 * ntime)
        # ensure start bin is not before start of observation
        if start_bin < 0:
            self.logger.warning("Start bin before start of file, shifting")
            start_bin = 0
        # number of bins to load is number to store plus dm delay
        nbin = int(ntime + sample_delay)
        end_bin = start_bin + nbin

        # Wait if the filterbank does not have enough samples yet
        # first update filterbank parameters to have correct nsamp
        self.filterbank_reader.store_fil_params()
        while end_bin >= self.filterbank_reader.nsamp:
            # wait until this much of the filterbank should be present on disk
            # start time, plus last bin to load, plus extra delay
            # (because filterbank is not immediately flushed to disk)
            tstart = Time(self.obs_config['startpacket'] / TIME_UNIT, format='unix')
            twait = tstart + end_bin * self.filterbank_reader.tsamp * u.s + self.config.delay * u.s
            self.logger.debug(f"Waiting until {twait.isot} for filterbank data to be present on disk")
            util.sleepuntil_utc(twait, event=self.stop_event)
            # re-read the number of samples to check if the data are available now
            self.filterbank_reader.store_fil_params()
            # ensure we don't wait forever if there is an error in the filterbank writer
            # if we are past the end time of the observation, we give up
            tend = tstart + self.obs_config['duration'] * u.s + self.config.delay * u.s
            if Time.now() > tend:
                break

        # if start time before start of file, or end time beyond end of file, shift the start and end time
        # first update filterbank parameters to have correct nsamp
        self.filterbank_reader.store_fil_params()
        if end_bin >= self.filterbank_reader.nsamp:
            # if start time is also beyond the end of the file, we cannot process this candidate and give an error
            if start_bin >= self.filterbank_reader.nsamp:
                self.logger.error("Start bin beyond end of file, error in filterbank data? Skipping this candidate")
                # log time taken
                timer_end = Time.now()
                self.logger.info(f"Processed ToA={toa.value:.4f}, DM={dm.value:.2f} "
                                 f"in {(timer_end - timer_start).to(u.s):.0f}")
                return
            self.logger.warning("End bin beyond end of file, shifting")
            diff = end_bin - self.filterbank_reader.nsamp + 1
            start_bin -= diff

        # load the data. Store as attribute so it can be accessed from other methods (ok as we only run
        # one extraction at a time)
        self.data = self.filterbank_reader.load_single_sb(sb, start_bin, nbin)

        # apply AMBER RFI mask
        self.data[self.rfi_mask, :] = 0.

        # run rfi cleaning
        if self.config.rfi_clean_type is not None:
            self._rficlean()

        # apply predownsampling
        self.data.downsample(predownsamp)
        # subtract median
        self.data.data -= np.median(self.data.data, axis=1, keepdims=True)

        # calculate DM range to try, ensure detection DM is in the list
        # increase dm half range by 5 units for each ms of pulse width
        # add another unit for each 100 units of DM
        dm_halfrange = self.config.dm_halfrange * u.pc / u.cm ** 3 + \
            5 * tsamp_effective / 1000. * u.pc / u.cm ** 3 + \
            dm / 100
        if self.config.ndm % 2 == 0:
            dms = np.linspace(dm - dm_halfrange, dm + dm_halfrange, self.config.ndm + 1)[:-1]
        else:
            dms = np.linspace(dm - dm_halfrange, dm + dm_halfrange, self.config.ndm)
        # ensure DM does not go below zero by shifting the entire range if this happens
        mindm = min(dms)
        if mindm < 0:
            dms += mindm

        # Dedisperse and get max S/N
        snrmax = 0
        width_best = None
        dm_best = None
        # range of widths for S/N determination. Never go above 250 samples,
        # which is typically RFI even without pre-downsampling
        widths = np.arange(max(1, postdownsamp // 2), min(250, postdownsamp * 2))
        # initialize DM-time array
        self.data_dm_time = np.zeros((self.config.ndm, self.config.ntime))
        for dm_ind, dm_val in enumerate(dms):
            # copy the data and dedisperse
            data = copy.deepcopy(self.data)
            data.dedisperse(dm_val.to(u.pc / u.cm**3).value)
            # create timeseries
            timeseries = data.data.sum(axis=0)[:ntime]
            # get S/N from timeseries
            snr, width = util.calc_snr_matched_filter(timeseries, widths=widths)
            # store index if this is the highest S/N
            if snr > snrmax:
                snrmax = snr
                width_best = width * predownsamp
                dm_best = dm_val

            # apply any remaining downsampling
            data.downsample(postdownsamp)
            # cut of excess bins and store
            self.data_dm_time[dm_ind] = data.data.sum(axis=0)[:self.config.ntime]

        # if max S/N is below local threshold, skip this trigger
        if snrmax < self.config.snr_min_local or dm_best.to(u.pc / u.cm**3).value < self.config.dm_min:
            if snrmax < self.config.snr_min_local:
                self.logger.warning(f"Skipping trigger with S/N ({snrmax}) below local threshold")
            else:
                self.logger.warning(f"Skipping trigger with DM ({dm_best}) below local threshold")
            # log time taken
            timer_end = Time.now()
            self.logger.info(f"Processed ToA={toa.value:.4f}, DM={dm.value:.2f} in {(timer_end - timer_start).to(u.s):.0f}")
            return

        # extract freq-time of best DM, and roll data to put brightest pixel in center
        self.data.dedisperse(dm_best.to(u.pc / u.cm ** 3).value)
        # apply downsampling in time and freq
        self.data.downsample(postdownsamp)
        self.data.subband(nsub=self.config.nfreq)
        # roll data
        brightest_pixel = np.argmax(self.data.data.sum(axis=0))
        shift = self.config.ntime // 2 - brightest_pixel
        self.data.data = np.roll(self.data.data, shift, axis=1)
        # cut off extra bins
        self.data.data = self.data.data[:, :self.config.ntime]

        # calculate effective toa after applying shift
        toa_effective = toa + shift * tsamp_effective * u.s

        # create output file
        output_file = f'data/TOA{toa.value:.4f}_DM{dm.value:.2f}_DS{downsamp_effective:.0f}_SNR{snr:.0f}.hdf5'
        params_amber = (dm.value, snr, toa.value, downsamp)
        params_opt = (dm_best.value, snrmax, toa_effective.value, width_best)
        self._store_data(output_file, sb, tsamp_effective, dms, params_amber, params_opt)

        # put path to file on output queue to be picked up by classifier
        self.output_queue.put(output_file)

        # log time taken
        timer_end = Time.now()
        self.logger.info(f"Processed ToA={toa:.4f}, DM={dm.value:.2f} in {(timer_end - timer_start).to(u.s):.0f}")

    def _rficlean(self):
        """
        Clean data of RFI
        """
        # ToDo: clean up this code, currently it is a copy from triggers.py as-is
        dtmean = self.data.data.mean(axis=1)
        nfreq = self.data.data.shape[0]

        # cleaning in time
        if self.config.rfi_clean_type in ['time', 'both']:
            for i in range(self.config.rfi_n_iter_time):
                dfmean = np.mean(self.data.data, axis=0)
                stdevf = np.std(dfmean)
                medf = np.median(dfmean)
                maskf = np.where(np.abs(dfmean - medf) > self.config.rfi_threshold_time * stdevf)[0]
                # replace with mean spectrum
                self.data.data[:, maskf] = dtmean[:, None] * np.ones(len(maskf))[None]

        if self.config.rfi_clean_type == 'perchannel':
            for i in range(self.config.rfi_n_iter_time):
                dtmean = np.mean(self.data.data, axis=1, keepdims=True)
                dtsig = np.std(self.data.data, axis=1)
                for nu in range(nfreq):
                    d = dtmean[nu]
                    sig = dtsig[nu]
                    maskpc = np.where(np.abs(self.data.data[nu] - d) > self.config.rfi_threshold_time * sig)[0]
                    self.data.data[nu][maskpc] = d

        # Clean in frequency
        # remove bandpass by averaging over bin_size adjacent channels
        if self.config.rfi_clean_type in ['frequency', 'both']:
            for i in range(self.config.rfi_n_iter_frequency):
                dtmean_nobandpass = self.data.data.mean(1) - dtmean.reshape(-1, self.config.rfi_bin_size).mean(
                    -1).repeat(self.config.rfi_bin_size)
                stdevt = np.std(dtmean_nobandpass)
                medt = np.median(dtmean_nobandpass)
                maskt = np.abs(dtmean_nobandpass - medt) > self.config.rfi_threshold_frequency * stdevt
                self.data.data[maskt] = np.median(dtmean)

    def _store_data(self, fname, sb, tsamp, dms, params_amber, params_opt):
        """
        Save candidate to HDF5 file

        :param str fname: Output file
        :param int sb: Synthesized beam index
        :param float tsamp: Sampling time (s)
        :param Quantity dms: array of DMs used in DM-time array
        :param tuple params_amber: AMBER parameters: dm, snr, toa
        :param tuple params_opt: Optimized parameters: dm, snr, toa
        """
        nfreq, ntime = self.data.data.shape
        ndm = self.data_dm_time.shape[0]
        with h5py.File(fname, 'w') as f:
            f.create_dataset('data_freq_time', data=self.data.data)
            f.create_dataset('data_dm_time', data=self.data_dm_time)

            f.attrs.create('nfreq', data=nfreq)
            f.attrs.create('ntime', data=ntime)
            f.attrs.create('ndm', data=ndm)
            f.attrs.create('tsamp', data=tsamp)
            f.attrs.create('sb', data=sb)
            f.attrs.create('dms', data=dms.value)

            f.attrs.create('dm', data=params_opt[0])
            f.attrs.create('snr', data=params_opt[1])
            f.attrs.create('toa', data=params_opt[2])
            f.attrs.create('downsamp', data=params_opt[3])

            f.attrs.create('dm_amber', data=params_amber[0])
            f.attrs.create('snr_amber', data=params_amber[1])
            f.attrs.create('toa_amber', data=params_amber[2])
            f.attrs.create('downsamp_amber', data=params_amber[3])
