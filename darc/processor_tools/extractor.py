#!/usr/bin/env python3
import os
import socket
import copy
import threading
import multiprocessing as mp
from queue import Empty
from time import sleep
from argparse import Namespace
import numpy as np
from astropy.time import Time
import astropy.units as u
import yaml
import h5py

from darc import util
from darc.processor_tools import ARTSFilterbankReader
from darc.definitions import CONFIG_FILE, BANDWIDTH, NCHAN, TIME_UNIT


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
        tstart = Time(self.obs_config['startpacket'] / TIME_UNIT, format='unix')
        # wait until this much of the filterbank should be present on disk
        # start time, plus last bin to load, plus extra delay
        # (because filterbank is not immediately flushed to disk)
        twait = tstart + end_bin * self.filterbank_reader.tsamp * u.s + self.config.delay * u.s
        while end_bin >= self.filterbank_reader.nsamp:
            self.logger.debug(f"Waiting until {twait.isot} for filterbank data to be present on disk")
            util.sleepuntil_utc(twait, event=self.stop_event)
            # re-read the number of samples to check if the data are available now
            self.filterbank_reader.store_fil_params()
            # if we are past the end time of the observation, we give up and try shifting the end bin instead
            tend = tstart + self.obs_config['duration'] * u.s + self.config.delay * u.s
            if Time.now() > tend:
                break

        # if start time before start of file, or end time beyond end of file, shift the start/end time
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
