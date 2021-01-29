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
from darc.definitions import CONFIG_FILE, BANDWIDTH, NCHAN, TIME_UNIT, NTAB
from darc.logger import get_queue_logger


class Extractor(mp.Process):
    """
    Extract data from filterbank files
    """

    def __init__(self, obs_config, output_dir, log_queue, input_queue, output_queue, ncand_above_threshold,
                 config_file=CONFIG_FILE):
        """
        :param dict obs_config: Observation settings
        :param str output_dir: Output directory for data products
        :param Queue log_queue: Queue to use for logging
        :param Queue input_queue: Input queue for clusters
        :param Queue output_queue: Output queue for classifier
        :param mp.Value ncand_above_threshold: 0
        :param str config_file: Path to config file
        """
        super(Extractor, self).__init__()
        module_name = type(self).__module__.split('.')[-1]
        self.logger = get_queue_logger(module_name, log_queue)
        self.output_dir = os.path.join(output_dir, 'data')
        self.obs_config = obs_config
        self.input_queue = input_queue
        self.output_queue = output_queue

        # load config
        self.config_file = config_file
        self.config = self._load_config()

        # create directory for output data
        util.makedirs(self.output_dir)

        # create stop event
        self.stop_event = mp.Event()

        self.input_empty = False
        self.filterbank_reader = None
        self.rfi_mask = np.array([], dtype=int)

        self.data = None
        self.data_dm_time = None
        self.ncand_above_threshold = ncand_above_threshold

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
            self.rfi_mask = self.filterbank_reader.header.nchans - 1 - \
                np.loadtxt(self.config.rfi_mask.replace('FREQ', str(freq))).astype(int)
        except OSError:
            self.logger.warning(f"No AMBER RFI mask found for {freq} MHz, not applying mask")
            self.rfi_mask = np.array([], dtype=int)

        do_stop = False
        while not self.stop_event.is_set():
            # read parameters of a trigger from input queue
            try:
                params = self.input_queue.get(timeout=1)
            except Empty:
                self.input_empty = True
                if do_stop:
                    # run stop in a thread, so processing can continue
                    thread = threading.Thread(target=self.stop)
                    thread.daemon = True
                    thread.start()
                    # then set do_stop to false, so it is not run a second time
                    do_stop = False
                continue
            else:
                self.input_empty = False
                if isinstance(params, str):
                    if params == f'stop_{self.name}':
                        # this extractor should stop
                        do_stop = True
                    elif params.startswith('stop'):
                        # another extractor should stop, put the message back on the queue
                        self.input_queue.put(params)
                        # sleep for a bit to avoid this extractor picking up the same command again
                        sleep(.1)
                else:
                    # do extraction
                    self._extract(*params)
        self.logger.info("Stopping extractor thread")

    def stop(self):
        """
        Stop this thread
        """
        # wait until the input queue is empty
        if not self.input_empty:
            self.logger.debug("Extractor waiting to finish processing")
        while not self.input_empty:
            sleep(1)  # do not use event.wait here, as the whole point is to not stop until processing is done
        # then stop
        self.stop_event.set()

    def _load_config(self):
        """
        Load configuration
        """
        with open(self.config_file, 'r') as f:
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
        predownsamp = max(1, int(t_smear.to(u.s).value / self.filterbank_reader.header.tsamp / 4))
        # calculate remaining downsampling to do after dedispersion
        postdownsamp = max(1, downsamp // predownsamp)
        # total downsampling factor (can be slightly different from original downsampling)
        self.logger.debug(f"Original dowsamp: {downsamp}, new downsamp: {predownsamp * postdownsamp} "
                          f"for ToA={toa.value:.4f}, DM={dm.value:.2f}")
        downsamp_effective = predownsamp * postdownsamp
        tsamp_effective = self.filterbank_reader.header.tsamp * downsamp_effective

        # calculate start/end bin we need to load from filterbank
        # DM delay in units of samples
        dm_delay = util.dm_to_delay(dm, self.obs_config['min_freq'] * u.MHz,
                                    self.obs_config['min_freq'] * u.MHz + BANDWIDTH)
        sample_delay = dm_delay.to(u.s).value // self.filterbank_reader.header.tsamp
        # number of input bins to store in final data
        ntime = self.config.ntime * downsamp_effective
        start_bin = int(toa.to(u.s).value // self.filterbank_reader.header.tsamp - .5 * ntime)
        # ensure start bin is not before start of observation
        if start_bin < 0:
            self.logger.warning(f"Start bin before start of file, shifting for ToA={toa.value:.4f}, DM={dm.value:.2f}")
            start_bin = 0
        # number of bins to load is number to store plus dm delay
        nbin = int(ntime + sample_delay)
        end_bin = start_bin + nbin

        # Wait if the filterbank does not have enough samples yet
        # first update filterbank parameters to have correct nsamp
        self.filterbank_reader.get_header(NTAB - 1)
        tstart = Time(self.obs_config['startpacket'] / TIME_UNIT, format='unix')
        # wait until this much of the filterbank should be present on disk
        # start time, plus last bin to load, plus extra delay
        # (because filterbank is not immediately flushed to disk)
        twait = tstart + end_bin * self.filterbank_reader.header.tsamp * u.s + self.config.delay * u.s
        first_loop = True
        while end_bin >= self.filterbank_reader.header.nsamples:
            if first_loop:
                # wait until data should be present on disk
                self.logger.debug(f"Waiting until at least {twait.isot} for filterbank data to be present on disk "
                                  f"for ToA={toa.value:.4f}, DM={dm.value:.2f}")
                util.sleepuntil_utc(twait, event=self.stop_event)
                first_loop = False
            else:
                # data should be there, but is not. Wait just a short time, then check again
                self.stop_event.wait(.5)
            # re-read the number of samples to check if the data are available now
            self.filterbank_reader.get_header(NTAB - 1)
            # if we are past the end time of the observation, we give up and try shifting the end bin instead
            tend = tstart + self.obs_config['duration'] * u.s + self.config.delay * u.s
            if Time.now() > tend:
                break

        # if start time before start of file, or end time beyond end of file, shift the start/end time
        # first update filterbank parameters to have correct nsamp
        self.filterbank_reader.get_header(NTAB - 1)
        if end_bin >= self.filterbank_reader.header.nsamples:
            # if start time is also beyond the end of the file, we cannot process this candidate and give an error
            if start_bin >= self.filterbank_reader.header.nsamples:
                timer_end = Time.now()
                self.logger.error(f"Start bin beyond end of file, error in filterbank data? Skipping "
                                  f"ToA={toa.value:.4f}, DM={dm.value:.2f}. Processed in "
                                  f"{(timer_end - timer_start).to(u.s):.0f}")
                return
            self.logger.warning(f"End bin beyond end of file, shifting for ToA={toa.value:.4f}, DM={dm.value:.2f}")
            diff = end_bin - self.filterbank_reader.header.nsamples + 1
            start_bin -= diff

        # load the data. Store as attribute so it can be accessed from other methods (ok as we only run
        # one extraction at a time)
        try:
            self.data = self.filterbank_reader.load_single_sb(sb, start_bin, nbin)
        except ValueError as e:
            timer_end = Time.now()
            self.logger.error(f"Failed to load filterbank data for ToA={toa.value:.4f}, DM={dm.value:.2f}: {e}. "
                              f"Processed in {(timer_end - timer_start).to(u.s):.0f}")
            return

        # apply AMBER RFI mask
        if self.config.rfi_apply_mask:
            self.data[self.rfi_mask, :] = 0.

        # run rfi cleaning
        if self.config.rfi_clean_type is not None:
            self._rficlean()

        # apply predownsampling
        self.data.downsample(predownsamp)
        # subtract median
        self.data.data -= np.median(self.data.data, axis=1, keepdims=True)

        # get S/N and width at DM=0 and at AMBER DM
        # timeseries at DM=0
        timeseries_dm0 = self.data.data.sum(axis=0)[:ntime]
        self.data.dedisperse(dm.to(u.pc / u.cm ** 3).value)
        # create timeseries
        timeseries = self.data.data.sum(axis=0)[:ntime]
        # get S/N and width
        # range of widths for S/N determination. Never go above 250 samples,
        # which is typically RFI even without pre-downsampling
        widths = np.arange(max(1, postdownsamp // 2), min(250, postdownsamp * 2))
        if len(widths) == 0:
            timer_end = Time.now()
            self.logger.error(f"Downsampling out of range; valid range: 1 to 250. "
                              f"ToA={toa.value:.4f}, DM={dm.value:.2f}. "
                              f"Processed in {(timer_end - timer_start).to(u.s):.0f}")
            return
        snr_dm0, width_dm0 = util.calc_snr_matched_filter(timeseries_dm0, widths=widths)
        snrmax, width_best = util.calc_snr_matched_filter(timeseries, widths=widths)
        # correct for already applied downsampling
        width_best *= predownsamp
        width_dm0 *= predownsamp

        # if max S/N is below local threshold, skip this trigger
        if snrmax < self.config.snr_min_local:
            # log time taken
            timer_end = Time.now()
            self.logger.debug(f"Skipping trigger with S/N ({snrmax:.2f}) below local threshold, "
                              f"ToA={toa.value:.4f}, DM={dm.value:.2f}. "
                              f"Processed in {(timer_end - timer_start).to(u.s):.0f}")
            return

        # if S/N at DM=0 is higher than at candidate DM by some amount, skip this trigger
        # (only if this filter is enabled)
        if self.config.snr_dm0_filter and (snr_dm0 - snrmax >= self.config.snr_dm0_diff_threshold):
            # log time taken
            timer_end = Time.now()
            self.logger.debug(f"Skipping trigger with S/N ({snrmax:.2f}) lower than at DM=0 (S/N={snr_dm0:.2f}), "
                              f"ToA={toa.value:.4f}, DM={dm.value:.2f}. "
                              f"Processed in {(timer_end - timer_start).to(u.s):.0f}")
            return

        # calculate DM range to try
        # increase dm half range by 5 units for each ms of pulse width
        # add another unit for each 100 units of DM
        dm_halfrange = self.config.dm_halfrange * u.pc / u.cm ** 3 + \
            5 * tsamp_effective / 1000. * u.pc / u.cm ** 3 + \
            dm / 100
        # get dms, ensure detection DM is in the list
        if self.config.ndm % 2 == 0:
            dms = np.linspace(dm - dm_halfrange, dm + dm_halfrange, self.config.ndm + 1)[:-1]
        else:
            dms = np.linspace(dm - dm_halfrange, dm + dm_halfrange, self.config.ndm)
        # ensure DM does not go below zero by shifting the entire range if this happens
        mindm = min(dms)
        if mindm < 0:
            dms -= mindm

        # Dedisperse
        # initialize DM-time array
        self.data_dm_time = np.zeros((self.config.ndm, self.config.ntime))
        # work on copy of global data as all these small sample
        # shifts may introduce a few zeroes of padding
        data = copy.deepcopy(self.data)
        for dm_ind, dm_val in enumerate(dms):
            # dedisperse
            data.dedisperse(dm_val.to(u.pc / u.cm**3).value)
            ts = data.data.sum(axis=0)
            # apply any remaining downsampling
            # note: do not do this in-place on self.data as
            # next iteration needs full-resolution data
            numsamp, remainder = divmod(len(ts), postdownsamp)
            ts = ts[:-remainder].reshape(numsamp, -1).sum(axis=1)
            # cut of excess bins and store
            self.data_dm_time[dm_ind] = ts[:self.config.ntime]

        # apply downsampling in time and freq to global freq-time data
        self.data.downsample(postdownsamp)
        self.data.subband(nsub=self.config.nfreq)
        # cut off extra bins
        self.data.data = self.data.data[:, :self.config.ntime]
        # roll data to put brightest pixel in the centre
        brightest_pixel = np.argmax(self.data.data.sum(axis=0))
        shift = self.config.ntime // 2 - brightest_pixel
        self.data.data = np.roll(self.data.data, shift, axis=1)
        # apply the same roll to the DM-time data
        for row in range(self.config.ndm):
            self.data_dm_time[row] = np.roll(self.data_dm_time[row], shift)

        # calculate effective toa after applying shift
        # ToDo: only apply this shift if not caused by candidate near start/end of observation
        toa_effective = toa + shift * tsamp_effective * u.s

        # create output file
        output_file = os.path.join(self.output_dir,
                                   f'TOA{toa_effective.value:.4f}_DM{dm.value:.2f}_'
                                   f'DS{width_best:.0f}_SNR{snrmax:.0f}.hdf5')
        params_amber = (dm.value, snr, toa.value, downsamp)
        params_opt = (snrmax, toa_effective.value, width_best)
        self._store_data(output_file, sb, tsamp_effective, dms, params_amber, params_opt)

        # put path to file on output queue to be picked up by classifier
        self.output_queue.put(output_file)

        with self.ncand_above_threshold.get_lock():
            self.ncand_above_threshold.value += 1

        # log time taken
        timer_end = Time.now()
        self.logger.info(f"Extracted ToA={toa.value:.4f}, DM={dm.value:.2f} in "
                         f"{(timer_end - timer_start).to(u.s):.0f}")

    def _rficlean(self):
        """
        Clean data of RFI
        """
        # ToDo: check if this code needs cleanup / can be sped up, currently it is a copy from triggers.py as-is
        dtmean = self.data.data.mean(axis=1, keepdims=True)
        nfreq = self.data.data.shape[0]

        # cleaning in time
        if self.config.rfi_clean_type in ['time', 'both']:
            for i in range(self.config.rfi_n_iter_time):
                dfmean = np.mean(self.data.data, axis=0)
                stdevf = np.std(dfmean)
                medf = np.median(dfmean)
                maskf = np.where(np.abs(dfmean - medf) > self.config.rfi_threshold_time * stdevf)[0]
                # if there is nothing to mask, we are done
                if not np.any(maskf):
                    break
                # replace with mean spectrum
                self.data.data[:, maskf] = dtmean * np.ones(len(maskf))[None]

        if self.config.rfi_clean_type == 'perchannel':
            for i in range(self.config.rfi_n_iter_time):
                dtsig = np.std(self.data.data, axis=1)
                done = True
                for nu in range(nfreq):
                    d = dtmean[nu]
                    sig = dtsig[nu]
                    maskpc = np.where(np.abs(self.data.data[nu] - d) > self.config.rfi_threshold_time * sig)[0]
                    # do another iteration only if any channel was changed
                    if np.any(maskpc):
                        done = False
                    self.data.data[nu][maskpc] = d
                if done:
                    break

        # Clean in frequency
        # remove bandpass by averaging over bin_size adjacent channels
        if self.config.rfi_clean_type in ['frequency', 'both']:
            for i in range(self.config.rfi_n_iter_frequency):
                dtmean_nobandpass = self.data.data.mean(1) - dtmean.reshape(-1, self.config.rfi_bin_size).mean(
                    -1).repeat(self.config.rfi_bin_size)
                stdevt = np.std(dtmean_nobandpass)
                medt = np.median(dtmean_nobandpass)
                maskt = np.abs(dtmean_nobandpass - medt) > self.config.rfi_threshold_frequency * stdevt
                # if there is nothing to mask, we are done
                if not np.any(maskt):
                    break
                self.data.data[maskt] = np.median(dtmean)

    def _store_data(self, fname, sb, tsamp, dms, params_amber, params_opt):
        """
        Save candidate to HDF5 file

        :param str fname: Output file
        :param int sb: Synthesized beam index
        :param float tsamp: Sampling time (s)
        :param Quantity dms: array of DMs used in DM-time array
        :param tuple params_amber: AMBER parameters: dm, snr, toa
        :param tuple params_opt: Optimized parameters: snr, toa
        """
        nfreq, ntime = self.data.data.shape
        ndm = self.data_dm_time.shape[0]
        with h5py.File(fname, 'w') as f:
            f.create_dataset('data_freq_time', data=self.data.data[::-1])  # store as lowest-freq first
            f.create_dataset('data_dm_time', data=self.data_dm_time)

            f.attrs.create('nfreq', data=nfreq)
            f.attrs.create('ntime', data=ntime)
            f.attrs.create('ndm', data=ndm)
            f.attrs.create('tsamp', data=tsamp)
            f.attrs.create('sb', data=sb)
            f.attrs.create('dms', data=dms.value)

            f.attrs.create('dm', data=params_amber[0])  # DM is only an AMBER parameter
            f.attrs.create('snr', data=params_opt[0])
            f.attrs.create('toa', data=params_opt[1])
            f.attrs.create('downsamp', data=params_opt[2])

            f.attrs.create('dm_amber', data=params_amber[0])  # this is duplicate, but there for completeness
            f.attrs.create('snr_amber', data=params_amber[1])
            f.attrs.create('toa_amber', data=params_amber[2])
            f.attrs.create('downsamp_amber', data=params_amber[3])
