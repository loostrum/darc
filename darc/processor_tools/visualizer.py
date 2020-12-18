#!/usr/bin/env python3

import os
import logging
import glob
from shutil import copy
import socket
from argparse import Namespace
import numpy as np
import h5py
import yaml
import matplotlib.pyplot as plt
import astropy.units as u
from PyPDF4 import PdfFileMerger

from darc.definitions import CONFIG_FILE, BANDWIDTH
from darc import util
from darc.logger import get_queue_logger


# disable debug log messages from matplotlib
logging.getLogger('matplotlib').setLevel(logging.ERROR)


# redefine here because import from darc.processor would result in a circular import
class ProcessorException(Exception):
    pass


class Visualizer:
    """
    Visualize candidates
    """

    def __init__(self, output_dir, result_dir, log_queue, obs_config, files, config_file=CONFIG_FILE):
        """
        :param str output_dir: Output directory for data products
        :param str result_dir: central directory to copy output PDF to
        :param Queue log_queue: Queue to use for logging
        :param dict obs_config: Observations settings
        :param list files: HDF5 files to visualize
        :param str config_file: Path to config file
        """
        module_name = type(self).__module__.split('.')[-1]
        self.output_dir = output_dir
        self.result_dir = result_dir
        self.logger = get_queue_logger(module_name, log_queue)
        self.obs_config = obs_config
        self.files = np.array(files)

        # load config
        self.config_file = config_file
        self.config = self._load_config()

        self.logger.info(f"Starting visualization of task ID "
                         f"{self.obs_config['parset']['task.taskID']}: {self.obs_config['datetimesource']}")
        # switch the plot backend to pdf
        old_backend = plt.get_backend()
        plt.switch_backend('PDF')
        try:
            self._visualize()
        except Exception as e:
            self.logger.error(f"Visualization of task ID {self.obs_config['parset']['task.taskID']} failed: "
                              f"{type(e)}: {e}")
        # put back the old backend
        plt.switch_backend(old_backend)

    def _load_config(self):
        """
        Load configuration

        :return: config (Namespace)
        """
        with open(self.config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['visualizer']
        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.format(**kwargs)

        # return as Namespace so the keys can be accessed as attributes
        return Namespace(**config)

    def _visualize(self):
        """
        Run the visualization of candidates
        """
        ncand = len(self.files)
        self.logger.debug(f"Visualizing {ncand} candidates")

        # get max galactic DM
        dmgal = util.get_ymw16(self.obs_config['parset'], self.obs_config['beam'], self.logger)
        # DMgal is zero if something failed, in that case set the value to infinity so no plots are marked, instead of
        # all
        if dmgal == 0:
            dmgal = np.inf

        # get plot order
        order = self._get_plot_order()
        # get the number of plot pages
        nplot_per_page = self.config.nplot_per_side ** 2
        npage = int(np.ceil(len(order) / nplot_per_page))
        # order files, then split per page
        try:
            files = self.files[order]
        except IndexError:
            self.logger.error("Failed to get plot order")
            return

        num_full_page, nplot_last_incomplete_page = divmod(len(files), nplot_per_page)
        files_split = []
        for page in range(num_full_page):
            files_split.append(files[page * nplot_per_page: (page + 1) * nplot_per_page])
        if nplot_last_incomplete_page != 0:
            files_split.append(files[-nplot_last_incomplete_page:])

        for page in range(npage):
            for plot_type in self.config.plot_types:
                # create figure
                fig, axes = plt.subplots(nrows=self.config.nplot_per_side, ncols=self.config.nplot_per_side,
                                         figsize=(self.config.figsize, self.config.figsize))
                axes = axes.flatten()
                # loop over the files
                for i, fname in enumerate(files_split[page]):
                    # load the data and parameters
                    data, params = self._load_data(fname, plot_type)
                    try:
                        ntime = data.shape[1]
                    except IndexError:
                        ntime = len(data)
                    times = np.arange(-ntime / 2, ntime / 2) * params['tsamp'] * 1e3

                    ax = axes[i]
                    xlabel = 'Time (ms)'
                    if plot_type == 'freq_time':
                        nfreq = data.shape[0]
                        ylabel = 'Frequency (MHz)'
                        title = 'p:{prob_freqtime:.2f} DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        freqs = np.linspace(0, BANDWIDTH.to(u.MHz).value, nfreq) + self.obs_config['min_freq']
                        X, Y = np.meshgrid(times, freqs)
                        ax.pcolormesh(X, Y, data, cmap=self.config.cmap_freqtime, shading='nearest',
                                      rasterized=True)
                        # Add DM 0 curve
                        delays = util.dm_to_delay(params['dm'] * u.pc / u.cm ** 3,
                                                  freqs[0] * u.MHz, freqs * u.MHz).to(u.ms).value
                        ax.plot(times[0] + delays, freqs, c='r', alpha=.5, rasterized=True)
                    elif plot_type == 'dm_time':
                        ylabel = r'DM (pc cm$^{-3}$)'
                        title = 'p:{prob_dmtime:.2f} DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        X, Y = np.meshgrid(times, params['dms'])
                        ax.pcolormesh(X, Y, data, cmap=self.config.cmap_dmtime, shading='nearest',
                                      rasterized=True)
                        # add line if DM 0 is in plot range
                        if min(params['dms']) <= 0 <= max(params['dms']):
                            ax.axhline(0, c='r', alpha=.5, rasterized=True)
                    elif plot_type == '1d_time':
                        ylabel = 'Power (norm.)'
                        title = 'DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        ax.plot(times, data, c=self.config.colour_1dtime, rasterized=True)
                    else:
                        raise ProcessorException(f"Unknown plot type: {plot_type}, should not be able to get here!")

                    # add plot title
                    ax.set_title(title)
                    # ylabel only the first column
                    if ax.is_first_col():
                        ax.set_ylabel(ylabel)
                    # xlabel only the last row. This is a bit tricky: on the last page, this is not necessarily
                    # the last possible row
                    if (page != npage - 1) and ax.is_last_row():
                        ax.set_xlabel(xlabel)
                    else:
                        # a plot is the bottom one in a column if the number of remaining plots is less than a full row
                        nplot_remaining = len(files_split[page]) - i - 1
                        if nplot_remaining < self.config.nplot_per_side:
                            ax.set_xlabel(xlabel)
                    ax.set_xlim(times[0], times[-1])
                    # add red border if DM > DMgal
                    if params['dm'] > dmgal:
                        plt.setp(ax.spines.values(), color='red', linewidth=2, alpha=0.85)

                    # on the last page, disable the remaining plots if there are any
                    if page == npage - 1:
                        remainder = nplot_per_page - nplot_last_incomplete_page
                        if remainder > 0:
                            for ax in axes[-remainder:]:
                                ax.axis('off')

                fig.suptitle(f"Task ID {self.obs_config['parset']['task.taskID']} - "
                             f"{self.obs_config['datetimesource']} - CB{self.obs_config['beam']:02d}")
                fig.set_tight_layout({'rect': [0, 0.03, 1, 0.97]})
                # ensure the number of digits used for the page index is always the same, and large enough
                # then sorting works as expected
                page_str = str(page).zfill(len(str(npage)))
                fig_fname = os.path.join(self.output_dir, f'ranked_{plot_type}_{page_str}.pdf')
                fig.savefig(fig_fname)
        # merge the plots
        output_file = f"{self.output_dir}/CB{self.obs_config['beam']:02d}.pdf"
        merger = PdfFileMerger()
        for plot_type in self.config.plot_types:
            fnames = glob.glob(f'{self.output_dir}/*{plot_type}*.pdf')
            fnames.sort()
            for fname in fnames:
                merger.append(fname)
        merger.write(output_file)
        # copy the file to the central output directory
        self.logger.info(f"Saving plots to {self.result_dir}/{os.path.basename(output_file)}")
        copy(output_file, self.result_dir)

    def _get_plot_order(self):
        """
        Get the order of files to plot them in descending freq-time probability order,
        then by S/N if probabilities are equal

        :return: file order (np.ndarray)
        """
        params = []
        for fname in self.files:
            with h5py.File(fname, 'r') as f:
                try:
                    prob = f.attrs['prob_freqtime']
                    snr = f.attrs['snr']
                except KeyError:
                    self.logger.error(f"Failed to get probability or S/N from {fname}, skipping file")
                    prob = np.nan
                    snr = np.nan
                params.append((prob, snr))

        # sort by probability, then S/N, in descending order
        params = np.array(params, dtype=[('p', '<f8'), ('snr', '<f8')])
        order = np.argsort(params, order=('p', 'snr'))[::-1]
        # remove nans
        nans = np.where(np.isnan(params['p']))[0]
        order = np.array([val for val in order if val not in nans])
        return order

    def _load_data(self, fname, data_type):
        """
        Load HDF5 data

        :param str fname: Path to HDF5 file
        :param data_type: which data type to get, options: freq_time, dm_time, 1d_time

        :return: data (np.ndarray), params (dict with tsamp, dm, snr, toa, downsamp, sb, dms)
        """

        params = {}
        with h5py.File(fname, 'r') as f:
            # load the optimized parameters
            for key in ('tsamp', 'dm', 'snr', 'toa', 'downsamp', 'sb', 'dms', 'prob_freqtime', 'prob_dmtime'):
                try:
                    params[key] = f.attrs[key]
                except KeyError:
                    self.logger.error(r"Failed to load key {key} from {fname}")
            # load and scale the data
            if data_type == 'freq_time':
                data = f['data_freq_time'][:]
                data -= np.median(data, axis=1, keepdims=True)
                # silence the potential runtime warning due to divide-by-zero
                with np.errstate(invalid='ignore'):
                    data /= np.std(data, axis=1, keepdims=True)
                data[~np.isfinite(data)] = np.nan
            elif data_type == 'dm_time':
                data = f['data_dm_time'][:]
                data -= np.median(data, axis=1, keepdims=True)
            elif data_type == '1d_time':
                data = f['data_freq_time'][:].sum(axis=0)
                data -= np.median(data)
                data /= np.amax(data)
            else:
                self.logger.error(f"Unknown data type: {data_type}")
                raise ProcessorException(f"Visualizer failed with unknown data type: {data_type}")
        return data, params
