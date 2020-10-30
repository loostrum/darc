#!/usr/bin/env python3

import os
import socket
from argparse import Namespace
import numpy as np
import h5py
import yaml
import matplotlib.pyplot as plt
import astropy.units as u

from darc.definitions import CONFIG_FILE, BANDWIDTH


# redefine here because import from darc.processor would result in a circular import
class ProcessorException(Exception):
    pass


class Visualizer:
    """
    Visualize candidates
    """

    def __init__(self, logger, obs_config, files):
        """
        """
        self.logger = logger
        self.obs_config = obs_config
        self.files = np.array(files)

        self.config = self._load_config()

        self.logger.info(f"Starting visualization of task ID "
                         f"{self.obs_config['parset']['task.taskID']}: {self.obs_config['datetimesource']}")

        # switch the plot backend to pdf
        old_backend = plt.get_backend()
        # plt.switch_backend('PDF')
        self._visualize()
        # put back the old backend
        plt.switch_backend(old_backend)

    @staticmethod
    def _load_config():
        """
        Load configuration

        :return: config (Namespace)
        """
        with open(CONFIG_FILE, 'r') as f:
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

        # get plot order
        order = self._get_plot_order()
        # get the number of plot pages
        npage = int(np.ceil(len(order) / self.config.nplot_per_side ** 2))
        # split the input files per page and order them
        files_split = np.array_split(self.files[order], npage)

        for page in range(npage):
            for plot_type in ('freq_time', 'dm_time', '1d_time'):
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
                    # times = np.arange(ntime) * params['tsamp'] * 1e3

                    ax = axes[i]
                    xlabel = 'Time (ms)'
                    if plot_type == 'freq_time':
                        nfreq = data.shape[0]
                        ylabel = 'Frequency (MHz)'
                        title = 'p:{prob_freqtime:.2f} DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        freqs = np.linspace(0, BANDWIDTH.to(u.MHz).value, nfreq) + self.obs_config['min_freq']
                        X, Y = np.meshgrid(times, freqs)
                        ax.pcolormesh(X, Y, data, cmap=self.config.cmap_freqtime, shading='nearest')
                    elif plot_type == 'dm_time':
                        ylabel = r'DM (pc cm$^{-3}$)'
                        title = 'p:{prob_dmtime:.2f} DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        X, Y = np.meshgrid(times, params['dms'])
                        ax.pcolormesh(X, Y, data, cmap=self.config.cmap_dmtime, shading='nearest')
                    elif plot_type == '1d_time':
                        ylabel = 'Power (norm.)'
                        title = 'DM:{dm:.2f} t:{toa:.2f}\n' \
                                'S/N:{snr:.2f} width:{downsamp} SB:{sb}'.format(**params)
                        ax.plot(times, data, c=self.config.colour_1dtime)
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

                    # on the last page, disable the remaining plots if there are any
                    if page == npage - 1:
                        remainder = self.config.nplot_per_side ** 2 - len(files_split[page])
                        if remainder > 0:
                            for ax in axes[-remainder:]:
                                ax.axis('off')

                fig.tight_layout()
        plt.show()

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
            # load the data
            if data_type == 'freq_time':
                data = f['data_freq_time'][:]
            elif data_type == 'dm_time':
                data = f['data_dm_time'][:]
            elif data_type == '1d_time':
                data = f['data_freq_time'][:].sum(axis=0)
                data -= np.median(data)
                data /= np.amax(data)
            else:
                self.logger.error(f"Unknown data type: {data_type}")
                raise ProcessorException(f"Visualizer failed with unknown data type: {data_type}")
        return data, params
