#!/usr/bin/env python3
import logging
import numpy as np
from blimpy import Waterfall

from darc import SBGenerator
from darc.processor_tools.spectra import Spectra
from darc.definitions import NTAB

# set blimpy to only log errors
logging.getLogger("blimpy").setLevel(logging.ERROR)


class ARTSFilterbankReaderError(Exception):
    pass


class ARTSFilterbankReader:
    def __init__(self, fname, cb, ntab=NTAB):
        """
        Filterbank reader for ARTS data, one file per TAB

        :param str fname: path to filterbank files, with {cb:02d} and {tab:02d} for CB and TAB indices
        :param int cb: CB index
        :param int ntab: Number of TABs (Default: NTAB from constants)
        """
        # initialize the SB Generator for SC4
        self.sb_generator = SBGenerator.from_science_case(4)

        self.ntab = ntab
        self.fnames = [fname.format(cb=cb, tab=tab) for tab in range(ntab)]
        self.store_fil_params()

        self.tab_data = None
        self.startbin = None
        self.chunksize = None
        self.times = None

    def store_fil_params(self):
        """
        Store filterbank parameters as attributes
        """
        self.nfreq, self.freqs, self.nsamp, self.tsamp = self.get_fil_params(tab=0)

    def get_fil_params(self, tab=0):
        """
        Read filterbank parameters

        :param int tab: TAB index (Default: 0)
        :return: nfreq (int), freqs (array), nsamp (int), tsamp (float)
        """
        fil = Waterfall(self.fnames[tab], load_data=False)
        # read data shape
        nsamp, _, nfreq = fil.file_shape
        # construct frequency axis
        freqs = np.arange(fil.header['nchans']) * fil.header['foff'] + fil.header['fch1']
        # set SB generator order
        if fil.header['foff'] < 0:
            self.sb_generator.reversed = True
        else:
            self.sb_generator.reversed = False

        return nfreq, freqs, nsamp, fil.header['tsamp']

    def read_filterbank(self, tab, startbin, chunksize):
        """
        Read a chunk of filterbank data

        :param int tab: TAB index
        :param int startbin: Index of first time sample to read
        :param int chunksize: Number of time samples to read
        :return: chunk of data with shape (nfreq, chunksize)
        """
        fil = Waterfall(self.fnames[tab], load_data=False)
        # read chunk of data
        fil.read_data(t_start=startbin, t_stop=startbin + chunksize)
        # keep only time and freq axes, transpose to have frequency first
        data = fil.data[:, 0, :].T.astype(float)
        return data

    def read_tabs(self, startbin, chunksize, tabs=None):
        """
        Read TAB data

        :param int startbin: Index of first time sample to read
        :param int chunksize: Number of time samples to read
        :param list tabs: which TABs to read (Default: all)
        """
        tab_data = np.zeros((self.ntab, self.nfreq, chunksize))
        if tabs is None:
            tabs = range(self.ntab)
        for tab in tabs:
            tab_data[tab] = self.read_filterbank(tab, startbin, chunksize)
        self.tab_data = tab_data
        self.startbin = startbin
        self.chunksize = chunksize
        self.times = np.arange(chunksize) * self.tsamp

    def get_sb(self, sb):
        """
        Construct an SB. TAB data must be read before calling this method

        :param int sb: SB index
        :return: Spectra object with SB data
        """
        if self.tab_data is None:
            raise ARTSFilterbankReaderError(f"No TAB data available, run {__class__.__name__}.read_tabs first")
        # synthesize the beam
        sb_data = self.sb_generator.synthesize_beam(self.tab_data, sb)
        # return as spectra object
        return Spectra(self.freqs, self.tsamp, sb_data, starttime=self.startbin * self.tsamp, dm=0)

    def load_single_sb(self, sb, startbin, chunksize):
        """
        Convenience tool to read only a single SB and its associated TABs.
        *Note*: Any internal TAB data is cleared after calling this method

        :param int sb: SB index
        :param int startbin: Index of first time sample to read
        :param int chunksize: Number of time samples to read
        :return: Spectra object with SB data
        """
        # load the data of the required TABs
        tabs = set(self.sb_generator.get_map(sb))
        self.read_tabs(startbin, chunksize, tabs)
        # generate the SB
        sb = self.get_sb(sb)
        # remove the TAB data to avoid issues when changing startbin/chunksize in other methods
        self.tab_data = None
        return sb
