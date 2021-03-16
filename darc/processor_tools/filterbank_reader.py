#!/usr/bin/env python3
import numpy as np
from sigpyproc import FilReader

from darc import SBGenerator
from darc.processor_tools.spectra import Spectra
from darc.definitions import NTAB, CONFIG_FILE


class ARTSFilterbankReaderError(Exception):
    pass


class ARTSFilterbankReader:
    def __init__(self, fname, cb, ntab=NTAB, config_file=CONFIG_FILE):
        """
        Filterbank reader for ARTS data, one file per TAB

        :param str fname: path to filterbank files, with {cb:02d} and {tab:02d} for CB and TAB indices
        :param int cb: CB index
        :param int ntab: Number of TABs (Default: NTAB from constants)
        :param str config_file: Path to config file, passed to SBGenerator
        """
        # initialize the SB Generator for SC4
        self.sb_generator = SBGenerator.from_science_case(4, config_file=config_file)

        self.header = None

        self.ntab = ntab
        self.fnames = [fname.format(cb=cb, tab=tab) for tab in range(ntab)]
        self.get_header()

        self.tab_data = None
        self.startbin = None
        self.chunksize = None
        self.times = None

    def get_header(self, tab=0):
        """
        Read filterbank parameters to self.header

        :param int tab: TAB index (Default: 0)
        """
        fil = FilReader(self.fnames[tab])
        # set SB generator order
        if fil.header.foff < 0:
            self.sb_generator.reversed = True
        else:
            self.sb_generator.reversed = False
        # store header
        self.header = fil.header
        # Add full frequency axis
        self.header.freqs = np.arange(fil.header.nchans) * fil.header.foff + fil.header.fch1

    def read_filterbank(self, tab, startbin, chunksize):
        """
        Read a chunk of filterbank data

        :param int tab: TAB index
        :param int startbin: Index of first time sample to read
        :param int chunksize: Number of time samples to read
        :return: chunk of data with shape (nfreq, chunksize)
        """
        fil = FilReader(self.fnames[tab])
        # read chunk of data as numpy array
        return fil.readBlock(startbin, chunksize, as_filterbankBlock=False)

    def read_tabs(self, startbin, chunksize, tabs=None):
        """
        Read TAB data

        :param int startbin: Index of first time sample to read
        :param int chunksize: Number of time samples to read
        :param list tabs: which TABs to read (Default: all)
        """
        tab_data = np.zeros((self.ntab, self.header.nchans, chunksize))
        if tabs is None:
            tabs = range(self.ntab)
        for tab in tabs:
            tab_data[tab] = self.read_filterbank(tab, startbin, chunksize)
        self.tab_data = tab_data
        self.startbin = startbin
        self.chunksize = chunksize
        self.times = np.arange(chunksize) * self.header.tsamp

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
        return Spectra(self.header.freqs, self.header.tsamp, sb_data, starttime=self.startbin * self.header.tsamp, dm=0)

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
