#!/usr/bin/env python3
#
# synthesized beam generator

import os
import socket
import yaml
import numpy as np

from darc.definitions import CONFIG_FILE


class SBGeneratorException(Exception):
    pass


class SBGenerator:
    """
    Synthesized beam generator
    """

    def __init__(self, fname=None, science_case=None, config_file=CONFIG_FILE):
        """
        __init__ should be called by cls.from_science_case or cls.from_table

        :param fname: path to synthesized beam table
        :param science_case: ARTS science case (3 or 4)
        :param str config_file: Path to config file
        """
        self.sb_table = None
        self.nsub = None
        self.numtab = None
        self.numsb = None
        self.sb_mapping = None
        self.__reversed = None

        # Load config
        with open(config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['sb_generator']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # Get full path to SB table
        if fname:
            if not fname.startswith('/'):
                fname = os.path.join(self.table_folder, fname)
        elif science_case:
            if science_case == 3:
                fname = os.path.join(self.table_folder, self.table['sc3'])
            else:
                fname = os.path.join(self.table_folder, self.table['sc4'])
        self.science_case = science_case
        self.fname = fname

        # load the table
        self._load_table()

    @property
    def reversed(self):
        """
        Whether or not the SB table is reversed for use on filterbank data

        :return: reversed (bool)
        """
        return self.__reversed

    @reversed.setter
    def reversed(self, state):
        """
        Reverse the SB table for use on filterbank data

        :param bool state: whether or not the table should be in filterbank order
        """
        if self.__reversed == state:
            # already in desired state
            return
        else:
            # reverse the table
            self.sb_mapping = self.sb_mapping[:, ::-1]
            # store state
            self.__reversed = state

    @classmethod
    def from_table(cls, fname, config_file=CONFIG_FILE):
        """
        Initalize with provided SB table

        :param str fname: Path to SB table
        :param str config_file: Path to config file
        :return: SBGenerator object
        """
        return cls(fname=fname, config_file=config_file)

    @classmethod
    def from_science_case(cls, science_case, config_file=CONFIG_FILE):
        """
        Initalize default table for given science case

        :param int science_case: science case (3 or 4)
        :param str config_file: Path to config file
        :return: SBGenerator object
        """
        if science_case not in (3, 4):
            raise SBGeneratorException('Invalid science case: {}'.format(science_case))
        return cls(science_case=science_case, config_file=config_file)

    def _load_table(self):
        """
        Load SB table
        """
        self.sb_mapping = np.loadtxt(self.fname, dtype=int)
        numsb, self.nsub = self.sb_mapping.shape
        # do some extra checks if table is loaded based on science case
        # otherwise this is the users's responsibility
        if self.science_case:
            # check that the number of SBs is what we expect and TABs are not out of range
            if self.science_case == 3:
                expected_numtab = self.numtab['sc3']
                expected_numsb = self.numsb['sc3']
            else:
                expected_numtab = self.numtab['sc4']
                expected_numsb = self.numsb['sc4']
            # number of SBs and TABs

            # verify number of SBs
            if not expected_numsb == numsb:
                raise SBGeneratorException("Number of SBs ({}) not equal to expected value ({})".format(numsb,
                                                                                                        expected_numsb))
            # verify max TAB index, might be less than maximum if not all SBs are generated
            if not np.amax(self.sb_mapping) < expected_numtab:
                raise SBGeneratorException("Maximum TAB ({}) higher than maximum for this science case ({})".format(
                                           max(self.sb_mapping), expected_numtab))
            self.numsb = numsb
            self.numtab = expected_numtab
        self.__reversed = False

    def get_map(self, sb):
        """
        Return mapping of requested SB

        :param int sb: beam to return mapping for
        :return: SB mapping for requested beam
        """
        return self.sb_mapping[sb]

    def synthesize_beam(self, data, sb):
        """
        Synthesize beam

        :param np.ndarray data: TAB data with shape [TAB, freq, time]
        :param int sb: SB index
        :return: SB data with shape [freq, time]
        """
        ntab, nfreq, ntime = data.shape
        # verify that SB index is ok
        if not sb < self.numsb:
            raise SBGeneratorException("SB index too high: {}; maximum is {}".format(sb, self.numsb - 1))
        if not sb >= 0:
            raise SBGeneratorException("SB index cannot be negative")
        # verify that number of TABs is ok
        if not ntab == self.numtab:
            raise SBGeneratorException("Number of TABs ({}) not equal to expected number of TABs ({})".format(
                                       ntab, self.numtab))
        # verify number of channels
        if nfreq % self.nsub:
            raise SBGeneratorException("Error: Number of subbands ({}) is not a factor of "
                                       "number of channels ({})".format(self.nsub, nfreq))

        nchan_per_subband = int(nfreq / self.nsub)
        beam = np.zeros((nfreq, ntime))
        for subband, tab in enumerate(self.sb_mapping[sb]):
            # get correct subband of correct tab and add it to raw SB
            # after vsplit, shape is (nsub, nfreq/nsub, ntime) -> simply [subband] gets correct subband
            # assign to subband of sb
            beam[subband * nchan_per_subband:(subband + 1) * nchan_per_subband] = \
                np.vsplit(data[tab], self.nsub)[subband]
        return beam
