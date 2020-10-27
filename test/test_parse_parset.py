#!/usr/bin/env python3

import os
import unittest

from darc.util import parse_parset


class TestParseParset(unittest.TestCase):

    def test_parse_parset(self):
        """
        Test that the parset parser works for strings
        """

        # generate a parset dict
        input_parset_dict = {'amber_dir': '/data2/output/20190518/2019-05-18-23:51:13.B1933+16/amber',
                             'network_port_event_iquv': 30001, 'obs_mode': 'survey', 'source': 'B1933+16',
                             'output_dir': '/data2/output/20190518/2019-05-18-23:51:13.B1933+16',
                             'network_port_start_i': '5000'}

        # write to file
        PARSET_FILE = 'test.parset'
        with open(PARSET_FILE, 'w') as f:
            for k, v in input_parset_dict.items():
                f.write("{}={}\n".format(k, v))

        # Read back as done in processing
        with open(PARSET_FILE) as f:
            parset_str = f.read()

        # Feed to the parser
        output_parset_dict = parse_parset(parset_str)

        self.assertDictEqual(input_parset_dict, output_parset_dict)

        # remove the temp parset
        os.remove(PARSET_FILE)

    def test_parse_parset_types(self):
        """
        Test that type conversion in the parser works
        """

        # generate a parset dict
        input_parset_dict = {'startpacket': 100000, 'beam': 0, 'ntabs': 12,
                             'nsynbeams': 71, 'duration': 300.05,
                             'history_i': 10.24, 'history_iquv': 15.36,
                             'snrmin': 10.1, 'min_freq': 1249.5,
                             'proctrigger': True, 'enable_iquv': False}

        # write to file
        PARSET_FILE = 'test.parset'
        with open(PARSET_FILE, 'w') as f:
            for k, v in input_parset_dict.items():
                f.write("{}={}\n".format(k, v))

        # Read back as done in processing
        with open(PARSET_FILE) as f:
            parset_str = f.read()

        # Feed to the parser
        output_parset_dict = parse_parset(parset_str)

        self.assertDictEqual(input_parset_dict, output_parset_dict)

        # remove the temp parset
        os.remove(PARSET_FILE)


if __name__ == '__main__':
    unittest.main()
