#!/usr/bin/env python

import os
import unittest
import multiprocessing as mp
import threading
from shutil import which
from time import sleep
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import numpy as np
try:
    import psrdada
except ImportError:
    psrdada = None

from darc.amber_listener import AMBERListener
try:
    from darc.processor import Processor
except ImportError:
    Processor = None


class TestProcessor(unittest.TestCase):

    def test_psrdada(self):
        """
        Check whether we can connect to and read from a psrdada buffer
        """

        # Skip if we don't have psrdada in python or on system
        if psrdada is None or which('dada_db') is None:
            self.skipTest("PSRDADA not available")
        

        # remove any old buffer
        os.system('dada_db -d')
        # start a buffer (default key: dada)
        os.system('dada_db -n 5')

        # init psrdada
        reader = psrdada.Reader()
        reader.connect(0xdada)

        writer = psrdada.Writer()
        writer.connect(0xdada)

        # write header
        writer.setHeader({'HEADERKEY': 'VALUE'})
        # write a few pages
        npagemax = 3
        npage = 0
        for page in writer:
            data = np.asarray(page)
            data.fill(npage)
            npage += 1
            if npage == npagemax:
                writer.markEndOfData()
        writer.disconnect()

        # read pages
        npage_received = 0
        for page in reader:
            npage_received += 1
        reader.disconnect()

        # number of written and read pages should be equal
        self.assertEqual(npagemax, npage_received)

        # remove buffer
        os.system('dada_db -d')
            

    def test_processor(self):
        """
        Test full processor run with fake data in ringbuffer
        """
        if Processor is None:
            self.skipTest("Could not import Processor")


if __name__ == '__main__':
    unittest.main()
