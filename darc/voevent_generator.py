#!/usr/bin/env python
#
# VOEvent Generator

import errno
import yaml
import logging
import logging.handlers
from queue import Queue, Empty
import threading
from astropy.coordinates import SkyCoord
import astropy.units as u

from darc.definitions import *
from darc.VOEvent_create import NewVOEvent


class VOEventGeneratorException(Exception):
    pass


class VOEventGenerator(threading.Thread):
    """
    Generate VOEvent from incoming trigger
    """

    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self.daemon = True

        self.voevent_queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['voevent_generator']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~')}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        handler = logging.handlers.WatchedFileHandler(self.log_file)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        # create and cd to voevent directory
        try:
            os.makedirs(self.voevent_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                self.logger.error("Cannot create voevent directory:, {}".format(e))
                raise VOEventGeneratorException("Cannot create voevent directory")
        os.chdir(self.voevent_dir)

    def set_source_queue(self, queue):
        if not isinstance(queue, Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise VOEventGeneratorException('Given source queue is not an instance of Queue')
        self.voevent_queue = queue

    def run(self):
        if not self.voevent_queue:
            self.logger.error('Queue not set')
            raise VOEventGeneratorException('Queue not set')

        self.logger.info("Running VOEvent generator")
        while not self.stop_event.is_set():
            try:
                trigger = self.voevent_queue.get(timeout=1)
            except Empty:
                continue
            self.logger.info("Received trigger: {}".format(trigger))
            self.create_and_send(trigger)

        self.logger.info("Stopping VOEvent generator")

    def create_and_send(self, trigger):
        if not isinstance(trigger, dict):
            self.logger.error("Trigger is not a dict")
            return

        # check if all required params are present
        # gl and gb are generated from ra and dec
        # semiMaj and semiMin have default set for IAB
        keys = ['dm', 'dm_err', 'width', 'snr', 'flux', 'semiMaj', 'semiMin',
                'ra', 'dec', 'ymw16', 'name', 'importance', 'utc']
        for key in keys:
            if key not in trigger.keys():
                self.logger.error("Parameter missing from trigger: {}".format(key))
                return

        # Parse coordinates
        coord = SkyCoord(ra=trigger['ra']*u.degree, dec=trigger['dec']*u.degree, frame='icrs')
        trigger['gl'] = coord.galactic.l.deg
        trigger['gb'] = coord.galactic.b.deg

        self.logger.info("Creating VOEvent")
        NewVOEvent(**trigger)
        self.logger.info("Generated VOEvent")
