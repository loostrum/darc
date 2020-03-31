#!/usr/bin/env python
#
# LOFAR trigger system

import os
import yaml
import multiprocessing as mp
import subprocess
from queue import Empty
from time import sleep
import numpy as np
import threading
import socket
import struct
from multiprocessing.managers import BaseManager
from astropy.coordinates import SkyCoord
import astropy.units as u
import astropy.constants as const
from astropy.time import Time

from darc.definitions import TSAMP, BANDWIDTH, NCHAN, TSYS, AP_EFF, DISH_DIAM, NDISH, CONFIG_FILE
from darc import util
from darc.logger import get_logger


class LOFARTriggerQueueServer(BaseManager):
    """
    Server for VOEvent input queue
    """
    pass


class LOFARTriggerException(Exception):
    pass


class LOFARTrigger(threading.Thread):
    """
    Select brightest trigger from incoming trigger and send to LOFAR for TBB triggering
    """
    def __init__(self):
        """
        """
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.daemon = True

        self.trigger_server = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['lofar_trigger']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # Initalize the queue server
        trigger_queue = mp.Queue()
        LOFARTriggerQueueServer.register('get_queue', callable=lambda: trigger_queue)
        self.trigger_queue = trigger_queue

        self.logger.info("LOFAR Trigger initialized")

    def stop(self):
        """
        Stop this service
        """
        self.stop_event.set()

    def run(self):
        """
        Main loop

        Read triggers from queue and process them
        """

        # start the queue server
        self.trigger_server = LOFARTriggerQueueServer(address=('', self.server_port),
                                                      authkey=self.server_auth.encode())
        self.trigger_server.start()

        # wait for events until stop is set
        while not self.stop_event.is_set():
            try:
                trigger = self.trigger_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                # a trigger was received, wait and read queue again in case there are multiple triggers
                sleep(self.interval)
                additional_triggers = []
                # read queue without waiting
                while True:
                    try:
                        additional_trigger = self.trigger_queue.get_nowait()
                    except Empty:
                        break
                    else:
                        additional_triggers.append(additional_trigger)

                # add additional triggers if there are any
                if additional_triggers:
                    trigger = [trigger] + additional_triggers

            self.create_and_send(trigger)

        # stop the queue server
        self.trigger_server.shutdown()
        self.logger.info("Stopping VOEvent generator")

    def create_and_send(self, trigger):
        """
        Create LOFAR trigger struct

        Event is only sent if enabled in config

        :param list/dict trigger: Trigger event(s). dict if one event, list of dicts if multiple events
        """

        # if multiple triggers are received, select one
        if isinstance(trigger, list):
            self.logger.info("Received {} triggers, selecting highest S/N".format(len(trigger)))
            trigger, num_unique_cb = self._select_trigger(trigger)
            self.logger.info("Number of unique CBs: {}".format(num_unique_cb))
        else:
            self.logger.info("Received 1 trigger")

        self.logger.info("Trigger: {}".format(trigger))

        if not self.send_events:
            self.logger.warning("Sending LOFAR triggers is disabled, not sending trigger")
            return

        # trigger should be a dict
        if not isinstance(trigger, dict):
            self.logger.error("Trigger is not a dict")
            return

        # check if all required params are present
        keys = ['dm', 'utc', 'snr', 'cb']
        for key in keys:
            if key not in trigger.keys():
                self.logger.error("Parameter missing from trigger: {}".format(key))
                return
        # test key; assert false if not present
        if 'test' not in trigger.keys():
            trigger['test'] = False
        # frequency key, assert 1.37 GHz if not present
        if 'nu_GHz' not in trigger.keys():
            trigger['nu_GHz'] = 1.37
        # remove unused keys
        trigger_generator_keys = ['dm', 'utc', 'nu_GHz', 'test']
        # run on copy of dict because dict could change in the loop
        for key in trigger.copy().keys():
            if key not in trigger_generator_keys:
                trigger.pop(key, None)

        # create trigger struct to send
        packet = self._new_trigger(**trigger)
        self.logger.info("Sending packet: {}".format(packet))
        # send
        try:
            # open a UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(packet, (self.lofar_host, self.lofar_port))
            sock.close()
        except Exception as e:
            self.logger.error("Failed to send LOFAR trigger: {}".format(e))
        else:
            self.logger.info("LOFAR trigger sent - disabling future LOFAR triggering")
            self.send_events = False

    @staticmethod
    def _select_trigger(triggers):
        """
        Select trigger with highest S/N from a list of triggers

        :param list triggers: one dict per trigger
        :return: trigger with highest S/N and number of unique CBs in trigger list
        """
        max_snr = 0
        index = None
        cbs = []
        # loop over triggers and check if current trigger has highest S/N
        for i, trigger in enumerate(triggers):
            snr = trigger['snr']
            if snr > max_snr:
                max_snr = snr
                index = i
            # save CB
            cbs.append(trigger['cb'])
        num_unique_cb = len(list(set(cbs)))
        # index is now index of trigger with highest S/N
        return triggers[index], num_unique_cb

    @staticmethod
    def _new_trigger(dm, utc, nu_GHz=1.37, test=False):
        """
        Create a LOFAR trigger struct

        :param float dm: Dispersion measure (pc cm**-3)
        :param str utc: UTC arrival time in ISOT format
        :param float nu_GHz: Apertif centre frequency (GHz)
        :param bool test: Whether to send a test event or observation event
        """
        # add units
        nu_GHz *= u.GHz
        dm *= u.pc*u.cm**-3

        # calculate pulse arrival time at LOFAR
        # AMBER uses top of band
        fhi = nu_GHz + .5*BANDWIDTH
        # LOFAR is referenced to 200 MHz
        flo = 200.*u.MHz
        dm_delay = util.dm_to_delay(dm, flo, fhi)
        # LOFAR TBB buffer size is 5 seconds, aim to have pulse in centre
        lofar_buffer_delay = 2.5*u.s
        # calculate buffer stop time
        tstop = Time(utc, scale='utc', format='isot') + dm_delay + lofar_buffer_delay
        # Use unix time, split into integer part and float part to ms accuracy
        tstop_s = int(tstop.unix)
        tstop_remainder = tstop.unix - tstop_s
        tstop_ms = int(np.round(tstop_remainder*1000))

        # dm is sent as int, multiplied by ten to preserve one decimal place
        dm_int = int(np.round(10 * dm.to(u.pc/u.cm**3).value))

        # test event or real event
        if test:
            test_flag = b'T'  # = \x54
        else:
            test_flag = b'S'  # = \x53

        # struct format
        fmt = '>cciHHc'

        # create and return the struct
        return struct.pack(fmt, b'\x99', b'\xA0', tstop_s, tstop_ms, dm_int, test_flag)
