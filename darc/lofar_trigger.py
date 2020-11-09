#!/usr/bin/env python3
#
# LOFAR trigger system

import os
import yaml
import multiprocessing as mp
from queue import Empty
from time import sleep
import numpy as np
import socket
import struct
from textwrap import dedent
from collections import OrderedDict
from multiprocessing.managers import BaseManager
import astropy.units as u
from astropy.time import Time
from astropy.coordinates import SkyCoord

from darc.definitions import CONFIG_FILE, TSAMP
from darc import util
from darc.logger import get_logger


class LOFARTriggerQueueServer(BaseManager):
    """
    Server for LOFAR Trigger input queue
    """
    pass


class LOFARTriggerException(Exception):
    pass


class LOFARTrigger(mp.Process):
    """
    Select brightest trigger from incoming trigger and send to LOFAR for TBB triggering
    """
    def __init__(self, source_queue, *args, config_file=CONFIG_FILE, **kwargs):
        """
        :param Queue source_queue: Input queue for controlling this service
        :param str config_file: Path to config file
        """
        super(LOFARTrigger, self).__init__()
        self.stop_event = mp.Event()
        self.control_queue = source_queue

        self.trigger_server = None

        with open(config_file, 'r') as f:
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

        self.logger.info("Starting LOFAR Trigger")

        # start the queue server
        self.trigger_server = LOFARTriggerQueueServer(address=('', self.server_port),
                                                      authkey=self.server_auth.encode())
        self.trigger_server.start()

        # wait for events until stop is set
        while not self.stop_event.is_set():
            # check if a stop command was received
            try:
                command = self.control_queue.get(timeout=.1)
            except Empty:
                pass
            else:
                if isinstance(command, str) and command == 'stop':
                    self.stop()
                else:
                    self.logger.error(f"Unknown command received: {command}")

            # check if a trigger was received
            try:
                trigger = self.trigger_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                if trigger == 'stop':
                    self.stop()
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
        self.logger.info("Stopping LOFAR Trigger")

    def create_and_send(self, trigger):
        """
        Create LOFAR trigger struct

        Event is only sent if enabled in config

        :param list/dict trigger: Trigger event(s). dict if one event, list of dicts if multiple events
        """

        # if multiple triggers are received, select one
        if isinstance(trigger, list):
            self.logger.info("Received {} triggers, selecting known source / highest S/N".format(len(trigger)))
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
        trigger_for_sending = trigger.copy()
        # be careful not to change dict we are looping over
        for key in trigger.keys():
            if key not in trigger_generator_keys:
                trigger_for_sending.pop(key, None)

        # create trigger struct to send
        packet = self._new_trigger(**trigger_for_sending)
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
            # self.logger.info("LOFAR trigger sent - disabling future LOFAR triggering")
            # self.send_events = False
            self.logger.info("LOFAR trigger sent")
            # send email warning
            try:
                # use the original trigger here, which has all keys
                self.send_email(trigger)
            except Exception as e:
                self.logger.error("Failed to send warning email: {}".format(e))
            else:
                self.logger.info("Sent warning email")

    def _select_trigger(self, triggers):
        """
        Select trigger with highest S/N from a list of triggers.
        If there are triggers from both known and new sources, select the known source

        :param list triggers: one dict per trigger
        :return: trigger with highest S/N and number of unique CBs in trigger list
        """

        # check known vs new sources
        # get known/new keyword for all triggers, and check if all are default name for new source
        # if not, there are known sources. Select only those
        if not np.all([trigger['name'] == 'candidate' for trigger in triggers]):
            triggers_known_sources = [trigger for trigger in triggers if trigger['name'] != 'candidate']
            ntrig_removed = len(triggers) - len(triggers_known_sources)
            triggers = triggers_known_sources

        # select max S/N
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

    def _new_trigger(self, dm, utc, nu_GHz=1.37, test=False):
        """
        Create a LOFAR trigger struct

        :param float dm: Dispersion measure (pc cm**-3)
        :param str utc: UTC arrival time in ISOT format
        :param float nu_GHz: Apertif centre frequency (GHz)
        :param bool test: Whether to send a test event or observation event
        """
        # add units
        nu_GHz *= u.GHz
        dm *= u.pc * u.cm**-3

        # calculate pulse arrival time at LOFAR
        # AMBER uses top of band, but is already correct to centre by AMBERClustering
        # LOFAR reference frequency
        flo = self.lofar_freq * u.MHz
        dm_delay = util.dm_to_delay(dm, flo, nu_GHz)
        # aim to have pulse in centre of TBB buffer
        lofar_buffer_delay = .5 * self.lofar_tbb_buffer_size * u.s
        # calculate buffer stop time
        tstop = Time(utc, scale='utc', format='isot') + dm_delay + lofar_buffer_delay
        # Use unix time, split into integer part and float part to ms accuracy
        tstop_s = int(tstop.unix)
        tstop_remainder = tstop.unix - tstop_s
        tstop_ms = int(np.round(tstop_remainder * 1000))

        # dm is sent as int, multiplied by ten to preserve one decimal place
        dm_int = int(np.round(10 * dm.to(u.pc * u.cm**-3).value))

        # test event or real event
        if test:
            test_flag = b'T'  # = \x54
        else:
            test_flag = b'S'  # = \x53

        # struct format
        fmt = '>cciHHc'

        # create and return the struct
        return struct.pack(fmt, b'\x99', b'\xA0', tstop_s, tstop_ms, dm_int, test_flag)

    def send_email(self, trigger):
        """
        Send email upon LOFAR trigger

        Consider running this method in a try/except block, as sending emails might fail in case of network
        interrupts

        :param dict trigger: Trigger as sent to LOFAR
        """

        # init email with HTML table
        content = dedent("""
                         <html>
                         <title>Apertif LOFAR Trigger Alert System</title>
                         <body>
                         <p>
                         <table style="width:50%">
                         """)

        # Add formatted coordinates to trigger, skip if unavailable
        try:
            s = SkyCoord(trigger['ra'], trigger['dec'], unit='deg')
            trigger['coord'] = s.to_string('hmsdms', precision=2)
        except KeyError:
            pass

        # add plot command
        date = ''.join(trigger['datetimesource'].split('-')[:3])
        filterbank_prefix = '/data2/output/{}/{}/filterbank/CB{:02d}'.format(date, trigger['datetimesource'], trigger['cb'])
        downsamp = int(trigger['width'] / TSAMP.to(u.ms).value)
        trigger['plot_cmd'] = 'python {} --cmap viridis --rficlean --sb {} --dm {:.2f} ' \
                              '--t {:.2f} --downsamp {} {}'.format(self.waterfall_sb, trigger['sb'],
                                                                   trigger['dm'], trigger['tarr'],
                                                                   downsamp, filterbank_prefix)
        # define formatting for trigger parameters
        # use an ordered dict to ensure order of parameters in the email is the same
        parameters = OrderedDict()
        parameters['datetimesource'] = ['Observation', '{}']
        parameters['dm'] = ['Dispersion measure', '{:.2f} pc cm<sup>-3</sup>']
        parameters['snr'] = ['S/N', '{:.2f}']
        parameters['tarr'] = ['AMBER arrival time', '{:.2f} s']
        parameters['width'] = ['Width', '{:.2f} ms']
        parameters['flux'] = ['Flux density', '{:.2f} Jy']
        parameters['cb'] = ['Compound beam', '{:02d}']
        parameters['sb'] = ['Synthesized beam', '{:02d}']
        parameters['coord'] = ['CB coordinates (J2000)', '{}']
        parameters['utc'] = ['LOFAR TBB freeze time', '{}']
        parameters['plot_cmd'] = ['Plot command', '{}']

        # add parameters to email, skip any that are unavailable
        for param, (name, formatting) in parameters.items():
            try:
                value = formatting.format(trigger[param])
                content += '<tr><th colspan="4" style="text-align:left">{}</th><td colspan="6">{}</td></tr>\n'.format(name, value)
            except KeyError:
                pass

        # add footer
        content += dedent("""
                          </table>
                          </p>
                          </body>
                          </html>
                          """)

        # set email subject with trigger time
        subject = 'ARTS LOFAR Trigger Alert'
        # set other email settings
        frm = "ARTS LOFAR Trigger Alert System <arts@{}.apertif>".format(socket.gethostname())
        to = ', '.join(self.email_settings['to'])
        body = {'type': 'html', 'content': content}

        # send
        util.send_email(frm, to, subject, body)
