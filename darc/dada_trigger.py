#!/usr/bin/env python3
#
# dada_dbevent triggers

import os
import threading
import socket

import numpy as np
from astropy.time import Time, TimeDelta

from darc import DARCBase
from darc import util
from darc.definitions import TIME_UNIT, CONFIG_FILE


class DADATriggerException(Exception):
    pass


class DADATrigger(DARCBase):
    """
    Generate and send dada_dbevent triggers
    """

    def __init__(self, *args, **kwargs):
        """
        """
        super(DADATrigger, self).__init__(*args, **kwargs)
        self.thread_trigger = None
        self.thread_polcal = None
        self.triggers_enabled = True

    def start_observation(self, obs_config, reload=True):
        """
        Start observation: run IQUV dumps automatically if source is polarisation calibrator.
        Else ensure normal FRB candidate I/IQUV dumps are enabled

        :param dict obs_config: Observation config
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

        # load parset
        parset = self._load_parset(obs_config)
        if parset is None:
            self.logger.warning("No observation parset found; not checking for polcal observation")
            # ensure normal triggers can be received and processed
            self.triggers_enabled = True
            return
        # Override regular IQUV trigger with polcal
        if 'polcal' in parset['task.source.name'] and int(parset['task.source.beam']) == obs_config['beam']:
            self.logger.info("Found polarisation calibrator in this beam")
            # disable regular triggering
            self.triggers_enabled = False
            # do the automated polcal dumps
            self.thread_polcal = threading.Thread(target=self.polcal_dumps, args=[obs_config])
            self.thread_polcal.start()
        else:
            self.logger.info("No polarisation calibrator in this beam, enabling regular triggering")
            # no polcal obs, ensure normal triggers can be received and processed
            self.triggers_enabled = True

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: command with arguments
        """
        if command['command'] == 'trigger':
            # trigger received, check if triggering is enabled
            if not self.triggers_enabled:
                self.logger.warning("Trigger received but triggering is disabled, ignoring")
                return
            # process trigger
            self.thread_trigger = threading.Thread(target=self.send_event, args=[command['trigger']])
            self.thread_trigger.start()
        elif command['command'] == 'get_attr':
            self.get_attribute(command)
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def cleanup(self):
        """
        Remove all trigger-sending threads
        """
        if self.thread_trigger:
            self.thread_trigger.join()
        if self.thread_polcal:
            self.thread_polcal.join()

    def send_event(self, triggers):
        """
        Send trigger to dada_dbevent

        :param list triggers: list of trigger dictionaries
        """
        self.logger.info("Received {} trigger(s):".format(len(triggers)))
        self.logger.info(triggers)

        # utc start is identical for all triggers of a set
        utc_start = triggers[0]['utc_start'].iso.replace(' ', '-')

        events_i = ""
        events_iquv = ""
        ntrig_i = 0
        ntrig_iquv = 0
        for trigger in triggers:
            stokes = trigger['stokes']
            if stokes.upper() not in ['I', 'IQUV']:
                self.logger.error("Skipping trigger with unknown stokes mode: {}".format(stokes))
                continue

            # start 2 pages before trigger time
            # 1 page should be enough, but due to a bug in dada_dbevent the start time is rounded up
            # to the next page, instead of down
            shift = 2.048  # 2 pages
            # calculate window size: equal to DM delay, but at least some minimum set in config
            # DM is roughly delay acros band in ms
            # add end delay defined in config and shift
            window_size = max(self.min_window_size, trigger['dm'] / 1000.) + self.delay_end + shift

            event_start_full = Time(trigger['utc_start']) + TimeDelta(trigger['time'], format='sec') - \
                TimeDelta(shift, format='sec')
            # ensure start time is past start time of observation
            if event_start_full < trigger['utc_start']:
                self.logger.info("Event start before start of observation - adapting event start")
                event_start_full = trigger['utc_start']
            event_end_full = event_start_full + TimeDelta(window_size, format='sec')
            # ToDo: ensure end time is before end of observation

            event_start, event_start_frac = event_start_full.iso.split('.')
            # event_start_frac = '.' + event_start_frac
            event_end, event_end_frac = event_end_full.iso.split('.')
            # event_end_frac = '.' + event_end_frac

            # Add utc start/end for event
            trigger['event_start'] = event_start.replace(' ', '-')
            trigger['event_start_frac'] = event_start_frac
            trigger['event_end'] = event_end.replace(' ', '-')
            trigger['event_end_frac'] = event_end_frac

            # Add to the event
            # here already sure that stokes.upper() is either IQUV or I
            if stokes.upper() == 'I':
                ntrig_i += 1
                events_i += ("{event_start} {event_start_frac} {event_end} {event_end_frac} {dm} "
                             "{snr} {width} {beam}\n".format(**trigger))
            else:
                ntrig_iquv += 1
                events_iquv += ("{event_start} {event_start_frac} {event_end} {event_end_frac} {dm} "
                                "{snr} {width} {beam}\n".format(**trigger))

        # send stokes I events
        if ntrig_i > 0:
            info_i = {'num_event': ntrig_i, 'utc_start': utc_start, 'events': events_i}
            event_i = "N_EVENTS {num_event}\n{utc_start}\n{events}".format(**info_i)
            self.send_events(event_i, 'I')
        # send stokes IQUV events
        if ntrig_iquv > 0:
            info_iquv = {'num_event': ntrig_iquv, 'utc_start': utc_start, 'events': events_iquv}
            event_iquv = "N_EVENTS {num_event}\n{utc_start}\n{events}".format(**info_iquv)
            self.send_events(event_iquv, 'IQUV')

    def send_events(self, event, stokes):
        """
        Send stokes I or IQUV events

        :param str event: raw event to send
        :param str stokes: I or IQUV
        :return:
        """
        # open socket
        if stokes.upper() == 'I':
            port = self.port_i
        else:
            port = self.port_iquv

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(("localhost", port))
        except socket.error as e:
            self.logger.error("Failed to connect to stokes {} dada_dbevent on port {}: {}".format(stokes,
                                                                                                  port, e))
            return

        # send event
        try:
            sock.sendall(event.encode())
        except socket.timeout:
            self.logger.error("Failed to send events within timeout limit")
            sock.close()
            return
        self.logger.info("Successfully sent events")

        # close socket
        sock.close()

    def _load_parset(self, obs_config):
        """
        Load the observation parset

        :param dict obs_config: Observation config
        :return: parset as dict
        """
        try:
            # encoded parset is already in config on master node
            # decode the parset
            raw_parset = util.decode_parset(obs_config['parset'])
            # convert to dict and store
            parset = util.parse_parset(raw_parset)
        except KeyError:
            self.logger.info("Observation parset not found in input config, looking for master parset")
            # Load the parset from the master parset file
            master_config_file = os.path.join(obs_config['master_dir'], 'parset', 'darc_master.parset')
            try:
                # Read raw config
                with open(master_config_file) as f:
                    master_config = f.read().strip()
                # Convert to dict
                master_config = util.parse_parset(master_config)
                # extract obs parset and decode
                raw_parset = util.decode_parset(master_config['parset'])
                parset = util.parse_parset(raw_parset)
            except Exception as e:
                self.logger.warning(
                    "Failed to load parset from master config file {}, setting parset to None: {}".format(master_config_file, e))
                parset = None

        return parset

    def polcal_dumps(self, obs_config):
        """
        Automatically dump IQUV data at regular intervals for polcal calibrator observations

        :param dict obs_config: Observation config
        """
        tstart = Time(obs_config['startpacket'] / TIME_UNIT, format='unix')
        duration = TimeDelta(obs_config['duration'], format='sec')
        tend = tstart + duration

        # round up polcal dump size to nearest 1.024 s
        dump_size = TimeDelta(np.ceil(self.polcal_dump_size / 1.024) * 1.024, format='sec')
        dump_interval = TimeDelta(self.polcal_interval, format='sec')

        # sleep until first trigger time
        util.sleepuntil_utc(tstart + dump_interval, event=self.stop_event)

        # run until trigger would be end past end time
        # add a second to avoid trigger running a little bit over end time
        # also stay below global limit on number of dumps during one obs
        # TODO: also set a total length limit
        ndump = 0
        while Time.now() + dump_interval - TimeDelta(1.0, format='sec') < tend and ndump < self.polcal_max_dumps:
            # generate an IQUV trigger
            params = {'utc_start': tstart.iso.replace(' ', '-')}
            # trigger start time: now, rounded to nearest 1.024s since utc start
            dt = TimeDelta(np.round((Time.now() - tstart).sec / 1.024) * 1.024, format='sec')
            # trigger start/end times
            event_start_full = tstart + dt
            event_end_full = tstart + dt + dump_size
            # convert to dada_dbevent format
            event_start, event_start_frac = event_start_full.iso.split('.')
            event_end, event_end_frac = event_end_full.iso.split('.')
            # store to params
            params['event_start'] = event_start.replace(' ', '-')
            params['event_start_frac'] = event_start_frac
            params['event_end'] = event_end.replace(' ', '-')
            params['event_end_frac'] = event_end_frac

            event = "N_EVENTS 1\n{utc_start}\n{event_start} {event_start_frac} {event_end} {event_end_frac} " \
                    "0 0 0 0".format(**params)  # dm, snr, width, beam are 0

            self.logger.info("Sending automated polcal IQUV dump event: {}".format(params))
            self.send_events(event, 'IQUV')
            # keep track of number of performed dumps
            ndump += 1

            # sleep
            self.stop_event.wait(dump_interval.sec)
