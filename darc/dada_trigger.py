#!/usr/bin/env python
#
# dada_dbevent triggers

import threading
import socket
from astropy.time import Time, TimeDelta

from darc.base import DARCBase


class DADATriggerException(Exception):
    pass


class DADATrigger(DARCBase):
    """
    Generate and send dada_dbevent triggers
    """

    def __init__(self):
        """
        """
        super(DADATrigger, self).__init__()
        self.thread = None

    def process_command(self, command):
        """
        Process command received from queue

        :param dict command: command with arguments
        """
        if command['command'] == 'trigger':
            # trigger received, send to dada_dbevent
            self.thread = threading.Thread(target=self.send_event, args=[command['trigger']])
            self.thread.daemon = True
            self.thread.start()
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def cleanup(self):
        """
        Remove all trigger-sending threads
        """
        if self.thread:
            self.thread.join()

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
