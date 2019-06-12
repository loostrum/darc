#!/usr/bin/env python
#
# dada_dbevent triggers

try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
from textwrap import dedent
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
        super(DADATrigger, self).__init__()
        self.thread = None

    def process_command(self, command):
        """
        Process command received from queue
        :param command: command dict
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
        Remove any remaining threads
        """
        if self.thread:
            self.thread.join()

    def send_event(self, triggers):
        """
        Send trigger to dada_dbevent
        :param triggers: list of trigger dictionaries
        """
        self.logger.info("Received {} trigger(s)".format(len(triggers)))

        # ToDo: move this to obs config
        port = triggers[0]['port']
        stokes = triggers[0]['stokes']
        utc_start = triggers[0]['utc_start'].iso

        events = ""
        # ensure utc_start is in iso format
        for trigger in triggers:
            trigger['utc_start'] = trigger['utc_start'].iso

            event_start_full = Time(trigger['utc_start']) + TimeDelta(trigger['time'], format='sec') - \
                                       TimeDelta(trigger['window_size'] / 2, format='sec')
            event_end_full = event_start_full + TimeDelta(trigger['window_size'], format='sec')

            event_start, event_start_frac = event_start_full.iso.split('.')
            # event_start_frac = '.' + event_start_frac
            event_end, event_end_frac = event_end_full.iso.split('.')
            # event_end_frac = '.' + event_end_frac

            # Add utc start/end for event
            trigger['event_start'] = event_start
            trigger['event_start_frac'] = event_start_frac
            trigger['event_end'] = event_end
            trigger['event_end_frac'] = event_end_frac

            # Add to the event
            events += ("{event_start} {event_start_frac} {event_end} {event_end_frac} {snr} "
                       "{dm} {width} {beam}\n".format(**trigger))

        # create event. the "\" ensures the string does not start with a newline
        # the actual newline needs to be there to make dedent work properly
        full_event = {'num_event': len(triggers), 'utc_start': utc_start, 'events': events}
        event = dedent("""\
                       N_EVENTS {num_event}
                       {utc_start}
                       {events}
                       """.format(**full_event))

        # open socket
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
            self.logger.error("Failed to send stokes {} event".format(stokes))
            return
        self.logger.info("Successfully sent stokes {} event".format(stokes))

        # close socket
        sock.close()
