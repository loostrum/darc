#!/usr/bin/env python
#
# utility functions

import os
import errno
import datetime
import time
import json
from astropy.time import Time
try:
    from queue import Empty
except ImportError:
    from Queue import Empty


def sleepuntil_utc(end_time, event=None):
    """
    Sleep until specified time.
    :param end_time: sleep until this time (datetime or astropy.time.Time object)
    :param event: If specified, uses event.wait instead of time.sleep
    """

    # convert datetime to astropy time
    if isinstance(end_time, datetime.datetime):
        end_time = Time(end_time)

    # get seconds to sleep
    now = Time.now()
    sleep_seconds = (end_time - now).sec

    # no need to wait it end time is in the past
    if sleep_seconds <= 0:
        return

    # sleep using event or time.sleep
    if event:
        event.wait(sleep_seconds)
    else:
        time.sleep(sleep_seconds)

    return


def makedirs(path):
    """
    Mimic os.makedirs, but do not error when directory already exists
    :param path: path to create
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise


def parse_parset(parset_str):
    """
    Parse parset into dict with proper types
    :param parset_str: raw parset as string
    :return: parset as dict
    """
    # split per line, remove comments
    raw_parset = parset_str.split('\n')
    # remove any line starting with #
    parset = []
    for line in raw_parset:
        if not line.startswith('#'):
            parset.append(line)
    # Split keys/values. Separator might be "=" or " = "
    parset = [item.replace(' = ', '=').strip().split('=') for item in parset]
    # remove empty last line if present
    if parset[-1] == ['']:
        parset = parset[:-1]

    # convert to dict
    parset_dict = dict(parset)
    # remove any comments
    for key, value in parset_dict.items():
        pos_comment_char = value.find('#')
        if pos_comment_char > 0:  # -1 if not found, leave in place if first char
            parset_dict[key] = value[:pos_comment_char].strip()  # remove any trailing spaces too
    # fix types where needed
    # anything not changed here remains a string
    # ints
    for key in ['startpacket', 'beam', 'ntabs', 'nsynbeams']:
        if key in parset_dict.keys():
            parset_dict[key] = int(parset_dict[key])
    # floats
    for key in ['duration', 'history_i', 'history_iquv', 'snrmin', 'min_freq']:
        if key in parset_dict.keys():
            parset_dict[key] = float(parset_dict[key])
    # bools
    for key in ['proctrigger', 'enable_iquv']:
        if key in parset_dict.keys():
            parset_dict[key] = json.loads(parset_dict[key].lower())
    return parset_dict


def tail(f, event, interval=.1):
    """
    Read all lines of a file, then tail until stop event is set
    :param f: handle to file to tail
    :param event: stop event
    :param interval: sleep time between checks for new lines (default: .1)
    """
    # first read any lines already present
    while not event.is_set():
        line = f.readline()
        if line:
            yield line
        else:
            # no more lines, start the tail
            while not event.is_set():
                where = f.tell()
                line = f.readline()
                if not line:
                    time.sleep(interval)
                    f.seek(where)
                else:
                    yield line


def clear_queue(queue):
    """
    Read all remaining items in a queue and discard them
    :param queue: queue to clear
    """
    try:
        while True:
            queue.get_nowait()
    except Empty:
        pass
