#!/usr/bin/env python
#
# utility functions

import os
import errno
import datetime
import time
import json
from astropy.time import Time, TimeDelta


def sleepuntil_utc(end_time, event=None):
    """
    Sleep until specified time.
    :param: end_time: sleep until this time (datetime or astropy.time.Time object)
    :param: event: If specified, uses event.wait instead of time.sleep
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
    :param: path: path to create
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
    :param: parset_str: raw parset as string
    :return: parset as dict
    """
    # Split keys/values. Separator might be "=" or " = "
    parset = [item.replace(' = ', '=').strip().split('=') for item in parset_str.split('\n')]
    # remove empty last line if present
    if parset[-1] == ['']:
        parset = parset[:-1]

    # convert to dict
    parset_dict = dict(parset)
    # fix types where needed
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
