#!/usr/bin/env python
#
# utility functions

import os
import errno
import datetime
import time
import json
import codecs
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import numpy as np
from astropy.time import Time
import astropy.units as u
import astropy.constants as const
from astropy.coordinates import SkyCoord, FK5
from queue import Empty

from darc.definitions import DISH_DIAM, TSYS, AP_EFF, BANDWIDTH, WSRT_LON, WSRT_LAT, NDISH


def sleepuntil_utc(end_time, event=None):
    """
    Sleep until specified time

    :param datetime.datetime/astropy.time.Time end_time: sleep until this time
    :param threading.Event event: if specified, uses event.wait instead of time.sleep
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

    :param str path: path to recursively create
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise


def decode_parset(parset_bytes):
    """
    Decode parset into string

    :param bytes parset_bytes: raw parset bytes
    :return: parset as string
    """
    return codecs.decode(codecs.decode(codecs.decode(parset_bytes, 'hex'), 'bz2'), 'utf-8')


def encode_parset(parset_str):
    """
    Encode parset string into bytes

    :param str parset_str: parset as one string
    :return: encoded parset as bytes
    """
    return codecs.encode(codecs.encode(codecs.encode(parset_str, 'utf-8'), 'bz2'), 'hex')


def parse_parset(parset_str):
    """
    Parse parset into dict with proper types

    :param str parset_str: raw parset as string
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
    for key in ['startpacket', 'beam', 'ntabs', 'nsynbeams', 'network_port_event_i', 'network_port_event_iquv']:
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

    :param filehandle f: handle to file to tail
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

    :param queue.Queue queue: queue to clear
    """
    try:
        while True:
            queue.get_nowait()
    except Empty:
        pass


def get_flux(snr, width, ndish=NDISH, npol=2, coherent=True):
    """
    Compute single pulse flux density using radiometer equation

    :param float snr: S/N
    :param astropy.units.quantity.Quantity width: Width
    :param int ndish: Number of dishes used (default: 8)
    :param int npol: Number of polarizations (default: 2)
    :param bool coherent: Using coherent beamforming (default: True)
    :return: Peak flux density (astropy.units.quantity.Quantity)
    """
    gain = AP_EFF * np.pi * (DISH_DIAM / 2.)**2 / (2 * const.k_B)
    if coherent:
        beta = 1
    else:
        beta = 1. / 2
    sefd = TSYS / (gain * ndish**beta)
    flux = snr * sefd / np.sqrt(npol * BANDWIDTH * width)
    return flux.to(u.mJy)


def ra_to_ha(ra, dec, t):
    """
    Convert J2000 RA, Dec to WSRT HA, Dec

    :param astropy.units.quantity.Quantity ra: right ascension with unit
    :param astropy.units.quantity.Quantity dec: declination with unit
    :param str/astropy.time.Time t: UTC time
    :return: SkyCoord object of apparent HA, Dec coordinates
    """

    # Convert time to Time object if given as string
    if isinstance(t, str):
        t = Time(t)

    # Apparent LST at WSRT at this time
    lst = t.sidereal_time('apparent', WSRT_LON)
    # Equinox of date (because hour angle uses apparent coordinates)
    coord_system = FK5(equinox='J{}'.format(t.decimalyear))
    # convert coordinates to apparent
    coord_apparent = SkyCoord(ra, dec, frame='icrs').transform_to(coord_system)
    # HA = LST - apparent RA
    ha = lst - coord_apparent.ra
    dec = coord_apparent.dec
    # return SkyCoord of (Ha, Dec)
    return SkyCoord(ha, dec, frame=coord_system)


def ha_to_ra(ha, dec, t):
    """
    Convert WSRT HA, Dec to J2000 RA, Dec

    :param astropy.units.quantity.Quantity ha: hour angle with unit
    :param astropy.units.quantity.Quantity dec: declination with unit
    :param str/astropy.time.Time t: UTC time
    :return: SkyCoord object of J2000 coordinates
    """

    # Convert time to Time object if given as string
    if isinstance(t, str):
        t = Time(t)

    # Apparent LST at WSRT at this time
    lst = t.sidereal_time('apparent', WSRT_LON)
    # Equinox of date (because hour angle uses apparent coordinates)
    coord_system = FK5(equinox='J{}'.format(t.decimalyear))
    # apparent RA = LST - HA
    ra_apparent = lst - ha
    coord_apparent = SkyCoord(ra_apparent, dec, frame=coord_system)
    return coord_apparent.transform_to('icrs')


def ha_to_proj(ha, dec):
    """
    Convert WSRT HA, Dec to parallactic angle

    :param astropy.units.quantity.Quantity ha: hour angle with unit
    :param astropy.units.quantity.Quantity dec: declination with unit
    """
    theta_proj = np.arctan(np.cos(WSRT_LAT) * np.sin(ha)
                           / (np.sin(WSRT_LAT) * np.cos(dec)
                           - np.cos(WSRT_LAT) * np.sin(dec) * np.cos(ha))).to(u.deg)
    return theta_proj.to(u.deg)


def dm_to_delay(dm, flo, fhi):
    """
    Convert DM to time delay

    :param astropy.units.quantity.Quantity dm: dispersion measure
    :param astropy.units.quantity.Quantity flo: lowest frequency
    :param astropy.units.quantity.Quantity fhi: highest frequency
    :return: time delay (astropy quantity)
    """
    k = 4.148808e3 * u.cm**3 * u.s * u.MHz**2 / u.pc
    delay = k * dm * (flo**-2 - fhi**-2)
    return delay.to(u.s)


def send_email(frm, to, subject, body, attachments=None):
    """
    Send email, only possible to ASTRON addresses

    :param str frm: From address
    :param str/list to: To addresses, either single or list of addresses
    :param str subject: Subject of email
    :param dict body: Dict with e-mail content (str) and type (str)
    :param dict/list attachments: optional dict or list of dicts with attachment path (str), type (str), and name (str)
    """

    # ensure "to" is a single string
    if isinstance(to, list):
        to = ', '.join(to)

    # init email
    msg = MIMEMultipart('mixed')
    # set to, from, subject
    msg['To'] = to
    msg['From'] = frm
    msg['Subject'] = subject

    # add body
    msg.attach(MIMEText(body['content'], body['type']))

    # add attachments
    if attachments is not None:
        # ensure it is a list
        if not isinstance(attachments, list):
            attachments = [attachments]
        for attachment in attachments:
            fname = attachment['path']
            name = attachment['name']
            typ = attachment['type']
            # load the file
            with open(fname, 'rb') as f:
                part = MIMEApplication(f.read(), typ)
                # set filename
                part['Content-Disposition'] = 'attachment; filename="{}"'.format(name)
            # attach to email
            msg.attach(part)

    # send the e-mail
    smtp = smtplib.SMTP()
    smtp.connect()
    try:
        smtp.sendmail(frm, to, msg.as_string())
    except smtplib.SMTPSenderRefused:
        # assume failed because messages is too big
        # so send again without attachments
        # first element of payload is main text, rest are the attachments
        msg.set_payload(msg.get_payload()[0])
        # send again
        smtp.sendmail(frm, to, msg.as_string())

    smtp.close()
