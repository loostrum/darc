#!/usr/bin/env python3
#
# utility functions

import os
import errno
import ast
import subprocess
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
from astropy.coordinates import SkyCoord, SphericalRepresentation
from queue import Empty

from darc.definitions import DISH_DIAM, TSYS, AP_EFF, BANDWIDTH, WSRT_LOC, NDISH


def sleepuntil_utc(end_time, event=None):
    """
    Sleep until specified time

    :param datetime.datetime/astropy.time.Time/str end_time: sleep until this time
    :param threading.Event event: if specified, uses event.wait instead of time.sleep
    """

    # convert datetime to astropy time
    if isinstance(end_time, datetime.datetime) or isinstance(end_time, str):
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
    # remove empty/whitespace-only lines and any line starting with #
    parset = []
    for line in raw_parset:
        if line.strip() and not line.startswith('#'):
            parset.append(line)
    # Split keys/values. Separator might be "=" or " = "
    parset = [item.replace(' = ', '=').strip().split('=') for item in parset]
    # remove empty last line if present
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
    # lists
    for key in ['beams']:
        if key in parset_dict.keys():
            parset_dict[key] = json.loads(parset_dict[key])
    return parset_dict


def tail(f, event, interval=.1):
    """
    Read all lines of a file, then tail until stop event is set

    :param filehandle f: handle to file to tail
    :param threading.Event event: stop event
    :param float interval: sleep time between checks for new lines (default: .1)
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


def radec_to_hadec(ra, dec, t):
    """
    Convert RA, Dec to apparent WSRT HA, Dec
    :param Quantity ra: Right ascension
    :param Quantity dec: Declination
    :param Time/str t: Observing time
    :return: HA (Quantity), Dec (Quantity)
    """

    # Convert time to Time object if given as string
    if isinstance(t, str):
        t = Time(t)

    coord = SkyCoord(ra, dec, frame='icrs', obstime=t)
    ha = WSRT_LOC.lon - coord.itrs.spherical.lon
    ha.wrap_at(12 * u.hourangle, inplace=True)
    dec = coord.itrs.spherical.lat

    return ha, dec


def hadec_to_radec(ha, dec, t):
    """
    Convert apparent HA, Dec to J2000 RA, Dec
    :param ha: hour angle with unit
    :param dec: declination with unit
    :param Time/str t: Observing time
    :return: SkyCoord object of J2000 coordinates
    """

    # Convert time to Time object if given as string
    if isinstance(t, str):
        t = Time(t)

    # create spherical representation of ITRS coordinates of given ha, dec
    itrs_spherical = SphericalRepresentation(WSRT_LOC.lon - ha, dec, 1.)
    # create ITRS object, which requires cartesian input
    coord = SkyCoord(itrs_spherical.to_cartesian(), frame='itrs', obstime=t)
    # convert to J2000
    return coord.icrs.ra, coord.icrs.dec


def hadec_to_rot(ha, dec):
    """
    Convert WSRT HA, Dec to TAB rotation angle

    :param astropy.units.quantity.Quantity ha: hour angle with unit
    :param astropy.units.quantity.Quantity dec: declination with unit
    """
    theta_rot = np.arctan2(np.sin(dec) * np.sin(ha), np.cos(ha))

    return theta_rot.to(u.deg)


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


def dm_to_smearing(dm, f, df):
    """
    Calculate intra-channel smearing time

    :param astropy.units.quantity.Quantity dm: dispersion measure
    :param astropy.units.quantity.Quantity f: frequency
    :param astropy.units.quantity.Quantity df: channel width
    """
    k = 4.148808e3 * u.cm ** 3 * u.s * u.MHz ** 2 / u.pc
    smearing = 2 * k * dm * df * f**-3
    return smearing.to(u.s)


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
        to_str = ', '.join(to)
    else:
        to_str = to

    # init email
    msg = MIMEMultipart('mixed')
    # set to, from, subject
    msg['To'] = to_str
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


def calc_snr_amber(data, thresh=3):
    """
    Calculate peak S/N using the same method as AMBER:
    Outliers are removed four times before calculating the S/N as peak - median / sigma
    The result is scaled by 1.048 to account for the removed values

    :param array data: timeseries data
    :param float thresh: sigma threshold for outliers (Default: 3)
    :return: peak S/N
    """
    sig = np.std(data)
    dmax = data.max()
    dmed = np.median(data)

    # remove outliers 4 times until there
    # are no events above threshold * sigma
    for i in range(4):
        ind = np.abs(data - dmed) < thresh * sig
        sig = np.std(data[ind])
        dmed = np.median(data[ind])
        data = data[ind]

    return (dmax - dmed) / (1.048 * sig)


def calc_snr_matched_filter(data, widths=None):
    """
    Calculate S/N using several matched filter widths, then pick the highest S/N

    :param np.ndarray data: timeseries data
    :param np.ndarray widths: matched filters widths to try
                             (Default: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500])
    :return: highest S/N, corresponding matched filter width
    """
    if widths is None:
        # all possible widths as used by AMBER
        widths = np.array([1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500])

    snr_max = 0
    width_max = None

    # get S/N for each width, store only max S/N
    for w in widths:
        # apply boxcar-shaped filter
        mf = np.ones(w)
        data_mf = np.correlate(data, mf)

        # get S/N
        snr = calc_snr_amber(data_mf)

        # store if S/N is highest
        if snr > snr_max:
            snr_max = snr
            width_max = w

    return snr_max, width_max


def get_ymw16(parset, beam=0, logger=None):
    """
    Get YMW16 DM

    :param dict parset: Observation parset
    :param int beam: CB for which to get YMW16 DM
    :param Logger logger: Logger object (optional)
    :return: YMW16 DM (float)
    """
    # get pointing
    try:
        key = "task.beamSet.0.compoundBeam.{}.phaseCenter".format(beam)
        c1, c2 = ast.literal_eval(parset[key].replace('deg', ''))
        c1 = c1 * u.deg
        c2 = c2 * u.deg
    except Exception as e:
        if logger is not None:
            logger.error("Could not parse pointing for CB{:02d}, setting YMW16 DM to zero ({})".format(beam, e))
        return 0
    # convert HA to RA if HADEC is used
    if parset['task.directionReferenceFrame'].upper() == 'HADEC':
        # Get RA at the mid point of the observation
        timestamp = Time(parset['task.startTime']) + .5 * parset['task.duration'] * u.s
        c1, c2 = radec_to_hadec(c1, c2, timestamp)

    pointing = SkyCoord(c1, c2)

    # ymw16 arguments: mode, Gl, Gb, dist(pc), 2=dist->DM. 1E6 pc should cover entire MW
    gl, gb = pointing.galactic.to_string(precision=8).split(' ')
    cmd = ['ymw16', 'Gal', gl, gb, '1E6', '2']
    try:
        result = subprocess.check_output(cmd)
    except OSError as e:
        if logger is not None:
            logger.error("Failed to run ymw16, setting YMW16 DM to zero: {}".format(e))
        return 0
    try:
        dm = float(result.split()[7])
    except Exception as e:
        if logger is not None:
            logger.error('Failed to parse DM from YMW16 output {}, setting YMW16 DM to zero: {}'.format(result, e))
        return 0
    return dm
