#!/usr/bin/env python
#
# VOEvent Generator

import os
import yaml
import multiprocessing as mp
import subprocess
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
from time import sleep
import numpy as np
import threading
import socket
from multiprocessing.managers import BaseManager
from astropy.coordinates import SkyCoord
import astropy.units as u
import astropy.constants as const
from astropy.time import Time
import pytz
import voeventparse as vp
import datetime
from xml.dom import minidom

from darc.definitions import TSAMP, BANDWIDTH, NCHAN, TSYS, AP_EFF, DISH_DIAM, NDISH, CONFIG_FILE
from darc import util
from darc.logger import get_logger


class VOEventQueueServer(BaseManager):
    pass


class VOEventGeneratorException(Exception):
    pass


class VOEventGenerator(threading.Thread):
    """
    Generate VOEvent from incoming trigger
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.daemon = True

        self.voevent_server = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['voevent_generator']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        # create and cd to voevent directory
        try:
            util.makedirs(self.voevent_dir)
        except Exception as e:
            self.logger.error("Cannot create voevent directory: {}".format(e))
            raise VOEventGeneratorException("Cannot create voevent directory")
        os.chdir(self.voevent_dir)

        # Initalize the queue server
        voevent_queue = mp.Queue()
        VOEventQueueServer.register('get_queue', callable=lambda: voevent_queue)
        self.voevent_queue = voevent_queue

        self.logger.info("VOEvent Generator initialized")

    def stop(self):
        """
        Stop the service
        """
        self.stop_event.set()

    def run(self):
        """
        Read triggers from queue and process them
        """

        # start the queue server
        self.voevent_server = VOEventQueueServer(address=('', self.server_port),
                                                 authkey=self.server_auth.encode())
        self.voevent_server.start()

        # wait for events until stop is set
        while not self.stop_event.is_set():
            try:
                trigger = self.voevent_queue.get(timeout=.1)
            except Empty:
                continue
            else:
                # a trigger was received, wait and read queue again in case there are multiple triggers
                sleep(self.interval)
                additional_triggers = []
                # read queue without waiting
                while True:
                    try:
                        additional_trigger = self.voevent_queue.get_nowait()
                    except Empty:
                        break
                    else:
                        additional_triggers.append(additional_trigger)

                # add additional triggers if there are any
                if additional_triggers:
                    trigger = [trigger] + additional_triggers

            self.create_and_send(trigger)

        # stop the queue server
        self.voevent_server.shutdown()
        self.logger.info("Stopping VOEvent generator")

    def create_and_send(self, trigger):
        """
        Creates VOEvent
        Sends if enabled in config
        :param trigger: Trigger event(s). dict if one event, list of dicts if multiple events
        """

        # if multiple triggers are received, select one
        if isinstance(trigger, list):
            self.logger.info("Received {} triggers, selecting highest S/N".format(len(trigger)))
            trigger = self._select_trigger(trigger)

        self.logger.info("Trigger: {}".format(trigger))

        # trigger should be a dict
        if not isinstance(trigger, dict):
            self.logger.error("Trigger is not a dict")
            return

        # check if all required params are present
        # others are calculated
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

        # calculate gain
        gain = (AP_EFF * np.pi * (DISH_DIAM / 2.) ** 2 / (2 * const.k_B) * NDISH).to(u.Kelvin/(1000*u.mJy)).value
        trigger['gain'] = gain

        # calculate parallactic angle - Useful when using SB for pointing instead of center of CB
        t = Time(trigger['utc'])
        # IERS server is down, avoid using it
        t.delta_ut1_utc = 0
        hadec = util.ra_to_ha(coord.ra, coord.dec, t)
        trigger['posang'] = util.ha_to_proj(hadec.ra, hadec.dec).to(u.deg).value

        self.logger.info("Creating VOEvent")
        self._NewVOEvent(**trigger)
        self.logger.info("Created event")

        if self.send_events:
            self.logger.info("Sending VOEvent")
            # Filename is {utc}.xml
            filename = os.path.join(self.voevent_dir, "{}.xml".format(trigger['utc']))
            if not os.path.isfile(filename):
                self.logger.error("Cannot find XML file to send")
                return
            cmd = "comet-sendvo -f {xmlfile} --host={host} " \
                  "--port={port}".format(xmlfile=filename, host=self.broker_host,
                                         port=self.broker_port)
            self.logger.info("Running {}".format(cmd))
            try:
                subprocess.check_output(cmd, shell=True)
            except subprocess.CalledProcessError as e:
                self.logger.error("Failed to send VOEvent: {}".format(e.output))
            else:
                self.logger.info("VOEvent sent - disabling future LOFAR triggering")
                self.send_events = False
        else:
            self.logger.warning("Sending VOEvents is disabled - Cancelling trigger")

    @staticmethod
    def _select_trigger(triggers):
        """
        :param triggers: list of trigger dictionaries
        :return: trigger with highest S/N
        """
        max_snr = 0
        index = None
        # loop over triggers and check if current trigger has highest S/N
        for i, trigger in enumerate(triggers):
            snr = trigger['snr']
            if snr > max_snr:
                max_snr = snr
                index = i
        # index is now index of trigger with highest S/N
        return triggers[index]

    def _NewVOEvent(self, dm, dm_err, width, snr, flux, ra, dec, semiMaj, semiMin,
                    ymw16, name, importance, utc, gl, gb, gain,
                    dt=TSAMP.to(u.ms).value, delta_nu_MHz=(BANDWIDTH/NCHAN).to(u.MHz).value,
                    nu_GHz=1.37, posang=0, test=False):

        z = dm/1200.0  # May change
        errDeg = semiMaj/60.0

        # Parse UTC
        utc_YY = int(utc[:4])
        utc_MM = int(utc[5:7])
        utc_DD = int(utc[8:10])
        utc_hh = int(utc[11:13])
        utc_mm = int(utc[14:16])
        utc_ss = float(utc[17:])
        t = Time(utc, scale='utc', format='isot')
        # IERS server is down, avoid using it
        t.delta_ut1_utc = 0
        mjd = t.mjd

        ivorn = ''.join([name, str(utc_hh), str(utc_mm), '/', str(mjd)])

        # Set role to either test or real observation
        if test:
            v = vp.Voevent(stream='nl.astron.apertif/alert', stream_id=ivorn,
                           role=vp.definitions.roles.test)
        else:
            v = vp.Voevent(stream='nl.astron.apertif/alert', stream_id=ivorn,
                           role=vp.definitions.roles.observation)
        # Author origin information
        vp.set_who(v, date=datetime.datetime.utcnow(), author_ivorn="nl.astron")
        # Author contact information
        vp.set_author(v, title="ARTS FRB alert system", contactName="Leon Oostrum",
                      contactEmail="oostrum@astron.nl", shortName="ALERT")
        # Parameter definitions

        # Apertif-specific observing configuration
        beam_sMa = vp.Param(name="beam_semi-major_axis", unit="MM",
                            ucd="instr.beam;pos.errorEllipse;phys.angSize.smajAxis", ac=True, value=semiMaj)
        beam_sma = vp.Param(name="beam_semi-minor_axis", unit="MM",
                            ucd="instr.beam;pos.errorEllipse;phys.angSize.sminAxis", ac=True, value=semiMin)
        beam_rot = vp.Param(name="beam_rotation_angle", value=posang, unit="Degrees",
                            ucd="instr.beam;pos.errorEllipse;instr.offset", ac=True)
        tsamp = vp.Param(name="sampling_time", value=dt, unit="ms", ucd="time.resolution", ac=True)
        bw = vp.Param(name="bandwidth", value=delta_nu_MHz, unit="MHz", ucd="instr.bandwidth", ac=True)
        nchan = vp.Param(name="nchan", value=str(NCHAN), dataType="int",
                         ucd="meta.number;em.freq;em.bin", unit="None")
        cf = vp.Param(name="centre_frequency", value=str(1000*nu_GHz), unit="MHz", ucd="em.freq;instr", ac=True)
        npol = vp.Param(name="npol", value="2", dataType="int", unit="None")
        bits = vp.Param(name="bits_per_sample", value="8", dataType="int", unit="None")
        gain = vp.Param(name="gain", value=gain, unit="K/Jy", ac=True)
        tsys = vp.Param(name="tsys", value=TSYS.to(u.Kelvin).value, unit="K", ucd="phot.antennaTemp", ac=True)
        backend = vp.Param(name="backend", value="ARTS")
        # beam = vp.Param(name="beam", value= )

        v.What.append(vp.Group(params=[beam_sMa, beam_sma, beam_rot, tsamp,
                                       bw, nchan, cf, npol, bits, gain, tsys, backend],
                               name="observatory parameters"))

        # Event parameters
        DM = vp.Param(name="dm", ucd="phys.dispMeasure", unit="pc/cm^3", ac=True, value=str(dm))
        DM_err = vp.Param(name="dm_err", ucd="stat.error;phys.dispMeasure", unit="pc/cm^3", ac=True, value=dm_err)
        Width = vp.Param(name="width", ucd="time.duration;src.var.pulse", unit="ms", ac=True, value=str(width))
        SNR = vp.Param(name="snr", ucd="stat.snr", unit="None", ac=True, value=str(snr))
        Flux = vp.Param(name="flux", ucd="phot.flux", unit="Jy", ac=True, value=str(flux))
        Flux.Description = "Calculated from radiometer equation. Not calibrated."
        Gl = vp.Param(name="gl", ucd="pos.galactic.lon", unit="Degrees", ac=True, value=str(gl))
        Gb = vp.Param(name="gb", ucd="pos.galactic.lat", unit="Degrees", ac=True, value=str(gb))

        # v.What.append(vp.Group(params=[DM, Width, SNR, Flux, Gl, Gb], name="event parameters"))
        v.What.append(vp.Group(params=[DM, DM_err, Width, SNR, Flux, Gl, Gb], name="event parameters"))

        # Advanced parameters (note, change script if using a differeing MW model)
        mw_dm = vp.Param(name="MW_dm_limit", unit="pc/cm^3", ac=True, value=str(ymw16))
        mw_model = vp.Param(name="galactic_electron_model", value="YMW16")
        redshift_inferred = vp.Param(name="redshift_inferred", ucd="src.redshift", unit="None", value=str(z))
        redshift_inferred.Description = "Redshift estimated using z = DM/1200.0 (Ioka 2003)"

        v.What.append(vp.Group(params=[mw_dm, mw_model, redshift_inferred], name="advanced parameters"))


        # WhereWhen
        vp.add_where_when(v, coords=vp.Position2D(ra=ra, dec=dec, err=errDeg, units='deg',
                                                  system=vp.definitions.sky_coord_system.utc_fk5_geo),
                          obs_time=datetime.datetime(utc_YY, utc_MM, utc_DD, utc_hh, utc_mm, int(utc_ss),
                                                     tzinfo=pytz.UTC),
                          observatory_location="WSRT")

        # Why
        vp.add_why(v, importance=importance)
        v.Why.Name = name

        if vp.valid_as_v2_0(v):
            with open('{}.xml'.format(utc), 'wb') as f:
                voxml = vp.dumps(v)
                xmlstr = minidom.parseString(voxml).toprettyxml(indent="   ")
                f.write(xmlstr.encode())
                self.logger.info(vp.prettystr(v.Who))
                self.logger.info(vp.prettystr(v.What))
                self.logger.info(vp.prettystr(v.WhereWhen))
                self.logger.info(vp.prettystr(v.Why))
        else:
            self.logger.error("Unable to write file {}.xml".format(name))

