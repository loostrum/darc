#!/usr/bin/env python
#
# VOEvent Generator

import errno
import yaml
import logging
import logging.handlers
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.time import Time
import voeventparse as vp
import datetime
import pytz
from xml.dom import minidom

from darc.definitions import *


class VOEventGeneratorException(Exception):
    pass


class VOEventGenerator(threading.Thread):
    """
    Generate VOEvent from incoming trigger
    """
    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self.daemon = True

        self.voevent_queue = None

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f)['voevent_generator']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~')}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
            setattr(self, key, value)

        # setup logger
        handler = logging.handlers.WatchedFileHandler(self.log_file)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        # create and cd to voevent directory
        try:
            os.makedirs(self.voevent_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                self.logger.error("Cannot create voevent directory:, {}".format(e))
                raise VOEventGeneratorException("Cannot create voevent directory")
        os.chdir(self.voevent_dir)

    def set_source_queue(self, queue):
        """
        :param queue: Source queue
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise VOEventGeneratorException('Given source queue is not an instance of Queue')
        self.voevent_queue = queue

    def run(self):
        """
        Read triggers from queue and call processing for each trigger
        """
        if not self.voevent_queue:
            self.logger.error('Queue not set')
            raise VOEventGeneratorException('Queue not set')

        self.logger.info("Running VOEvent generator")
        while not self.stop_event.is_set():
            try:
                trigger = self.voevent_queue.get(timeout=1)
            except Empty:
                continue
            self.logger.info("Received trigger: {}".format(trigger))
            self.create_and_send(trigger)

        self.logger.info("Stopping VOEvent generator")

    def create_and_send(self, trigger):
        """
        Creates VOEvent
        Sends if enabled in config
        :param trigger: Trigger event
        """
        if not isinstance(trigger, dict):
            self.logger.error("Trigger is not a dict")
            return

        # check if all required params are present
        # gl and gb are generated from ra and dec
        # semiMaj and semiMin have default set for IAB
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

        self.logger.info("Creating VOEvent")
        self.NewVOEvent(**trigger)
        self.logger.info("Generated VOEvent")

        if self.send_events:
            self.logger.info("Sending VOEvent")
            # Filename is {utc}.xml
            filename = os.path.join(self.voevent_dir, "{}.xml".format(trigger['utc']))
            if not os.path.isfile(filename):
                self.logger.error("Cannot find XML file to send")
                return
            cmd = "comet-sendfo -f {xmlfile} --host={host} " \
                  "--port={port}".format(xmlfile=filename, host=self.broker_host,
                                         port=self.broker_port)
            # to be replaced by subprocess
            # and check if sent successfully
            os.system(cmd)

    def NewVOEvent(self, dm, dm_err, width, snr, flux, ra, dec, semiMaj, semiMin,
                   ymw16, name, importance, utc, gl, gb):

        z = dm/1200.0  #May change
        errDeg = semiMaj/60.0

        # Parse UTC
        utc_YY = int(utc[:4])
        utc_MM = int(utc[5:7])
        utc_DD = int(utc[8:10])
        utc_hh = int(utc[11:13])
        utc_mm = int(utc[14:16])
        utc_ss = float(utc[17:])
        t = Time('T'.join([utc[:10], utc[11:]]), scale='utc', format='isot')
        mjd = t.mjd

        now = Time.now()
        mjd_now = now.mjd

        ivorn = ''.join([name, str(utc_hh), str(utc_mm), '/', str(mjd_now)])

        # v = vp.Voevent(stream='nl.astron.apertif/alert', stream_id=ivorn,
        #                role=vp.definitions.roles.observation)
        v = vp.Voevent(stream='nl.astron.apertif/alert', stream_id=ivorn,
                       role=vp.definitions.roles.test)
        # Author origin information
        vp.set_who(v, date=datetime.datetime.utcnow(), author_ivorn="nl.astron")
        # Author contact information
        vp.set_author(v, title="ASTRON ALERT FRB Detector", contactName="Leon Oostrum",
                      contactEmail="leonoostrum@gmail.com", shortName="ALERT")
        # Parameter definitions

        # Apertif-specific observing configuration %%TODO: update parameters as necessary for new obs config
        beam_sMa = vp.Param(name="beam_semi-major_axis", unit="MM",
                            ucd="instr.beam;pos.errorEllipse;phys.angSize.smajAxis", ac=True, value=semiMaj)
        beam_sma = vp.Param(name="beam_semi-minor_axis", unit="MM",
                            ucd="instr.beam;pos.errorEllipse;phys.angSize.sminAxis", ac=True, value=semiMin)
        beam_rot = vp.Param(name="beam_rotation_angle", value=0.0, unit="Degrees",
                            ucd="instr.beam;pos.errorEllipse;instr.offset", ac=True)
        tsamp = vp.Param(name="sampling_time", value=0.08192, unit="ms", ucd="time.resolution", ac=True)
        bw = vp.Param(name="bandwidth", value=300.0, unit="MHz", ucd="instr.bandwidth", ac=True)
        nchan = vp.Param(name="nchan", value="1536", dataType="int",
                         ucd="meta.number;em.freq;em.bin", unit="None")
        cf = vp.Param(name="centre_frequency", value=1400.0, unit="MHz", ucd="em.freq;instr", ac=True)
        npol = vp.Param(name="npol", value="2", dataType="int", unit="None")
        bits = vp.Param(name="bits_per_sample", value="8", dataType="int", unit="None")
        gain = vp.Param(name="gain", value=1.0, unit="K/Jy", ac=True)
        tsys = vp.Param(name="tsys", value=75.0, unit="K", ucd="phot.antennaTemp", ac=True)
        backend = vp.Param(name="backend", value="ARTS")
        # beam = vp.Param(name="beam", value= )

        v.What.append(vp.Group(params=[beam_sMa, beam_sma, beam_rot, tsamp,
                                       bw, nchan, cf, npol, bits, gain, tsys, backend],
                               name="observatory parameters"))

        # Event parameters
        DM = vp.Param(name="dm", ucd="phys.dispMeasure", unit="pc/cm^3", ac=True, value=str(dm))
        # DM_err = vp.Param(name="dm_err", ucd="stat.error;phys.dispMeasure", unit="pc/cm^3", ac=True, value=dm_err)
        Width = vp.Param(name="width", ucd="time.duration;src.var.pulse", unit="ms", ac=True, value=str(width))
        SNR = vp.Param(name="snr", ucd="stat.snr", unit="None", ac=True, value=str(snr))
        Flux = vp.Param(name="flux", ucd="phot.flux", unit="Jy", ac=True, value=str(flux))
        Flux.Description = "Calculated from radiometer equation. Not calibrated."
        Gl = vp.Param(name="gl", ucd="pos.galactic.lon", unit="Degrees", ac=True, value=str(gl))
        Gb = vp.Param(name="gb", ucd="pos.galactic.lat", unit="Degrees", ac=True, value=str(gb))

        v.What.append(vp.Group(params=[DM, Width, SNR, Flux, Gl, Gb], name="event parameters"))
        # v.What.append(vp.Group(params=[DM, DM_err, Width, SNR, Flux, Gl, Gb], name="event parameters"))

        # Advanced parameters (note, change script if using a differeing MW model)
        mw_dm = vp.Param(name="MW_dm_limit", unit="pc/cm^3", ac=True, value=str(ymw16))
        mw_model = vp.Param(name="galactic_electron_model", value="YMW16")
        redshift_inferred = vp.Param(name="redshift_inferred", ucd="src.redshift", unit="None", value=str(z))
        redshift_inferred.Description = "Redshift estimated using z = DM/1200.0 (Ioka 2003)"

        v.What.append(vp.Group(params=[mw_dm, mw_model, redshift_inferred], name="advanced parameters"))


        # WhereWhen
        vp.add_where_when(v, coords=vp.Position2D(ra=ra, dec=dec, err=errDeg, units='deg',
                                                  system=vp.definitions.sky_coord_system.utc_fk5_geo),
                          obs_time=datetime.datetime(utc_YY,utc_MM,utc_DD,utc_hh,utc_mm,int(utc_ss),
                                                     tzinfo=pytz.UTC),
                          observatory_location="WSRT")

        # Why
        vp.add_why(v, importance=importance)
        v.Why.Name = name

        if vp.valid_as_v2_0(v):
            with open('{}.xml'.format(utc), 'wb') as f:
                voxml = vp.dumps(v)
                xmlstr = minidom.parseString(voxml).toprettyxml(indent="   ")
                f.write(xmlstr)
                self.logger.info(vp.prettystr(v.Who))
                self.logger.info(vp.prettystr(v.What))
                self.logger.info(vp.prettystr(v.WhereWhen))
                self.logger.info(vp.prettystr(v.Why))
        else:
            self.logger.error("Unable to write file {}.xml".format(name))