#!/usr/bin/env python
#
# AMBER Clustering

import os
from time import sleep
import yaml
import ast
import subprocess
import threading
import multiprocessing as mp
import numpy as np
from astropy.time import Time, TimeDelta
import astropy.units as u
from astropy.coordinates import SkyCoord


from darc.base import DARCBase
from darc.voevent_generator import VOEventQueueServer
from darc.definitions import TSAMP, NCHAN, BANDWIDTH, WSRT_LON, CONFIG_FILE, MASTER
from darc.external import tools
from darc import util


class AMBERClusteringException(Exception):
    pass


class AMBERClustering(DARCBase):
    """
    Cluster AMBER clusters
    """

    def __init__(self, connect_vo=True):
        super(AMBERClustering, self).__init__()
        self.needs_source_queue = True
        self.needs_target_queue = True

        self.connect_vo = connect_vo
        self.dummy_queue = mp.Queue()

        self.threads = {}
        self.hdr_mapping = {}
        self.obs_config = None
        self.observation_running = False
        self.amber_triggers = []
        self.source_list = None

        # store when we are allowed to do IQUV / LOFAR triggering
        self.time_iquv = Time.now()

        # connect to VOEvent generator
        if self.connect_vo:
            try:
                self.vo_queue = self.voevent_connector()
                self.logger.info("Connected to VOEvent Generator")
                self.have_vo = True
            except Exception as e:
                self.logger.error("Failed to connect to VOEvent Generator, setting dummy queue ({})".format(e))
                self.vo_queue = self.dummy_queue
                self.have_vo = False
        else:
            # dummy queue
            self.logger.info("VO Generator connection disabled, setting dummy queue")
            self.vo_queue = mp.Queue()
            self.have_vo = False

    def _load_source_list(self):
        """
        Load the list with known source DMs
        :return: source list with dict per category
        """
        try:
            with open(self.source_file, 'r') as f:
                source_list = yaml.load(f, Loader=yaml.SafeLoader)
        except OSError as e:
            raise AMBERClusteringException("Cannot load source list: {}".format(e))
        return source_list

    def process_command(self, command):
        """
        Process command received from queue
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                self.amber_triggers.append(command['trigger'])
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def start_observation(self, obs_config):
        """
        Parse obs config and start listening for amber triggers on queue
        """

        # clean any old triggers
        self.amber_triggers = []
        # parse parset
        obs_config['parset'] = self._load_parset(obs_config)
        # set config
        self.obs_config = obs_config

        self.observation_running = True

        # (re)load source list in case of changes
        self.source_list = self._load_source_list()

        # try connecting to VO server if enabled
        # always do this in case a connection was available before, but failed at some point
        if self.connect_vo:
            try:
                self.vo_queue = self.voevent_connector()
                self.logger.info("Connected to VOEvent Generator")
                self.have_vo = True
            except Exception as e:
                self.logger.error("Failed to connect to VOEvent Generator, setting dummy queue ({})".format(e))
                self.vo_queue = self.dummy_queue
                self.have_vo = False

        # process triggers in thread
        self.threads['processing'] = threading.Thread(target=self._process_triggers)
        self.threads['processing'].daemon = True
        self.threads['processing'].start()

        self.logger.info("Observation started")

    def stop_observation(self):
        """
        Stop observation
        """
        # set running to false
        self.observation_running = False
        # clear triggers
        self.amber_triggers = []
        # clear header
        self.hdr_mapping = {}
        # clear config
        self.obs_config = None
        # clear threads
        for key, thread in self.threads.items():
            if thread is not None:
                thread.join()
                self.threads[key] = None

    @staticmethod
    def voevent_connector():
        """
        Connect to the VOEvent generator on the master node
        """
        # Load VO server settings
        VOEventQueueServer.register('get_queue')
        with open(CONFIG_FILE, 'r') as f:
            server_config = yaml.load(f, Loader=yaml.SafeLoader)['voevent_generator']
        port = server_config['server_port']
        key = server_config['server_auth'].encode()
        server = VOEventQueueServer(address=(MASTER, port), authkey=key)
        server.connect()
        return server.get_queue()

    def _get_source(self):
        """
        Try to get DM for a known source
        :return: DM for known source, else None
        """
        # get source name from parset
        try:
            source = self.obs_config['parset']['task.source.name']
        except KeyError:
            self.logger.error("Cannot get source name from parset, will not do known-source triggering")
            return None

        # check if source is in source list
        # first check aliases
        try:
            alias = self.source_list['aliases'][source]
        except KeyError:
            # not found
            pass
        else:
            # replace source by alias so we can look it up in other lists
            self.logger.info("Using alias {} for source {}".format(alias, source))
            source = alias

        # check if source is a known pulsar or frb
        dm_src = None
        src_type = None
        for key in ['pulsars', 'frbs']:
            try:
                dm_src = self.source_list[key][source]
                src_type = key[:-1]
            except KeyError:
                pass
            else:
                break

        return dm_src, src_type

    def _check_triggers(self, triggers, sys_params, utc_start, dm_min=0, dm_max=np.inf, dm_src=None,
                        width_max=np.inf, snr_min=8, src_type=None, dmgal=0, pointing=None):
        """
        Cluster triggers and run IQUV and/or LOFAR triggering
        :param triggers: list of raw triggers
        :param sys_params: dict of system parameters (dt, delta_nu_MHz, nu_GHz)
        :param utc_start: start time of observation
        :param dm_min: minimum DM (default: 0)
        :param dm_max: maximum DM (default: inf)
        :param dm_src: DM of known source (default: None)
        :param width_max: maximum width (default: inf)
        :param snr_min: mininum S/N (default: 8)
        :param src_type: Source type (pulsar, frb, None)
        :param dmgal: galactic maximum DM
        :param pointing: SkyCoord object with pointing for LOFAR triggering (default: None)
        """
        # cluster using IQUV thresholds
        # LOFAR thresholds are assumed to be more strict for every parameter
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _ = \
            tools.get_triggers(triggers,
                               dm_min=dm_min, dm_max=dm_max,
                               sig_thresh=snr_min,
                               read_beam=True, **sys_params)

        # select on width
        mask = np.array(cluster_downsamp) <= width_max
        cluster_snr = np.array(cluster_snr)[mask]
        cluster_dm = np.array(cluster_dm)[mask]
        cluster_time = np.array(cluster_time)[mask]
        cluster_downsamp = np.array(cluster_downsamp)[mask].astype(int)
        cluster_sb = np.array(cluster_sb)[mask].astype(int)
        ncluster = len(cluster_snr)

        if src_type is not None:
            known = 'known'
        else:
            known = 'new'
        
        self.logger.info("Clustered {} raw triggers into {} IQUV trigger(s) "
                         "for {} source".format(len(triggers), ncluster, known))

        # return if there are no clusters
        if ncluster == 0:
            return

        # there are clusters, do IQUV triggering
        # check if we can do triggering
        now = Time.now()
        if now < self.time_iquv:
            self.logger.warning("Cannot trigger IQUV yet, next possible time: {}".format(self.time_iquv))
        else:
            self.logger.info("Sending IQUV trigger")
            # update last trigger time
            self.time_iquv = now + TimeDelta(self.thresh_iquv['interval'], format='sec')
            # trigger IQUV
            dada_triggers = []
            for i in range(ncluster):
                # send known source dm if available
                if dm_src is not None:
                    dm_to_send = dm_src
                else:
                    dm_to_send = cluster_dm[i]
                dada_trigger = {'stokes': 'IQUV', 'dm': dm_to_send, 'beam': cluster_sb[i],
                                'width': cluster_downsamp[i], 'snr': cluster_snr[i],
                                'time': cluster_time[i], 'utc_start': utc_start}
                dada_triggers.append(dada_trigger)
            self.target_queue.put({'command': 'trigger', 'trigger': dada_triggers})

        ###########
        # Print brightest trigger for pulsar to debug LOFAR triggering
        # only do this for CB00 (assuming that is where the pulsar is, more
        # elaborate checks are possible but not required at this point)
        try:
            beam = self.obs_config['beam']
        except Exception:
            beam = None
        if src_type == 'pulsar' and int(beam) == 0 and dm_src is not None:
            # select brightest trigger
            ind = np.argmax(cluster_snr)
            dm = cluster_dm[ind]
            snr = cluster_snr[ind]
            width = cluster_downsamp[ind] * 81.92E-3
            # calculate TBB end time: arrival time at 200 MHz + half of 5s buffer
            dm_delay = dm_src * 4.15E3 * (1520.**-2 - 200.**-2)  # in seconds
            delay = TimeDelta(dm_delay + 2.5, format='sec')
            # time should be integer unix time
            time_lofar = int(np.round((utc_start + TimeDelta(cluster_time[ind], format='sec') + delay).unix))
            self.logger.warning("TRIGGER: UTC: {} S/N: {} Width: {} ms DM: {} pc/cc".format(time_lofar, snr, width, dm))
        ###########

        # skip LOFAR triggering for pulsars
        if src_type == 'pulsar':
            self.logger.warning("Skipping LOFAR triggering for pulsars")
            return

        # select LOFAR thresholds
        if src_type is not None:
            # known source, use same width and DM threshold as IQUV, but apply S/N threshold
            # DM_min and width_max effectively do nothing here, but they need to be defined
            # for the mask = line below to work
            snr_min_lofar = self.thresh_lofar['snr_min']
            dm_min_lofar = dm_min
            width_max_lofar = width_max
        else:
            # new source, apply all LOFAR thresholds
            snr_min_lofar = self.thresh_lofar['snr_min']
            dm_min_lofar = max(dmgal * self.thresh_lofar['dm_frac_min'], self.dm_min_global)
            width_max_lofar = self.thresh_lofar['width_max']
        # create mask for given thresholds
        mask = (cluster_snr >= snr_min_lofar) & (cluster_dm >= dm_min_lofar) & (cluster_downsamp <= width_max_lofar)

        # check for any remaining triggers
        if np.any(mask):
            ncluster = np.sum(mask)

            self.logger.info("Found {} possible LOFAR trigger(s)".format(ncluster))
            # note: the server keeps track of when LOFAR triggers were sent
            # and whether or not a new trigger can be sent
            # check if there are multiple triggers
            if ncluster > 1:
                self.logger.info("Multiple triggers - selecting trigger with highest S/N")
            # argmax also works if there is one trigger, so just run it always
            ind = np.argmax(cluster_snr[mask])
            # estimate flux density based on peak S/N and width
            snr = cluster_snr[mask][ind]
            width = TSAMP.to(u.ms) * cluster_downsamp[mask][ind]
            # astropy units only knows mJy, but the VOEvent Generator expects Jy
            flux = util.get_flux(snr, width).to(u.mJy).value / 1000.
            # select known source DM if available
            if dm_src is not None:
                dm_to_send = dm_src
                dm_err = 0.
            else:
                dm_to_send = cluster_dm[mask][ind]
                # set DM uncertainty to DM delay across pulse width
                # Apertif has roughly 1 DM unit = 1 ms delay across band
                dm_err = width.to(u.ms).value
            # set a source name
            if src_type is not None:
                name = src_type
            else:
                name = 'candidate'
            # check whether or not pointing information is available
            if pointing is None:
                self.logger.error("No pointing information available - cannot trigger LOFAR")
            # check if we are connected to the server
            elif not self.have_vo:
                self.logger.error("No VO Generator connection available - cannot trigger LOFAR")
            # do the trigger
            else:
                # create the full trigger and put on VO queue
                lofar_trigger = {'dm': dm_to_send,
                                 'dm_err': dm_err,
                                 'width': width.to(u.ms).value,  # ms
                                 'snr': snr,
                                 'flux': flux,  # Jy
                                 'ra': pointing.ra.deg,  # decimal deg
                                 'dec': pointing.dec.deg,  # decimal deg
                                 'ymw16': dmgal,
                                 'semiMaj': 15,  # arcmin, CB
                                 'semiMin': 15,  # arcmin, CB
                                 'name': name,
                                 'utc': (utc_start + TimeDelta(cluster_time[mask][ind], format='sec')).isot,
                                 'importance': 0.1}
                # add system parameters (dt, central freq (GHz), bandwidth (MHz))
                lofar_trigger.update(sys_params)
                self.logger.info("Sending LOFAR trigger")
                self.vo_queue.put(lofar_trigger)

    def _process_triggers(self):
        """
        Read thresholds (DM, width, S/N) for clustering
        Continuously read AMBER triggers from queue and start processing for known and/or new sources
        """

        # set observation parameters
        utc_start = Time(self.obs_config['startpacket'] / 781250., format='unix')
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq']*u.MHz + 0.5*BANDWIDTH).to(u.GHz).value
        sys_params = {'dt': dt, 'delta_nu_MHz': chan_width, 'nu_GHz': cent_freq}
        pointing = self._get_pointing()
        if pointing is not None:
            dmgal = self._get_ymw16(pointing)
        else:
            dmgal = 0

        # get known source dm and type
        dm_src, src_type = self._get_source()
        if src_type is not None:
            thresh_src = {'dm_src': dm_src,
                          'src_type': src_type,
                          'dm_min': max(dm_src - self.dm_range, self.dm_min_global),
                          'dm_max': dm_src + self.dm_range,
                          'width_max': np.inf,
                          'snr_min': self.snr_min_global,
                          'pointing': pointing,
                          }
            # Add LOFAR thresholds
            self.logger.info("Setting known source trigger DM range to {dm_min} - {dm_max}, "
                             "max downsamp={width_max}, min S/N={snr_min}".format(**thresh_src))

        # set min and max DM for new sources with unknown DM
        thresh_new = {'src_type': None,
                      'dm_min': max(dmgal * self.thresh_iquv['dm_frac_min'], self.dm_min_global),
                      'dm_max': np.inf,
                      'width_max': self.thresh_iquv['width_max'],
                      'snr_min': self.thresh_iquv['snr_min'],
                      'pointing': pointing,
                      }
        self.logger.info("Setting new source trigger DM range to {dm_min} - {dm_max}, "
                        "max downasmp={width_max}, min S/N={snr_min}".format(**thresh_new))

        # main loop
        while self.observation_running:
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                triggers = self.amber_triggers

                self.amber_triggers = []
                # check for header (always, because it is received once for every amber instance)
                if not self.hdr_mapping:
                    for trigger in triggers:
                        if trigger.startswith('#'):
                            # read header, remove comment symbol
                            header = trigger.split()[1:]
                            self.logger.info("Received header: {}".format(header))
                            # Check if all required params are present and create mapping to col index
                            keys = ['beam_id', 'integration_step', 'time', 'DM', 'SNR']
                            for key in keys:
                                try:
                                    self.hdr_mapping[key] = header.index(key)
                                except ValueError:
                                    self.logger.error("Key missing from clusters header: {}".format(key))
                                    self.hdr_mapping = {}
                                    return

                # header should be present now
                if not self.hdr_mapping:
                    self.logger.error("First clusters received but header not found")
                    continue

                # remove headers from triggers (i.e. any trigger starting with #)
                triggers = [trigger for trigger in triggers if not trigger.startswith('#')]

                # triggers is empty if only header was received
                if not triggers:
                    self.logger.info("Only header received - Canceling processing")
                    continue

                # split strings and convert to numpy array
                try:
                    triggers = np.array(list(map(lambda val: val.split(), triggers)), dtype=float)
                except Exception as e:
                    self.logger.error("Failed to process triggers: {}".format(e))
                    continue

                #
                # pick columns to feed to clustering algorithm
                triggers_for_clustering = triggers[:, (self.hdr_mapping['DM'], self.hdr_mapping['SNR'],
                                                       self.hdr_mapping['time'], self.hdr_mapping['integration_step'],
                                                       self.hdr_mapping['beam_id'])]

                # known source and new source triggering, in thread so clustering itself does not
                # delay next run
                # known source triggering
                if src_type is not None:
                    self.threads['trigger_known_source'] = threading.Thread(target=self._check_triggers,
                                                                            args=(triggers_for_clustering, sys_params,
                                                                                  utc_start),
                                                                            kwargs=thresh_src)
                    self.threads['trigger_known_source'].daemon = True
                    self.threads['trigger_known_source'].start()
                # new source triggering
                self.threads['trigger_new_source'] = threading.Thread(target=self._check_triggers,
                                                                      args=(triggers_for_clustering, sys_params,
                                                                            utc_start),
                                                                      kwargs=thresh_new)
                self.threads['trigger_new_source'].daemon = True
                self.threads['trigger_new_source'].start()

            sleep(self.interval)
        self.logger.info("Observation finished")

    def _get_pointing(self):
        """
        Get pointing of this CB from parset
        :return: pointing SkyCoord
        """
        # read parset
        try:
            parset = self.obs_config['parset']
        except KeyError as e:
            self.logger.error("Cannot read parset ({})".format(e))
            return None
        # read beam
        try:
            beam = self.obs_config['beam']
        except KeyError as e:
            self.logger.error("Cannot read beam from parset, setting CB to 0 ({})".format(e))
            beam = 0
        # read beam coordinates from parset
        try:
            key = "task.beamSet.0.compoundBeam.{}.phaseCenter".format(beam)
            c1, c2 = ast.literal_eval(parset[key].replace('deg', ''))
        except Exception as e:
            self.logger.error("Could not parse pointing for CB{:02d} ({})".format(beam, e))
            return None
        # convert HA to RA if HADEC is used
        if parset['task.directionReferenceFrame'].upper() == 'HADEC':
            # RA = LST - HA. Get RA at the start of the observation
            start_time = Time(parset['task.startTime'])
            # set delta UT1 UTC to zero to avoid requiring up-to-date IERS table
            start_time.delta_ut1_utc = 0
            lst_start = start_time.sidereal_time('mean', WSRT_LON).to(u.deg)
            c1 = lst_start.to(u.deg).value - c1
        # create SkyCoord object
        pointing = SkyCoord(c1, c2, unit=(u.deg, u.deg))
        return pointing

    def _get_ymw16(self, pointing):
        """
        Get YMW16 DM from pointing
        :param pointing: pointing SkyCoord
        :return: YMW16 DM
        """
        # ymw16 arguments: mode, Gl, Gb, dist(pc), 2=dist->DM. 1E6 pc should cover entire MW
        gl, gb = pointing.galactic.to_string(precision=8).split(' ')
        cmd = ['ymw16', 'Gal', gl, gb, '1E6', '2']
        # run ymw16 command
        try:
            result = subprocess.check_output(cmd)
        except OSError as e:
            self.logger.error("Failed to run ymw16, setting YMW16 DM to zero: {}".format(e))
            return 0
        # parse output
        try:
            dm = float(result.split()[7])
        except Exception as e:
            self.logger.error('Failed to parse DM from YMW16 output {}, setting YMW16 DM to zero: {}'.format(result, e))
            return 0
        return dm

    def _load_parset(self, obs_config):
        """
        Load the observation parset
        :param obs_config: Observation config
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
