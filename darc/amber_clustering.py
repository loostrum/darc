#!/usr/bin/env python3
#
# AMBER Clustering

import os
from time import sleep
import yaml
import ast
import threading
import multiprocessing as mp
import numpy as np
from astropy.time import Time, TimeDelta
import astropy.units as u
from astropy.coordinates import SkyCoord


from darc import DARCBase, VOEventQueueServer, LOFARTriggerQueueServer
from darc.definitions import TSAMP, NCHAN, BANDWIDTH, MASTER, TIME_UNIT
from darc.external import tools
from darc import util


class AMBERClusteringException(Exception):
    pass


class AMBERClustering(DARCBase):
    """
    Trigger IQUV / LOFAR / VOEvent system based on AMBER candidates

    1. Cluster incoming triggers
    2. Apply thresholds (separate for known and new sources, and for IQUV vs LOFAR)
    3. Put IQUV triggers on output queue
    4. Put LOFAR triggers on remote LOFAR trigger queue and on VOEvent queue
    """

    def __init__(self, *args, connect_vo=True, connect_lofar=True, **kwargs):
        """
        :param bool connect_vo: Whether or not to connect to VOEvent queue on master node
        :param bool connect_lofar: Whether or not to connect to LOFAR trigger queue on master node
        """
        super(AMBERClustering, self).__init__(*args, **kwargs)
        self.connect_vo = connect_vo
        self.connect_lofar = connect_lofar
        self.dummy_queue = mp.Queue()

        self.threads = {}
        self.hdr_mapping = {}
        self.obs_config = None
        self.observation_running = False
        self.amber_triggers = []
        self.source_list = None
        self.lock = mp.Lock()

        # store when we are allowed to do IQUV / LOFAR triggering
        self.time_iquv = Time.now()

        # connect to VOEvent generator
        if self.connect_vo:
            try:
                self.vo_queue = self.voevent_connector()
                self.logger.info("Connected to VOEvent Generator on master node")
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

        # connect to LOFAR trigger
        if self.connect_lofar:
            try:
                self.lofar_queue = self.lofar_connector()
                self.logger.info("Connected to LOFAR Trigger on master node")
                self.have_lofar = True
            except Exception as e:
                self.logger.error("Failed to connect to LOFAR Trigger, setting dummy queue ({})".format(e))
                self.lofar_queue = self.dummy_queue
                self.have_lofar = False
        else:
            # dummy queue
            self.logger.info("LOFAR Trigger connection disabled, setting dummy queue")
            self.lofar_queue = mp.Queue()
            self.have_lofar = False

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

        :param dict command: Command to process
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger(s) received but no observation is running - ignoring")
            else:
                with self.lock:
                    self.amber_triggers.append(command['trigger'])
        elif command['command'] == 'get_attr':
            self.get_attribute(command)
        else:
            self.logger.error("Unknown command received: {}".format(command['command']))

    def start_observation(self, obs_config, reload=True):
        """
        Parse obs config and start listening for amber triggers on queue

        :param dict obs_config: Observation configuration
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

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
                self.logger.info("Connected to VOEvent Generator on master node")
                self.have_vo = True
            except Exception as e:
                self.logger.error("Failed to connect to VOEvent Generator, setting dummy queue ({})".format(e))
                self.vo_queue = self.dummy_queue
                self.have_vo = False

        # try connecting to LOFAR trigger serverr if enabled
        # always do this in case a connection was available before, but failed at some point
        if self.connect_lofar:
            try:
                self.lofar_queue = self.lofar_connector()
                self.logger.info("Connected to LOFAR Trigger on master node")
                self.have_lofar = True
            except Exception as e:
                self.logger.error("Failed to connect to LOFAR Trigger, setting dummy queue ({})".format(e))
                self.lofar_queue = self.dummy_queue
                self.have_lofar = False

        # process triggers in thread
        self.threads['processing'] = threading.Thread(target=self._process_triggers)
        self.threads['processing'].start()

        self.logger.info("Observation started")

    def stop_observation(self, *args, **kwargs):
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

    def voevent_connector(self):
        """
        Connect to the VOEvent generator on the master node
        """
        # Load VO server settings
        VOEventQueueServer.register('get_queue')
        with open(self.config_file, 'r') as f:
            server_config = yaml.load(f, Loader=yaml.SafeLoader)['voevent_generator']
        port = server_config['server_port']
        key = server_config['server_auth'].encode()
        server = VOEventQueueServer(address=(MASTER, port), authkey=key)
        server.connect()
        return server.get_queue()

    def lofar_connector(self):
        """
        Connect to the LOFAR triggering system on the master node
        """
        # Load LOFAR trigger server settings
        LOFARTriggerQueueServer.register('get_queue')
        with open(self.config_file, 'r') as f:
            server_config = yaml.load(f, Loader=yaml.SafeLoader)['lofar_trigger']
        port = server_config['server_port']
        key = server_config['server_auth'].encode()
        server = LOFARTriggerQueueServer(address=(MASTER, port), authkey=key)
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
            return None, None, None

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

        return dm_src, src_type, source

    def _check_triggers(self, triggers, sys_params, utc_start, datetimesource, dm_min=0, dm_max=np.inf, dm_src=None,
                        width_max=np.inf, snr_min=8, src_type=None, src_name=None, dmgal=0, pointing=None,
                        skip_lofar=False):
        """
        Cluster triggers and run IQUV and/or LOFAR triggering

        :param list triggers: Raw triggers
        :param dict sys_params: System parameters (dt, delta_nu_MHz, nu_GHz)
        :param str utc_start: start time of observation, in format readable by astropy.time.Time
        :param str datetimesource: Field name with date and time
        :param float dm_min: minimum DM (default: 0)
        :param float dm_max: maximum DM (default: inf)
        :param float dm_src: DM of known source (default: None)
        :param float width_max: maximum width (default: inf)
        :param float snr_min: mininum S/N (default: 8)
        :param str src_type: Source type (pulsar, frb, None)
        :param str src_name: Source name (default: None)
        :param float dmgal: galactic maximum DM
        :param astropy.coordinates.SkyCoord pointing: Pointing for LOFAR triggering (default: None)
        :param bool skip_lofar: Skip LOFAR triggering (default: False)

        """
        # cluster using IQUV thresholds
        # LOFAR thresholds are assumed to be more strict for every parameter
        cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _, ncand_per_cluster = \
            tools.get_triggers(triggers,
                               dm_min=dm_min, dm_max=dm_max, sig_thresh=snr_min, t_window=self.clustering_window,
                               read_beam=True, return_clustcounts=True, sb_filter=self.sb_filter,
                               sb_filter_period_min=self.sb_filter_period_min,
                               sb_filter_period_max=self.sb_filter_period_max,
                               **sys_params)

        # select on width
        mask = np.array(cluster_downsamp) <= width_max
        cluster_snr = np.array(cluster_snr)[mask]
        cluster_dm = np.array(cluster_dm)[mask]
        cluster_time = np.array(cluster_time)[mask]
        cluster_downsamp = np.array(cluster_downsamp)[mask].astype(int)
        cluster_sb = np.array(cluster_sb)[mask].astype(int)
        ncand_per_cluster = np.array(ncand_per_cluster)[mask].astype(int)
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

        # skip LOFAR triggering for pulsars or if explicitly disabled
        if src_type == 'pulsar' or skip_lofar:
            return

        # select LOFAR thresholds
        if src_type is not None:
            # known source, use same DM threshold as IQUV, but apply width and S/N thresholds
            # DM_min effectively does nothing here because the value is the same as for IQUV
            # but it needs to be defined for the mask = line below to work
            # no limit on candidates per cluster
            snr_min_lofar = self.thresh_lofar['snr_min']
            dm_min_lofar = dm_min
            width_max_lofar = self.thresh_lofar['width_max']
            max_cands_per_cluster = np.inf

            # Overrides for specific sources
            if src_name in self.lofar_trigger_sources:
                # check CB number
                try:
                    allowed_cbs = self.thresh_lofar_override['cb']
                    if isinstance(allowed_cbs, float):
                        allowed_cbs = [allowed_cbs]
                    if self.obs_config['beam'] not in allowed_cbs:
                        return
                except KeyError:
                    # any CB is valid if cb key is not present
                    pass
                else:
                    # source known, CB valid: set thresholds
                    snr_min_lofar = self.thresh_lofar_override['snr_min']
                    width_max_lofar = self.thresh_lofar_override['width_max']
                    self.logger.warning("Setting LOFAR trigger thresholds: S/N > {}, downsamp <= {}".format(snr_min_lofar,
                                                                                                            width_max_lofar))
        else:
            # new source, apply all LOFAR thresholds
            snr_min_lofar = self.thresh_lofar['snr_min']
            dm_min_lofar = max(dmgal * self.thresh_lofar['dm_frac_min'], self.dm_min_global)
            width_max_lofar = self.thresh_lofar['width_max']
            max_cands_per_cluster = self.thresh_lofar['max_cands_per_cluster']

        # create mask for given thresholds
        # also remove triggers where number of raw candidates is too high (this indicates RFI)
        mask = (cluster_snr >= snr_min_lofar) & (cluster_dm >= dm_min_lofar) & \
               (cluster_downsamp <= width_max_lofar) & \
               (ncand_per_cluster <= max_cands_per_cluster)

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
            # calculate arrival time at reference frequency = central frequency
            cent_freq = sys_params['nu_GHz'] * 1000.
            max_freq = cent_freq + .5 * BANDWIDTH.to(u.MHz).value
            dm_delay = 4.148808E3 * dm_to_send * (cent_freq**-2 - max_freq**-2)
            utc_arr = (utc_start + TimeDelta(cluster_time[mask][ind] - dm_delay, format='sec')).isot
            # set a source name
            if src_type is not None:
                name = src_type
            else:
                name = 'candidate'
            # check whether or not pointing information is available
            if pointing is None:
                self.logger.error("No pointing information available - cannot trigger LOFAR")
            # check if we are connected to the server
            elif not self.have_lofar:
                self.logger.error("No LOFAR Trigger connection available - cannot trigger LOFAR")
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
                                 'cb': self.obs_config['beam'],
                                 'sb': cluster_sb[mask][ind],
                                 'ymw16': dmgal,
                                 'semiMaj': 15,  # arcmin, CB
                                 'semiMin': 15,  # arcmin, CB
                                 'name': name,
                                 'src_name': src_name,
                                 'datetimesource': datetimesource,
                                 'utc': utc_arr,
                                 'tarr': cluster_time[mask][ind],
                                 'importance': 0.1}
                # add system parameters (dt, central freq (GHz), bandwidth (MHz))
                lofar_trigger.update(sys_params)
                self.logger.info("Sending LOFAR trigger to LOFAR Trigger system")
                self.lofar_queue.put(lofar_trigger)
                if self.have_vo:
                    self.logger.info("Sending same trigger to VOEvent system")
                    self.vo_queue.put(lofar_trigger)
                else:
                    self.logger.error("No VOEvent Generator connection available - not sending VO trigger")

    def _process_triggers(self):
        """
        Read thresholds (DM, width, S/N) for clustering

        Continuously read AMBER triggers from queue and start processing for known and/or new sources
        """

        # set observation parameters
        utc_start = Time(self.obs_config['startpacket'] / TIME_UNIT, format='unix')
        datetimesource = self.obs_config['datetimesource']
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq'] * u.MHz + 0.5 * BANDWIDTH).to(u.GHz).value
        sys_params = {'dt': dt, 'delta_nu_MHz': chan_width, 'nu_GHz': cent_freq}
        pointing = self._get_pointing()
        dmgal = util.get_ymw16(self.obs_config['parset'], self.obs_config['beam'], self.logger)

        # get known source dm and type
        dm_src, src_type, src_name = self._get_source()
        if src_type is not None:
            thresh_src = {'dm_src': dm_src,
                          'src_type': src_type,
                          'src_name': src_name,
                          'dm_min': max(dm_src - self.dm_range, self.dm_min_global),
                          'dm_max': dm_src + self.dm_range,
                          'width_max': np.inf,
                          'snr_min': self.snr_min_global,
                          'pointing': pointing,
                          'dmgal': dmgal
                          }
            self.logger.info("Setting {src_name} trigger DM range to {dm_min} - {dm_max}, "
                             "max downsamp={width_max}, min S/N={snr_min}".format(**thresh_src))

        # set min and max DM for new sources with unknown DM
        thresh_new = {'src_type': None,
                      'src_name': None,
                      'dm_min': max(dmgal * self.thresh_iquv['dm_frac_min'], self.dm_min_global),
                      'dm_max': np.inf,
                      'width_max': self.thresh_iquv['width_max'],
                      'snr_min': self.thresh_iquv['snr_min'],
                      'pointing': pointing,
                      'dmgal': dmgal
                      }
        # if known source, check whether or not LOFAR triggering should be enabled for new sources
        if src_type is not None and src_name in self.lofar_trigger_sources:
            thresh_new['skip_lofar'] = not self.thresh_lofar['trigger_on_new_sources']
        else:
            thresh_new['skip_lofar'] = False

        self.logger.info("Setting new source trigger DM range to {dm_min} - {dm_max}, "
                         "max downsamp={width_max}, min S/N={snr_min}, skip LOFAR "
                         "triggering={skip_lofar}".format(**thresh_new))

        # main loop
        while self.observation_running:
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                with self.lock:
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
                                                                                  utc_start, datetimesource),
                                                                            kwargs=thresh_src)
                    self.threads['trigger_known_source'].start()
                # new source triggering
                self.threads['trigger_new_source'] = threading.Thread(target=self._check_triggers,
                                                                      args=(triggers_for_clustering, sys_params,
                                                                            utc_start, datetimesource),
                                                                      kwargs=thresh_new)
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
            c1 = c1 * u.deg
            c2 = c2 * u.deg
        except Exception as e:
            self.logger.error("Could not parse pointing for CB{:02d} ({})".format(beam, e))
            return None
        # convert HA to RA if HADEC is used
        if parset['task.directionReferenceFrame'].upper() == 'HADEC':
            # Get RA at the mid point of the observation
            timestamp = Time(parset['task.startTime']) + .5 * parset['task.duration'] * u.s
            c1, c2 = util.radec_to_hadec(c1, c2, timestamp)
        # create SkyCoord object
        pointing = SkyCoord(c1, c2)
        return pointing

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
