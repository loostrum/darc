#!/usr/bin/env python
#
# AMBER Clustering

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

        self.proc_thread = None
        self.hdr_mapping = {}
        self.obs_config = None
        self.observation_running = False
        self.amber_triggers = []

        # store when we are allowed to do IQUV / LOFAR triggering
        self.time_iquv = Time.now()

        # connect to VOEvent generator
        if connect_vo:
            self.vo_queue = self.voevent_connector()
        else:
            self.vo_queue = mp.Queue()

    def process_command(self, command):
        """
        Process command received from queue
        :return:
        """
        if command['command'] == 'trigger':
            if not self.observation_running:
                self.logger.error("Trigger received but no observation is running - ignoring")
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
        # set config
        self.obs_config = obs_config

        self.observation_running = True

        # process triggers in thread
        self.proc_thread = threading.Thread(target=self.process_triggers)
        self.proc_thread.daemon = True
        self.proc_thread.start()

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
        # clear the processing thead
        if self.proc_thread:
            self.proc_thread.join()
            self.proc_thread = None

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

    def process_triggers(self):
        """
        Applies thresholding to clusters
        Puts approved clusters on queue
        """

        # set observation parameters
        utc_start = Time(self.obs_config['startpacket'] / 781250., format='unix')
        dt = TSAMP.to(u.second).value
        chan_width = (BANDWIDTH / float(NCHAN)).to(u.MHz).value
        cent_freq = (self.obs_config['min_freq']*u.MHz + 0.5*BANDWIDTH).to(u.GHz).value
        dmgal = self._get_ymw16(self.obs_config)
        dm_min_iquv = dmgal * self.thresh_iquv['dm_frac_min']
        dm_min_lofar = dmgal * self.thresh_lofar['dm_frac_min']

        while self.observation_running:
            if self.amber_triggers:
                # Copy the triggers so class-wide list can receive new triggers without those getting lost
                triggers = self.amber_triggers

                self.amber_triggers = []
                self.logger.info("Starting processing of {} AMBER triggers".format(len(triggers)))
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
                                                       self.hdr_mapping['time'], self.hdr_mapping['integration_step'])]
                triggers_for_clustering_sb = triggers[:, self.hdr_mapping['beam_id']].astype(int)
                self.logger.info("Clustering")
                # cluster using IQUV thresholds
                # LOFAR thresholds are assumed to be more strict for every parameter
                cluster_snr, cluster_dm, cluster_time, cluster_downsamp, cluster_sb, _ = \
                    tools.get_triggers(triggers_for_clustering,
                                       tab=triggers[:, self.hdr_mapping['beam_id']],
                                       dm_min=dm_min_iquv, dm_max=np.inf,
                                       sig_thresh=self.thresh_iquv['snr_min'],
                                       dt=dt, delta_nu_MHz=chan_width, nu_GHz=cent_freq,
                                       sb=triggers_for_clustering_sb)
                self.logger.info("Clustered {} raw triggers into {} clusters".format(len(triggers_for_clustering),
                                                                                     len(cluster_snr)))

                ncluster = len(cluster_snr)
                if ncluster > 0:
                    self.logger.info("Found {} trigger(s) for IQUV triggering".format(ncluster))
                    # check if we can do triggering
                    now = Time.now()
                    if now < self.time_iquv:
                        self.logger.warning("Cannot trigger IQUV yet, next possible time: {}".format(self.time_iquv))
                    else:
                        self.logger.info("Can trigger IQUV - proceeding")
                        # update last trigger time
                        self.time_iquv = now + TimeDelta(self.thresh_iquv['interval'], format='sec')
                        # trigger IQUV
                        dada_triggers = []
                        for i in range(ncluster):
                            # set window size to roughly two DM delays, and at least one page
                            dada_trigger = {'stokes': 'IQUV', 'dm': cluster_dm[i], 'beam': cluster_sb[i],
                                            'width': cluster_downsamp[i], 'snr': cluster_snr[i],
                                            'time': cluster_time[i], 'utc_start': utc_start}
                            dada_triggers.append(dada_trigger)
                        self.target_queue.put({'command': 'trigger', 'trigger': dada_triggers})

                    # Check for LOFAR triggers
                    mask = (cluster_dm >= dm_min_lofar) & (cluster_snr >= self.thresh_lofar['snr_min'])
                    if np.any(mask):
                        num = np.sum(mask)
                        self.logger.info("Found {} trigger(s) for LOFAR".format(num))
                        # note: the server keeps track of when LOFAR triggers were sent
                        # and whether or not a new trigger can be sent
                        # check if there are multiple triggers
                        if num > 1:
                            self.logger.info("Multiple triggers - selecting trigger with highest S/N")
                        # argmax also works if there is one trigger, so run it always
                        ind = np.argmax(cluster_snr[mask])
                        lofar_trigger = {'dm': cluster_dm[mask][ind],
                                         'dm_err': 0,
                                         'width': 0,  # ms
                                         'snr': cluster_snr[mask][ind],
                                         'flux': 0,  # mJy
                                         'ra': 0,  # decimal deg
                                         'dec': 0,  # decimal deg
                                         'ymw16': dmgal,
                                         'semiMaj': 0,  # arcmin
                                         'semiMin': 0,  # arcmin
                                         'name': 0,
                                         'utc': (utc_start + TimeDelta(cluster_time[mask][i], format='sec')).isot,
                                         'importance': 0.1,
                                         'test': True}  # just testing
                        self.vo_queue.put(lofar_trigger)

            sleep(self.interval)
        self.logger.info("Observation finished")

    def _get_ymw16(self, obs_config):
        """
        Get YMW16 DM
        :param obs_config: Observation config
        :return: YMW16 DM
        """
        # get pointing
        parset = obs_config['parset']
        if self.host_type == 'master':
            beam = 0
        else:
            beam = obs_config['beam']
        try:
            key = "task.beamSet.0.compoundBeam.{}.phaseCenter".format(beam)
            c1, c2 = ast.literal_eval(parset[key].replace('deg', ''))
        except Exception as e:
            self.logger.error("Could not parse pointing for CB{:02d}, setting YMW16 DM to zero ({})".format(beam, e))
            return 0
        # convert HA to RA if HADEC is used
        if parset['task.directionReferenceFrame'].upper() == 'HADEC':
            # RA = LST - HA. Get RA at the start of the observation
            start_time = Time(parset['task.startTime'])
            # set delta UT1 UTC to zero to avoid requiring up-to-date IERS table
            start_time.delta_ut1_utc = 0
            lst_start = start_time.sidereal_time('mean', WSRT_LON).to(u.deg)
            c1 = lst_start.to(u.deg).value - c1
        pointing = SkyCoord(c1, c2, unit=(u.deg, u.deg))

        # ymw16 arguments: mode, Gl, Gb, dist(pc), 2=dist->DM. 1E6 pc should cover entire MW
        gl, gb = pointing.galactic.to_string(precision=8).split(' ')
        cmd = ['ymw16', 'Gal', gl, gb, '1E6', '2']
        result = subprocess.check_output(cmd)
        try:
            dm = float(result.split()[7])
        except Exception as e:
            self.logger.error('Failed to parse DM from YMW16 output {}, setting YMW16 DM to zero: {}'.format(result, e))
            return 0
        return dm
