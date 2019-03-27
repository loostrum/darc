#!/usr/bin/env python
#
# DARC master process
# Controls all services

import os
import sys
import ast
import yaml
import errno
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
import threading
import socket
import subprocess
from time import time

from astropy.time import Time, TimeDelta

from darc.definitions import *
from darc.logger import get_logger
from darc.control import send_command
from darc import util


class OfflineProcessingException(Exception):
    pass

class OfflineProcessing(threading.Thread):
    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

        self.observation_queue = None
        self.threads = []

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['offline_processing']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
                config[key] = value
            setattr(self, key, value)
        self.config = config

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        self.logger.info('OfflineProcessing initialized')

    def set_source_queue(self, queue):
        """ 
        :param queue: Source of start_observation commands
        """
        if not isinstance(queue, mp.queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise OfflineProcessingException('Given source queue is not an instance of Queue')
        self.observation_queue = queue

    def run(self):
        """
        Wait for start_observation command
        """
        if not self.observation_queue:
            self.logger.error('Observation queue not set')
            raise AMBERTriggeringException('Observation queue not set')

        self.logger.info("Starting Offline Processing")
        data = None
        while not self.stop_event.is_set():
            try:
                data = self.observation_queue.get(timeout=1)
            except Empty:
                continue

            # start observation in thread
            host_type = data['host_type']
            obs_config  = data['obs_config']
            
            if host_type == 'master':
                thread = threading.Thread(target=self._start_observation_master, args=[obs_config])
                thread.daemon = True
                self.threads.append(thread)
                thread.start()
            elif host_type == 'worker':
                thread = threading.Thread(target=self._start_observation_worker, args=[obs_config])
                thread.daemon = True
                self.threads.append(thread)
                thread.start()
            else:
                self.logger.error("Unknown host type: {}".format(self.host_type))

        self.logger.info("Stopping Offline processing")

    def _start_observation_master(self, obs_config):
        """
        Start observation on master node
        """
        self.logger.info("Starting observation on master node")
        # Add general config to obs config
        obs_config.update(self.config)

        email_script = '{home}/ARTS-obs/emailer.py'.format(home=os.path.expanduser('~'))
        cmd = "(sleepuntil_utc {endtime}; python {email_script} {master_dir} '{beams}' {ntabs}) &".format(email_script=email_script, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

    def _start_observation_worker(self, obs_config):
        """
        Start observation on worker node
        """
        self.logger.info("Starting observation on worker node")
        # Add general config to obs config
        obs_config.update(self.config)

        # TAB or IAB mode
        if obs_config['ntabs'] == 1:
            obs_config['mode'] = 'IAB'
        else:
            obs_config['mode'] = 'TAB'

        # wait until end time + 10s
        start_processing_time = Time(obs_config['endtime']) + TimeDelta(10, format='sec')
        self.logger.info("Sleeping until {}".format(start_processing_time))
        util.sleepuntil_utc(start_processing_time, event=self.stop_event)

        # merge the trigger files
        self.logger.info("Merging raw trigger files")
        numcand_raw = self._merge_triggers(obs_config)

        # Run clustering for IAB / each TAB
        self.logger.info("Clustering candidates")
        numcand_grouped = 0
        # ToDo run in parallel
        for tab in range(obs_config['ntabs']):
            if obs_config['mode'] == 'IAB':
                filterbank_file = "{output_dir}/filterbank/CB{beam:02d}.fil".format(**obs_config)
            else:
                filterbank_file = "{output_dir}/filterbank/CB{beam:02d}_{tab:02d}.fil".format(tab=tab+1, **obs_config)
            numcand_grouped += self._cluster(obs_config, tab, filterbank_file)

        # Run classifier
        self.logger.info("Classifying candidates")
        for tab in range(obs_config['ntabs']-1, -1, -1):
            output_prefix, data_file = self._classify(obs_config, tab)
        
        # Gather results
        self.logger.info("Gathering results")
        kwargs = {'output_prefix': output_prefix, 'data_file': data_file, 'numcand_raw': numcand_raw, 'numcand_grouped': numcand_grouped}
        self._gather_results(obs_config, **kwargs)

        return

    def _merge_triggers(self, obs_config):
        """
        Merge AMBER triggers into one file
        """
        prefix = "{amber_dir}/CB{beam:02d}".format(**obs_config)
        cmd = "awk '(FNR==1 && NR!=1) || !/./{{next;}}{{print}}' {prefix}_step*.trigger > {prefix}.trigger".format(prefix=prefix)
        # awk command from Thijs:
        # (FNR==1 && NR!=1) means "if it's the first line of the current file we're processing, 
        # but not the first line read overall then skip this line {"next"}. This matches the first 
        # header line in all files expect the first file. The other clause "|| !/./"  makes it skip
        # empty lines, which can happen when there is superfluous carriage returns at the end of files.
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        # check number of triggers
        cmd = "grep -v \# {prefix}.trigger | wc -l".format(prefix=prefix)
        try:
            numcand_raw = int(subprocess.check_output(cmd)) - 1
        except Exception as e:
            self.logger.warning("Failed to get number of raw triggers, setting to -1: {}".format(e))
            numcand_raw = -1

        return numcand_raw

    def _cluster(self, obs_config, tab, filterbank_file):
        """
        Run triggers.py
        :param: tab: TAB number to process (0 for IAB)
        :params: filterbank_file: Full path to filterbank file to use
        """

        prefix = "{amber_dir}/CB{beam:02d}".format(**obs_config)
        cmd = "python {triggering} --rficlean --sig_thresh_local {snrmin_processing_local} --time_limit {duration} --descending_snr " \
              " --beamno {beam:02d} --mk_plot --dm_min {dmmin} --dm_max {dmmax} --sig_thresh {snrmin_processing} --ndm {ndm} " \
              " --save_data concat --nfreq_plot {nfreq_plot} --ntime_plot {ntime_plot} --cmap {cmap} --outdir={output_dir}/triggers " \
              " --tab {tab} {filterbank_file} {prefix}.trigger".format(tab=tab, filterbank_file=filterbank_file, prefix=prefix, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        # Check number of candidates
        if obs_config['mode'] == 'IAB':
            fname = '{output_dir}/triggers/grouped_pulses.singlepulse'.format(**obs_config)
        else:
            fname = '{output_dir}/triggers/grouped_pulses_{tab:02d}.singlepulse'.format(tab=tab+1, **obs_config)

        cmd = "wc -l {fname} | awk '{{print $1}}')".format(fname=fname)
        try:
            numcand_grouped = int(subprocess.check_output(cmd))
        except Exception as e:
            self.logger.warning("Failed to get number of grouped triggers, setting to -1: {}".format(e))
            numcand_grouped = -1

        return numcand_grouped


    def _classify(self, obs_config, tab):
        """
        Run the ML classifier in python 3 virtualenv
        :param:  tab: TAB to process
        """
        if obs_config['mode'] == 'IAB':
            output_prefix = "{output_dir}/triggers/ranked_CB{beam:02d}".format(**obs_config)
            data_file =  "{output_dir}/triggers/data/data_full.hdf5".format(**obs_config)
        else:
            output_prefix = "{output_dir}/triggers/ranked_CB{beam:02d}_TAB{tab:02d}".format(tab=tab, **obs_config)
            data_file =  '{output_dir}/triggers/data/data_{tab:02d}_full.hdf5'.format(tab=tab+1, **obs_config)

        cmd = "source {venv_dir}/bin/activate; export CUDA_VISIBLE_DEVICES={ml_gpus}; python {classifier} " \
              " --fn_model_time {model_dir}/heimdall_b0329_mix_147411d_time.hdf5 " \
              " --pthresh {pthresh} --save_ranked --plot_ranked --fnout={output_prefix} {data_file} " \
              " {model_dir}/20190125-17114-freqtimefreq_time_model.hdf5".format(output_prefix=output_prefix, data_file=data_file, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)
        #ToDo: count number of output figures
        return output_prefix, data_file

    def _gather_results(self, obs_config, **kwargs):
        """
        Merge classifier output plots and call trigger_to_master
        :param: kwargs: number of candidate, output prefixes
        """

        # Merge output figures
        output_file = "{output_dir}/triggers/candidates_summary.pdf".format(**obs_config)
        cmd = "gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile={output_file} {output_dir}/triggers/*pdf".format(output_file=output_file, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        # Copy results
        # ToDo: properly get results of all TABs
        conf = obs_config.copy()
        conf.update(**kwargs)
        cmd = "python {trigger_to_master} {data_file} {output_prefix}_freq_time.hdf5 {numcand_raw} {numcand_grouped} {master_dir}".format(**conf)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        return