#!/usr/bin/env python
#
# DARC master process
# Controls all services

import os
import ast
import glob
import yaml
import codecs
import multiprocessing as mp
from multiprocessing import queues
from queue import Empty
import threading
import socket
import subprocess
from shutil import copyfile, which
import random
import string

import h5py
import numpy as np
from astropy.time import Time, TimeDelta
from astropy.coordinates import SkyCoord
import astropy.units as u

from darc.definitions import ROOT_DIR, CONFIG_FILE, WSRT_LON, NUMCB
from darc.logger import get_logger
from darc import util


class OfflineProcessingException(Exception):
    pass


class OfflineProcessing(threading.Thread):
    """
    Full offline processing pipeline:

    #. Candidate clustering
    #. Extraction of filterbank data
    #. ML classification
    #. Email to astronomers

    Also includes:

    - Automated pulsar folding
    - Automated run of calibration tools for drift scans
    """
    def __init__(self):
        """
        """
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = threading.Event()

        self.observation_queue = None
        self.threads = {}

        with open(os.path.join(ROOT_DIR, CONFIG_FILE), 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['offline_processing']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        for key, value in config.items():
            if isinstance(value, str):
                value = value.format(**kwargs)
                config[key] = value
            setattr(self, key, value)

        # set TAB or SB for hdf5 keys
        if self.process_sb:
            config['keys_data'].append('sb')
        else:
            config['keys_data'].append('tab')
        self.config = config

        # setup logger
        self.logger = get_logger(__name__, self.log_file)

        self.logger.info('OfflineProcessing initialized')

    def set_source_queue(self, queue):
        """
        Set input queue

        :param queues.Queue queue: Source of start_observation commands
        """
        if not isinstance(queue, queues.Queue):
            self.logger.error('Given source queue is not an instance of Queue')
            raise OfflineProcessingException('Given source queue is not an instance of Queue')
        self.observation_queue = queue

    def stop(self):
        """
        Stop this service
        """
        self.stop_event.set()

    def run(self):
        """
        Wait for start_observation command, then run offline processing pipeline.
        Executed commands depend on whether this is a worker node or the master node.
        """
        if not self.observation_queue:
            self.logger.error('Observation queue not set')
            raise OfflineProcessingException('Observation queue not set')
        self.logger.info("Starting Offline Processing")
        data = None
        while not self.stop_event.is_set():
            try:
                data = self.observation_queue.get(timeout=1)
            except Empty:
                continue

            # load observation config
            host_type = data['host_type']
            self.host_type = host_type
            obs_config = data['obs_config']
            # add general config to obs config
            obs_config.update(self.config)
            # format result dir
            obs_config['result_dir'] = os.path.join(self.result_dir, obs_config['date'], obs_config['datetimesource'])
            # get parset
            obs_config['parset'] = self._load_parset(obs_config)
            # get taskid
            try:
                taskid = obs_config['parset']['task.taskID']
            except Exception as e:
                self.logger.warning("Cannot find task ID in parset - using random number thread")
                # taskid length is 8
                taskid = ''.join([random.choice(string.ascii_lowercase) for i in range(8)])

            # start observation corresponding to host type
            if host_type == 'master':
                thread = threading.Thread(target=self._start_observation_master, args=[obs_config], name=taskid)
                thread.daemon = True
                self.threads[taskid] = thread
                thread.start()
            elif host_type == 'worker':
                thread = threading.Thread(target=self._start_observation_worker, args=[obs_config], name=taskid)
                thread.daemon = True
                self.threads[taskid] = thread
                thread.start()
            else:
                self.logger.error("Unknown host type: {}".format(self.host_type))

        self.logger.info("Stopping Offline processing")

    def _start_observation_master(self, obs_config):
        """
        Start observation on master node

        Generated observation summary and send email once all workers are done

        :param dict obs_config: Observation config
        """
        self.logger.info("Starting observation on master node")
        # create result dir
        try:
            util.makedirs(obs_config['result_dir'])
        except Exception as e:
            self.logger.error("Failed to create results directory")
            raise OfflineProcessingException("Failed to create result directory: {}".format(e))

        # create general info file
        self._get_overview(obs_config)

        # create coordinates file
        self._get_coordinates(obs_config)

        # wait until end time + 10s
        start_processing_time = Time(obs_config['parset']['task.stopTime']) + TimeDelta(10, format='sec')
        self.logger.info("Sleeping until {}".format(start_processing_time.iso))
        util.sleepuntil_utc(start_processing_time, event=self.stop_event)

        cmd = "python2 {emailer} {result_dir} '{beams}' {ntabs}".format(**obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)
        self.logger.info("Finished processing of observation {output_dir}".format(**obs_config))

    def _start_observation_worker(self, obs_config):
        """
        Start observation on worker node:

        #. Candidate clustering
        #. Extraction of filterbank data
        #. ML classification

        :param dict obs_config: Observation config
        """
        self.logger.info("Starting observation on worker node")
        # create result dir
        try:
            util.makedirs(obs_config['result_dir'])
        except Exception as e:
            self.logger.error("Failed to create results directory")
            raise OfflineProcessingException("Failed to create result directory: {}".format(e))

        # TAB or IAB mode
        if obs_config['ntabs'] == 1:
            obs_config['mode'] = 'IAB'
            trigger_output_file = "{output_dir}/triggers/data/data_00_full.hdf5".format(**obs_config)
        else:
            obs_config['mode'] = 'TAB'
            trigger_output_file = "{output_dir}/triggers/data/data_full.hdf5".format(**obs_config)

        # wait until end time + 10s
        start_processing_time = Time(obs_config['parset']['task.stopTime']) + TimeDelta(10, format='sec')
        self.logger.info("Sleeping until {}".format(start_processing_time.iso))
        util.sleepuntil_utc(start_processing_time, event=self.stop_event)

        # fold pulsar if this is beam 0 and a test pulsar is being observed
        try:
            source = obs_config['parset']['task.source.name']
            ref_beam = int(obs_config['parset']['task.source.beam'])
            # Requires access to parset
            if source in self.test_pulsars and (obs_config['beam'] == ref_beam):
                self.logger.info("Test pulsar detected: {}".format(source))
                self._fold_pulsar(source, obs_config)
        except Exception as e:
            self.logger.error("Pulsar folding failed: {}".format(e))
            
        # run calibration tools if this is a calibrator scan
        # these have "drift" in the source name
        try:
            source = obs_config['parset']['task.source.name']
            if 'drift' in source:
                # split into actual source name and which beams were scanned
                name, beam_range = source.split('drift')
                # parse beam range, can be one beam or start/end beam
                if len(beam_range) == 2:
                    # one beam
                    drift_beams = [int(beam_range)]
                elif len(beam_range) == 4:
                    # start and end beam
                    sbeam = int(beam_range[:2])
                    ebeam = int(beam_range[2:])
                    drift_beams = range(sbeam, ebeam+1)
                else:
                    self.logger.error("Failed to parse beam range for calibrator scan: {}".format(source))
                    drift_beams = []

                # run calibration tools if this is a calibrator scan of this beam
                if name in self.calibrators and (obs_config['beam'] in drift_beams):
                    self.logger.info("Calibrator scan through this beam detected: {}".format(source))
                    self._run_calibration_tools(name, obs_config)
        except Exception as e:
            self.logger.error("Calibration tools failed: {}".format(e))

        # create trigger directory
        trigger_dir = "{output_dir}/triggers".format(**obs_config)
        try:
            util.makedirs(trigger_dir)
        except Exception as e:
            self.logger.error("Failed to create triggers directory")
            raise OfflineProcessingException("Failed to create triggers directory: {}".format(e))
        # change to trigger directory
        try:
            os.chdir(trigger_dir)
        except Exception as e:
            self.logger.error("Failed to cd to triggers directory {}: {}".format(trigger_dir, e))
        # create subdir
        try:
            util.makedirs('data')
        except Exception as e:
            self.logger.error("Failed to create triggers/data directory")
            raise OfflineProcessingException("Failed to create triggers/data directory: {}".format(e))

        # merge the trigger files
        self.logger.info("Merging raw trigger files")
        numcand_raw = self._merge_triggers(obs_config)

        # Run clustering for each SB in TAB mode if enabled
        if self.process_sb and obs_config['mode'] == 'TAB':
            self.logger.info("Clustering candidates in SB mode")
            tstart = Time.now()
            # spread the SBs over the allowed number of threads
            # output grouped pulses file will only be generated by thread including SB00
            chunks = np.array_split(range(obs_config['nsynbeams']), self.numthread)
            numcand_all = np.zeros(self.numthread)
            filterbank_prefix = "{output_dir}/filterbank/CB{beam:02d}".format(**obs_config)
            threads = []
            self.logger.info("Starting trigger clustering with {} threads".format(self.numthread))
            for ind, chunk in enumerate(chunks):
                # pick the SB range
                sbmin, sbmax = min(chunk), max(chunk)
                # create thread
                thread = threading.Thread(target=self._cluster, args=[obs_config, filterbank_prefix],
                                          kwargs={'out': numcand_all, 'sbmin': sbmin, 'sbmax': sbmax,
                                                  'ind': ind})
                thread.daemon = True
                threads.append(thread)
                thread.start()
            # wait until all are done
            for thread in threads:
                thread.join()
            # gather results
            if self.process_sb:
                # each element equal
                numcand_grouped = int(numcand_all[0])
            else:
                # each element is one TAB
                numcand_grouped = int(np.sum(numcand_all[numcand_all != -1]))

        # Run clustering for IAB / each TAB
        else:
            self.logger.info("Clustering candidates in {} mode".format(obs_config['mode']))
            tstart = Time.now()
            if obs_config['mode'] == 'IAB':
                filterbank_file = "{output_dir}/filterbank/CB{beam:02d}.fil".format(**obs_config)
                numcand_grouped = self._cluster(obs_config, 0, filterbank_file)
            elif obs_config['mode'] == 'TAB':
                numcand_all = np.zeros(obs_config['ntabs'])
                # max numtread tabs per run; so ntabs / numthread chunks
                n_chunk = int(np.ceil(obs_config['ntabs'] / float(self.numthread)))
                chunks = np.array_split(range(obs_config['ntabs']), n_chunk)
                self.logger.info("Starting trigger clustering with {} chunks of {} threads".format(n_chunk, self.numthread))
                # start the threads
                for tab_set in chunks:
                    threads = []
                    for tab in tab_set:
                        filterbank_file = "{output_dir}/filterbank/CB{beam:02d}_{tab:02d}.fil".format(tab=tab, **obs_config)
                        thread = threading.Thread(target=self._cluster, args=[obs_config, filterbank_file],
                                                  kwargs={'out': numcand_all, 'tab': tab})
                        thread.daemon = True
                        threads.append(thread)
                        thread.start()
                    # wait until all are done
                    for thread in threads:
                        thread.join()
                # gather results
                numcand_grouped = int(np.sum(numcand_all[numcand_all != -1]))
        tend = Time.now()
        self.logger.info("Trigger clustering took {}s".format((tend-tstart).sec))

        # Create one hdf5 file for entire CB
        if obs_config['mode'] == 'TAB':
            self.logger.info("Merging output HDF5 files")
            numcand_merged = self._merge_hdf5(obs_config, trigger_output_file)
        # TEMP so that IAB still works
        else:
            numcand_merged = 9999

        # Run classifier
        if numcand_merged != 0:
            self.logger.info("Classifying candidates")
            output_prefix = self._classify(obs_config, trigger_output_file)
        else:
            self.logger.info("No candidates post-merge. Not running classifier")
            output_prefix = ''
        
        # Merge PDFs
        if numcand_merged > 0:
            self.logger.info("Merging classifier output files")
            self._merge_plots(obs_config)
        else:
            self.logger.info("No candidates found post-classifier, not creating merged PDF")

        # Centralize results
        self.logger.info("Gathering results")

        kwargs = {'output_prefix': output_prefix, 'data_file': trigger_output_file, 'numcand_raw': numcand_raw, 
                  'numcand_grouped': numcand_grouped}
        self._gather_results(obs_config, **kwargs)

        self.logger.info("Finished processing of observation {output_dir}".format(**obs_config))

        return

    def _merge_triggers(self, obs_config):
        """
        Merge AMBER triggers into one file

        :param dict obs_config: Observation config
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
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        # check number of triggers
        cmd = "grep -v \# {prefix}.trigger | wc -l".format(prefix=prefix)
        self.logger.info("Running {}".format(cmd))
        try:
            numcand_raw = int(subprocess.check_output(cmd, shell=True)) - 1
        except Exception as e:
            self.logger.warning("Failed to get number of raw triggers, setting to -1: {}".format(e))
            numcand_raw = -1

        return numcand_raw

    def _cluster(self, obs_config, filterbank_name, tab=None, ind=None, sbmin=None, sbmax=None, out=None):
        """
        Run triggers.py, which takes care of clustering and extracting filterbank data

        :param dict obs_config: Observation config
        :param str filterbank_name: Full path to filterbank file (TAB/IAB) or prefix (SB) to use
        :param int tab: TAB number to process (0 for IAB, absent/None for SB)
        :param int ind: Index of out array where to store results
        :param int sbmin: First SB to process (SB mode only)
        :param int sbmax: Last SB to process (SB mode only)
        :param np.ndarray out: output array where return value is put at index <ind> (optional)
        :return: number of grouped candidates (put in out array, else returned)
        """

        # in TAB processing mode, out index is TAB number
        if ind is None:
            ind = tab

        # Set minimum DM
        obs_config['dmmin'] = self.dmmin

        prefix = "{amber_dir}/CB{beam:02d}".format(**obs_config)
        time_limit = self.max_proc_time / self.numthread
        if self.process_sb:
            cmd = "nice python2 {triggering} --rficlean --sig_thresh_local {snrmin_processing_local} " \
                  "--time_limit {time_limit} --descending_snr " \
                  "--beamno {beam:02d} --dm_min {dmmin} --dm_max {dmmax} --sig_thresh {snrmin_processing} " \
                  "--ndm {ndm} --save_data concat --nfreq_plot {nfreq_plot} --ntime_plot {ntime_plot} " \
                  "--cmap {cmap} --outdir={output_dir}/triggers --clean_type {clean_type} " \
                  "--synthesized_beams --sbmin {sbmin} --sbmax {sbmax} --central_freq {freq} " \
                  "{filterbank_prefix} {prefix}.trigger".format(filterbank_prefix=filterbank_name, sbmin=sbmin,
                                                                sbmax=sbmax, prefix=prefix,
                                                                time_limit=time_limit, **obs_config)
        else:
            cmd = "nice python2 {triggering} --rficlean --sig_thresh_local {snrmin_processing_local} " \
                  "--time_limit {time_limit} --descending_snr " \
                  "--beamno {beam:02d} --dm_min {dmmin} --dm_max {dmmax} --sig_thresh {snrmin_processing} " \
                  "--ndm {ndm} --save_data concat --nfreq_plot {nfreq_plot} --ntime_plot {ntime_plot} " \
                  "--cmap {cmap} --outdir={output_dir}/triggers --central_freq {freq} " \
                  "--tab {tab} {filterbank_file} {prefix}.trigger".format(tab=tab, filterbank_file=filterbank_name,
                                                                          prefix=prefix,
                                                                          time_limit=time_limit, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        # Check number of candidates
        if obs_config['mode'] == 'IAB':
            fname = '{output_dir}/triggers/grouped_pulses.singlepulse'.format(**obs_config)
        else:
            if self.process_sb:
                fname = '{output_dir}/triggers/grouped_pulses_synthesized_beams.singlepulse'.format(**obs_config)
            else:
                fname = '{output_dir}/triggers/grouped_pulses_{tab:02d}.singlepulse'.format(tab=tab, **obs_config)

        cmd = "wc -l {fname} | awk '{{print $1}}'".format(fname=fname)
        self.logger.info("Running {}".format(cmd))
        try:
            numcand_grouped = int(subprocess.check_output(cmd, shell=True))
        except Exception as e:
            self.logger.warning("Failed to get number of grouped triggers, setting to -1: {}".format(e))
            numcand_grouped = -1

        if out is not None:
            out[ind] = numcand_grouped
        else:
            return numcand_grouped

    def _merge_hdf5(self, obs_config, output_file):
        """
        Merge HDF5 files generated by clustering

        :param dict obs_config: Observation config
        :param str output_file: Filename of merged HDF5 data
        :return: Number of triggers in combined HDF5 file
        """

        # define the data set keys
        keys_data = self.config['keys_data']
        key_ntrigger_skipped = self.config['key_ntriggers_skipped']
        keys_all = keys_data + [key_ntrigger_skipped]

        ntrigger = 0

        # initialize the output data
        out_data = {key_ntrigger_skipped: 0}
        for key in keys_data:
            out_data[key] = []

        # extend the datasets by the other TABs
        if self.process_sb:
            nitem = self.numthread
            data_files = glob.glob('{output_dir}/triggers/data/data_sb??_??_full.hdf5'.format(**obs_config))
        else:
            nitem = obs_config['ntabs']

        for item in range(nitem):
            if self.process_sb:
                try:
                    data_file = data_files[item]
                except IndexError:
                    self.logger.error("Data file {} out of {} not found, skipping".format(item+1, nitem))
                    continue
            else:
                data_file = '{output_dir}/triggers/data/data_{tab:02d}_full.hdf5'.format(tab=item, **obs_config)
            try:
                with h5py.File(data_file) as h5:
                    keys = h5.keys()

                    # check number of skipped triggers
                    key = 'ntriggers_skipped'
                    if key not in keys:
                        self.logger.error("Error: key {} not found, skipping HDF5 file {}".format(key, data_file))
                        break
                    val = h5[key][:]
                    out_data[key] += val

                    # check number of clustered triggers
                    key = 'data_freq_time'
                    if key not in keys:
                        self.logger.error("Error: key {} not found, skipping HDF5 file {}".format(key, data_file))
                        break

                    # check if there are any triggers
                    val = len(h5[key][:])
                    ntrigger += val
                    if val == 0:
                        self.logger.info("No triggers found in HDF5 file {}".format(data_file))
                        continue

                    # load the datasets
                    for key in keys_data:
                        if key not in keys:
                            self.logger.error("Warning: key {} not found, skipping HDF5 file {}".format(key, data_file))
                            continue
                        out_data[key] += list(h5[key][:])

            except IOError as e:
                self.logger.error("Failed to load HDF5 file {}: {}".format(data_file, e))
                continue

        # create the output data file
        try:
            with h5py.File(output_file, 'w') as out:
                for key in keys_all:
                    out.create_dataset(key, data=out_data[key])
        except IOError as e:
            self.logger.error("Failed to create output HDF5 file {}: {}".format(output_file, e))
        return ntrigger

    def _classify(self, obs_config, input_file):
        """
        Run the ML classifier

        :param dict obs_config: Observation config
        :param str input_file: HDF5 file to process
        :return: prefix of output figure path
        """

        output_prefix = "{output_dir}/triggers/ranked_CB{beam:02d}".format(**obs_config)

        # Synthesized beams
        if self.process_sb:
            sb_option = '--synthesized_beams'
        else:
            sb_option = ''

        # Galactic DM
        dmgal = self._get_ymw16(obs_config)

        # Add optional classifier models (1D time and DM-time)
        model_option = ''
        if self.model_dmtime:
            model_option += ' --fn_model_dm {model_dir}/{model_dmtime}'.format(**obs_config)
        if self.model_1dtime:
            model_option += ' --fn_model_time {model_dir}/{model_1dtime}'.format(**obs_config)

        cmd = "export CUDA_VISIBLE_DEVICES={ml_gpus}; python {classifier} " \
              " {model_option} {sb_option} " \
              " --pthresh {pthresh_freqtime} --save_ranked --plot_ranked --fnout={output_prefix} {input_file} " \
              " --pthresh_dm {pthresh_dmtime} --DMgal {dmgal} " \
              " {model_dir}/20190416freq_time.hdf5".format(output_prefix=output_prefix, sb_option=sb_option,
                                                           model_option=model_option, dmgal=dmgal,
                                                           input_file=input_file, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)
        # ToDo: count number of output figures
        return output_prefix

    def _merge_plots(self, obs_config):
        """
        Merge classifier output plots into one pdf

        :param dict obs_config: Observation config
        """
        output_file = "{output_dir}/triggers/candidates_summary.pdf".format(**obs_config)
        cmd = "gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile={output_file} {output_dir}/triggers/*pdf".format(output_file=output_file, **obs_config)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

        return

    def _gather_results(self, obs_config, **kwargs):
        """
        Gather output results and put in central location

        :param dict obs_config: Observation config
        :param dict kwargs: number of candidates, output prefixes; these are added to obs_config
        """

        # Copy results
        conf = obs_config.copy()
        conf.update(**kwargs)

        # Read number of skipped triggers and tab
        if self.process_sb:
            beam = 'SB'
        else:
            beam = 'TAB'
        self.logger.info("Reading number of skipped triggers and {} column".format(beam))
        try:
            # read dataset
            with h5py.File(kwargs.get('data_file'), 'r') as f:
                try:
                    ncand_skipped = int(f['ntriggers_skipped'][0])
                except ValueError as e:
                    self.logger.warning("Could not read ntriggers_skipped from {}: {}".format(kwargs.get('data_file'), e))
                    ncand_skipped = -1
                try:
                    beam_data = f[beam.lower()][:]
                except Exception as e:
                    self.logger.warning("Could not read {} column from {}: {}".format(beam, kwargs.get('data_file'), e))
                    beam_data = None
        except IOError as e:
            # success = False
            self.logger.warning("Could not get ncand_skipped from {}: {}".format(kwargs.get('data_file'), e))
            ncand_skipped = -1

        # read classifier output (only the first file!)
        self.logger.info("Reading classifier output file")
        try:
            fname_classifier = glob.glob("{output_prefix}_*freq_time*.hdf5".format(**conf))[0]
        except IndexError:
            self.logger.info("No classifier output file found")
            ncand_classifier = 0
        else:
            try:
                # read dataset
                with h5py.File(fname_classifier, 'r') as f:
                    frb_index = f['frb_index'][:]
                    data_frb_candidate = f['data_frb_candidate'][:]
                    probability = f['probability'][:][frb_index]
                    params = f['params'][:][frb_index]  # snr, DM, downsampling, arrival time, dt
                    if self.process_sb:
                        beam_data = f['sb'][:]
                    elif beam_data is not None:
                        beam_data = beam_data[frb_index]
            except Exception as e:
                self.logger.warning("Could not read classifier output file: {}".format(e))
                ncand_classifier = 0
            # Process output
            else:
                self.logger.info("Saving trigger metadata")
                # convert widths to ms
                params[:, 2] *= params[:, 4] * 1000
                # number of candidates
                ncand_classifier = len(params)
                # make one big matrix with candidates, removing the dt column
                if beam_data is not None:
                    data = np.column_stack([params[:, :4], probability, beam_data])
                else:
                    data = np.column_stack([params[:, :4], probability])
                # sort by probability
                data = data[data[:, -1].argsort()[::-1]]
                # save to file in master output directory
                fname = "{result_dir}/CB{beam:02d}_triggers.txt".format(**conf)
                if beam_data is not None:
                    header = "SNR DM Width T0 p {}".format(beam)
                    np.savetxt(fname, data, header=header, fmt="%.2f %.2f %.4f %.3f %.2f %.0f")
                else:
                    header = "SNR DM Width T0 p"
                    np.savetxt(fname, data, header=header, fmt="%.2f %.2f %.4f %.3f %.2f")

        # copy candidates file if it exists
        fname = "{output_dir}/triggers/candidates_summary.pdf".format(**conf)
        if os.path.isfile(fname):
            self.logger.info("Saving candidate pdf")
            copyfile(fname, "{result_dir}/CB{beam:02d}_candidates_summary.pdf".format(**conf))
        else:
            self.logger.info("No candidate pdf found")

        # create summary file
        self.logger.info("Creating summary file")
        summary = {}
        summary['ncand_raw'] = kwargs.get('numcand_raw', -1)
        summary['ncand_trigger'] = kwargs.get('numcand_grouped', -1)
        summary['ncand_skipped'] = ncand_skipped
        summary['ncand_abovethresh'] = kwargs.get('numcand_grouped', ncand_skipped-1) - ncand_skipped
        summary['ncand_classifier'] = ncand_classifier
        fname = "{result_dir}/CB{beam:02d}_summary.yaml".format(**conf)
        with open(fname, 'w') as f:
            yaml.dump(summary, f, default_flow_style=False)

        return

    def _get_coordinates(self, obs_config):
        """
        Generate coordinates file from the pointing directions

        File contains RA, Dec, gl, gb for each CB used in this observation

        :param dict obs_config: Observation config
        """
        parset = obs_config['parset']
        # check the reference frame
        ref_frame = parset['task.directionReferenceFrame'].upper()
        if ref_frame == 'J2000':
            self.logger.info("Detected J2000 reference frame")
            mode = 'RA'
        elif ref_frame == 'HADEC':
            self.logger.info("Detected HADEC reference frame")
            mode = 'HA'
        else:
            self.logger.error("Unknown reference frame: {}. Assuming J2000 RA,Dec".format(ref_frame))
            mode = 'RA'

        # get the CB pointings
        coordinates = {}
        for cb in range(NUMCB):
            try:
                key = "task.beamSet.0.compoundBeam.{}.phaseCenter".format(cb)
                c1, c2 = ast.literal_eval(parset[key].replace('deg', ''))
                if mode == 'HA':
                    # RA = LST - HA. Get RA at the start of the observation
                    start_time = Time(parset['task.startTime'])
                    # set delta UT1 UTC to zero to avoid requiring up-to-date IERS table
                    start_time.delta_ut1_utc = 0
                    lst_start = start_time.sidereal_time('mean', WSRT_LON).to(u.deg)
                    c1 = lst_start.to(u.deg).value - c1
                pointing = SkyCoord(c1, c2, unit=(u.deg, u.deg))
            except Exception as e:
                self.logger.error("Failed to get pointing for CB{}: {}".format(cb, e))
                coordinates[cb] = [0, 0, 0, 0]
            else:
                # get pretty strings
                ra = pointing.ra.to_string(unit=u.hourangle, sep=':', pad=True, precision=1)
                dec = pointing.dec.to_string(unit=u.deg, sep=':', pad=True, precision=1)
                gl, gb = pointing.galactic.to_string(precision=8).split(' ')
                coordinates[cb] = [ra, dec, gl, gb]

        # save to result dir
        with open(os.path.join(obs_config['result_dir'], 'coordinates.txt'), 'w') as f:
            for cb in range(NUMCB):
                coord = coordinates[cb]
                line = "{:02d} {} {} {} {}\n".format(cb, *coord)
                f.write(line)

    def _get_overview(self, obs_config):
        """
        Generate observation overview file

        :param dict obs_config: Observation config
        """
        # For now replicate old command
        parset = obs_config['parset']
        info_file = os.path.join(obs_config['result_dir'], 'info.yaml')
        info = {}
        info['utc_start'] = parset['task.startTime']
        info['tobs'] = parset['task.duration']
        info['source'] = parset['task.source.name']
        info['ymw16'] = "{:.2f}".format(self._get_ymw16(obs_config))
        info['telescopes'] = parset['task.telescopes'].replace('[', '').replace(']', '')
        info['taskid'] = parset['task.taskID']
        with open(info_file, 'w') as f:
            yaml.dump(info, f, default_flow_style=False)

    def _get_ymw16(self, obs_config):
        """
        Get YMW16 DM

        :param dict obs_config: Observation config
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
        try:
            result = subprocess.check_output(cmd)
        except OSError as e:
            self.logger.error("Failed to run ymw16, setting YMW16 DM to zero: {}".format(e))
            return 0
        try:
            dm = float(result.split()[7])
        except Exception as e:
            self.logger.error('Failed to parse DM from YMW16 output {}, setting YMW16 DM to zero: {}'.format(result, e))
            return 0
        return dm

    def _fold_pulsar(self, source, obs_config):
        """
        Fold pulsar with PRESTO

        :param str source: pulsar name including B or J
        :param dict obs_config: Observation config
        """

        if obs_config['mode'] == 'IAB':
            fname = "{output_dir}/filterbank/CB{beam:02d}.fil".format(**obs_config)
        else:
            fname = "{output_dir}/filterbank/CB{beam:02d}_00.fil".format(**obs_config)

        # try to find par file, otherwise use psr option
        parfile = "{}/tzpar/{}.par".format(os.environ['TEMPO'], source[1:])
        if not os.path.isfile(parfile):
            opt = "-psr {}".format(source)
        else:
            opt = "-par {}".format(parfile)

        # run in filterbank dir
        os.chdir("{output_dir}/filterbank".format(**obs_config))

        # prepfold command
        prepfold_cmd = "prepfold -n 64 -nsub 128 -nodmsearch -nopdsearch {opt}" \
                       " -noxwin -filterbank" \
                       " {fname}".format(opt=opt, fname=fname)

        # convert to pdf command
        figname = fname.replace('.fil', '_PSR_{}.pfd.ps'.format(source[1:]))
        convert_cmd = "ps2pdf {}".format(figname)

        # Create and run full command
        full_cmd = prepfold_cmd + " && " + convert_cmd + ' &'
        self.logger.info("Running {}".format(full_cmd))
        os.system(full_cmd)

    def _run_calibration_tools(self, source, obs_config):
        """
        :param str source: Source name (like 3C296, *not* 3C286drift0107)

        :param dict obs_config: Observation config
        """
        # init settings for calibration tools script
        kwargs = {'source': source, 'calibration_tools': self.calibration_tools}

        # get number of dishes
        # stored as string in parset, using a comma-separated format
        dishes = obs_config['parset']['task.telescopes']
        kwargs['ndish'] = dishes.count(',') + 1

        # create output directories
        prefix = '{}_{}'.format(obs_config['date'], source)
        output_dir = os.path.join(self.calibration_dir, prefix)
        for sub_dir in ['plots', 'data']:
            util.makedirs(os.path.join(output_dir, sub_dir))
        kwargs['output_dir'] = output_dir

        # find full path to cp, to avoid using user alias like "cp -i"
        kwargs['cp'] = which('cp')

        # run in filterbank dir
        os.chdir("{output_dir}/filterbank".format(**obs_config))

        # define command (python2!)
        cmd = "(nice python2 {calibration_tools} --Ndish {ndish} --src {source} CB??_??.fil;" \
              " {cp} *.pdf {output_dir}/plots; {cp} *.npy {output_dir}/data) &".format(**kwargs)
        self.logger.info("Running {}".format(cmd))
        os.system(cmd)

    def _load_parset(self, obs_config):
        """
        Load the observation parset

        :param dict obs_config: Observation config
        :return: parset as dict
        """
        if self.host_type == 'master':
            # encoded parset is already in config on master node
            # decode the parset
            raw_parset = util.decode_parset(obs_config['parset'])
            # convert to dict and store
            parset = util.parse_parset(raw_parset)
        else:
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
