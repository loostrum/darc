#!usr/bin/env python3

import os
import logging
import socket
import threading
from textwrap import dedent
import ast
import yaml
import multiprocessing as mp
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time, TimeDelta
import numpy as np

from darc import DARCBase
from darc import util
from darc.control import send_command
from darc.definitions import WORKERS, TSAMP, TIME_UNIT
from darc.logger import get_queue_logger, get_queue_logger_listener


class ProcessorMasterManager(DARCBase):
    """
    Control logic for running several ProcessorMaster instances, one per observation
    """

    def __init__(self, *args, **kwargs):
        """
        """
        # init DARCBase without logger, as we need a non-default logger
        super(ProcessorMasterManager, self).__init__(*args, no_logger=True, **kwargs)

        # initialize queue logger listener
        self.log_queue = mp.Queue()
        self.log_listener = get_queue_logger_listener(self.log_queue, self.log_file)
        self.log_listener.start()

        # create queue logger
        self.logger = get_queue_logger(self.module_name, self.log_queue)

        self.observations = {}
        self.observation_end_times = {}
        self.observation_queues = {}

        self.scavenger = None
        self.status_generator = None

        # reduce logging from status check commands
        logging.getLogger('darc.control').setLevel(logging.ERROR)

        self.logger.info("{} initialized".format(self.log_name))

    def run(self):
        """
        Main loop. Create thread scavenger, then run parent class run method
        """
        # create a thread scavenger
        self.scavenger = threading.Thread(target=self.thread_scavenger, name='scavenger')
        self.scavenger.start()
        # create a status generator for the processing website
        self.status_generator = threading.Thread(target=self.processing_status_generator, name='status_generator')
        self.status_generator.start()
        super(ProcessorMasterManager, self).run()

    def thread_scavenger(self):
        """
        Remove any finished threads at regular intervals
        """
        self.logger.info("Starting thread scavenger")
        while not self.stop_event.is_set():
            for taskid, thread in self.observations.copy().items():
                if not thread.is_alive():
                    # if the thread is dead, remove it from the list
                    self.logger.info(f"Scavenging thread of taskid {taskid}")
                    self.observations.pop(taskid)
                    self.observation_queues.pop(taskid)

            self.stop_event.wait(self.scavenger_interval)

    def processing_status_generator(self):
        """
        At regular interval, create status file for processing website
        """
        self.logger.info("Starting processing status file generator")
        # create the output directory if it does not exist
        util.makedirs(self.processing_status_path)
        hostname = socket.gethostname()
        out_file = os.path.join(self.processing_status_path, f"{hostname}.js")
        while not self.stop_event.is_set():
            # get list of taskids that are being processed
            taskids = sorted(self.observations.keys())
            times = []
            if not taskids:
                # nothing is running
                status = "idle"
            else:
                status = "running"
                now = Time.now()
                for taskid in taskids:
                    # check elapsed time
                    processing_time = now - self.observation_end_times[taskid]
                    # if negative, the observation is still running
                    if processing_time.sec < 0:
                        times.append('observing')
                    else:
                        # format as hh:mm:ss
                        full_min, seconds = divmod(processing_time.sec, 60)
                        hours, minutes = divmod(full_min, 60)
                        times.append(f"{hours:02.0f}h{minutes:02.0f}m{seconds:02.0f}s")

            content = dedent(f"""
                              var {hostname} = {{
                                  "node_name": "{hostname}",
                                  "node_status": "{status}",
                                  "node_process": "{','.join(taskids)}",
                                  "time": "{','.join(times)}"
                              }};
                              """)
            with open(out_file, 'w') as f:
                f.write(content)
            self.stop_event.wait(self.processing_status_generator_interval)

        # upon exit, create file to indicate node is offline
        content = dedent(f"""
                          var {hostname} = {{
                              "node_name": "{hostname}",
                              "node_status": "offline",
                              "node_process": "",
                              "time": ""
                          }};
                          """)
        with open(out_file, 'w') as f:
            f.write(content)
            f.flush()

    def stop(self, abort=False):
        """
        Stop this service

        :param bool abort: Ignored; a stop of the manager always equals an abort
        """
        self.logger.info("Stopping {}".format(self.log_name))
        # Abort any existing observations
        # loop over dictionary items. Use copy to avoid changing dict in loop
        for taskid, obs in self.observations.copy().items():
            if obs.is_alive():
                self.logger.info(f"Aborting observation with taskid {taskid}")
                self.observation_queues[taskid].put('abort')
            obs.join()
        # stop the log listener
        self.log_listener.stop()
        # stop the manager
        self.stop_event.set()
        # wait for subprocesses to exit
        if self.scavenger is not None:
            self.scavenger.join()
        if self.status_generator is not None:
            self.status_generator.join()

    def start_observation(self, obs_config, reload=True):
        """
        Initialize a ProcessorMaster and call its start_observation
        """
        if reload:
            self.load_config()

        # add parset to obs config
        obs_config['parset'] = self._load_parset(obs_config)
        # get task ID
        taskid = obs_config['parset']['task.taskID']

        self.logger.info(f"Starting observation with task ID {taskid}")

        # refuse to do anything if an observation with this task ID already exists
        if taskid in self.observations.keys():
            self.logger.error(f"Failed to start observation: task ID {taskid} already exists")
            return

        # initialize a Processor for this observation
        queue = mp.Queue()
        proc = ProcessorMaster(source_queue=queue, log_queue=self.log_queue, config_file=self.config_file)
        proc.name = taskid
        proc.start()
        # start the observation and store thread
        queue.put({'command': 'start_observation', 'obs_config': obs_config, 'reload': reload})
        self.observations[taskid] = proc
        self.observation_queues[taskid] = queue
        self.observation_end_times[taskid] = Time(obs_config['startpacket'] / TIME_UNIT, format='unix') + \
            TimeDelta(obs_config['duration'], format='sec')
        return

    def stop_observation(self, obs_config):
        """
        Stop observation with task ID as given in parset

        :param dict obs_config: Observation config
        """
        # load the parset
        parset = self._load_parset(obs_config)
        # get task ID
        taskid = parset['task.taskID']
        # check if an observation with this task ID exists
        if taskid not in self.observations.keys():
            self.logger.error(f"Failed to stop observation: no such task ID {taskid}")
            return

        # signal the processor of this observation to stop the observation
        # when processing is finished, this also stops the Process
        self.observation_queues[taskid].put({'command': 'stop_observation'})

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
                    "Failed to load parset from master config file {}, "
                    "setting parset to None: {}".format(master_config_file, e))
                parset = None

        return parset


class ProcessorMaster(DARCBase):
    """
    Combine results from worker node processors
    """
    def __init__(self, log_queue, *args, **kwargs):
        """
        :param Queue log_queue: Queue to use for logging
        """
        # init DARCBase without logger, as we need a non-default logger
        super(ProcessorMaster, self).__init__(*args, no_logger=True, **kwargs)

        # create queue logger
        self.logger = get_queue_logger(self.module_name, log_queue)

        # read result dir from worker processor config
        self.result_dir = self._get_result_dir()

        self.obs_config = None
        self.warnings_sent = []
        self.status = None
        self.process = None
        self.central_result_dir = None

        self.logger.info("{} initialized".format(self.log_name))

    def start_observation(self, obs_config, reload=True):
        """
        Parse obs config and start observation processing after end time has passed

        :param dict obs_config: Observation configuration
        :param bool reload: reload service settings (default: True)
        """
        # reload config
        if reload:
            self.load_config()

        # add observation-specific path to result_dir
        self.central_result_dir = os.path.join(self.result_dir, obs_config['date'], obs_config['datetimesource'])
        util.makedirs(self.central_result_dir)

        self.obs_config = obs_config

        # process the observation in a separate thread (not a process, as then we can't stop it directly
        # through the stop event of ProcessorMaster; and no other processing happens anyway)
        self.process = threading.Thread(target=self._process_observation)
        self.process.start()

    def _process_observation(self):
        """
        Process observation
        """

        # wait until the observation finishes
        start_processing_time = Time(self.obs_config['parset']['task.stopTime'])
        self.logger.info("Sleeping until {}".format(start_processing_time.iso))
        self.status = 'Observation in progress'
        util.sleepuntil_utc(start_processing_time, event=self.stop_event)
        if self.stop_event.is_set():
            return

        try:
            # generate observation info files
            self.status = 'Generating observation info files'
            info, coordinates = self._generate_info_file()
            if self.stop_event.is_set():
                return

            # wait for all result files to be present
            self.status = 'Waiting for nodes to finish processing'
            self._wait_for_workers()
            if self.stop_event.is_set():
                return

            # combine results, copy to website and generate email
            self.status = 'Combining node results'
            email, attachments = self._process_results(info, coordinates)
            if self.stop_event.is_set():
                return

            # publish results on web link and send email
            self.status = 'Sending results to website'
            self._publish_results(email, attachments)
            self.status = 'Sending results to email'
            self._send_email(email, attachments)
            self.status = 'Done'
        except Exception as e:
            self.logger.error(f"Failed to process observation. Status = {self.status}: {type(e)}: {e}")
        else:
            self.logger.info(f"Finished processing observation: {self.obs_config['parset']['task.taskID']}: "
                             f"{self.obs_config['datetimesource']}")
        # stop this processor instance
        self.stop_event.set()

    def stop_observation(self, abort=False):
        """
        Stop observation

        :param bool abort: Whether or not to abort the observation
        """
        # nothing to stop unless we are aborting
        if abort and self.process is not None:
            # terminate the processing by setting the stop event (this will also stop the ProcessorMaster)
            self.stop_event.set()
            self.logger.info(f"Observation aborted: {self.obs_config['parset']['task.taskID']}: "
                             f"{self.obs_config['datetimesource']}")
        elif self.process is not None:
            # processing is running, nothing to do (processing will set stop event)
            return
        else:
            # no processing is running, only stop the processor
            self.stop_event.set()

    def stop(self, abort=None):
        """
        Stop this service

        :param bool abort: Ignored, a stop of the service always equals abort
        """
        if hasattr(self, 'obs_config'):
            self.logger.info(f"ProcessorMaster for {self.obs_config['parset']['task.taskID']}: "
                             f"{self.obs_config['datetimesource']} received stop")
        else:
            self.logger.info("ProcessorMaster received stop")
        # abort observation, this also stops the ProcessorMaster
        self.stop_observation(abort=True)

    def _get_result_dir(self):
        """
        Get result directory from worker processor config
        """
        with open(self.config_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)['processor']

        # set config, expanding strings
        kwargs = {'home': os.path.expanduser('~'), 'hostname': socket.gethostname()}
        return config['result_dir'].format(**kwargs)

    def _wait_for_workers(self):
        """
        Wait for all worker nodes to finish processing this observation
        """
        obs = self.obs_config['datetimesource']
        self.logger.info(f"Waiting for workers to finish processing {obs}")
        twait = 0

        for beam in self.obs_config['beams']:
            # Log which beam we are waiting for
            self.logger.info(f"{obs} waiting for results from CB{beam:02d}")
            result_file = os.path.join(self.central_result_dir, f'CB{beam:02d}_summary.yaml')
            # wait until the result file is present
            while not os.path.isfile(result_file):
                # wait until the next check time
                self.stop_event.wait(self.check_interval)
                # abort if processing is stopped
                if self.stop_event.is_set():
                    return
                twait += self.check_interval
                # if we waited a long time, check if a warning should be sent if the node is offline
                node = WORKERS[beam]
                if (twait > self.max_wait_time) and (not self._check_node_online(node)) and \
                   (node not in self.warnings_sent) and (not os.path.isfile(result_file)):
                    self._send_warning(node)
                    # store that we sent a warning
                    self.warnings_sent.append(node)

    def _check_node_online(self, node):
        """
        Check if the processor on a node is still online and processing the current observation

        :param str node: Hostname of node to check
        :return: status (bool): True if node is online, else False
        """
        # check if the processor on the node is online
        try:
            reply = send_command(self.node_timeout, 'processor', 'status', host=node)
            if reply is None:
                self.logger.debug(f"No reply received from {node}, assuming it is offline")
                return False
            status = reply['message']['processor']
        except Exception as e:
            self.logger.error(f"Failed to get {node} status: {type(e)}: {e}")
            status = ''
        if status != 'running':
            # processor is not running
            self.logger.debug(f"{node} processor is not running")
            return False

        # get list of running observations from node
        self.logger.debug(f"{node} is online, checking for observations")
        try:
            output = send_command(self.node_timeout, 'processor', 'get_attr observations',
                                  host=node)['message']['processor']
            # parse the observation list
            # the list contains reference to processes, which should be put in quotes first
            output = ast.literal_eval(output.replace('<', '\'<').replace('>', '>\''))
            taskids = output['ProcessorManager.observations'].keys()
        except Exception as e:
            self.logger.error(f"Failed to get observation list from {node}: {type(e)}: {e}")
            return False
        self.logger.debug(f"{node} taskids: {taskids}")

        # check if the node is still processing the current taskid
        try:
            taskid = self.obs_config['parset']['task.taskID']
        except (KeyError, TypeError):
            # KeyError if parset or task.taskID are missing, TypeError if obs_config is None
            self.logger.error(f"Failed to get task ID of current master observation, assuming {node} is online")
            return True

        if taskid in taskids:
            return True
        else:
            return False

    def _send_warning(self, node):
        """
        Send a warning email about a node

        :param str node: Node to send warning about
        """
        # get observation info from obs config
        try:
            date = self.obs_config['date']
            datetimesource = self.obs_config['datetimesource']
            taskid = self.obs_config['parset']['task.taskID']
        except (KeyError, TypeError):
            # KeyError if parset or task.taskID are missing, TypeError if obs_config is None
            self.logger.error(f"Failed to get parameters of current master observation, not sending warning email for "
                              f"{node}")
            return

        # generate email
        beam = int(node[-2:]) - 1
        content = dedent(f"""
                        <html>
                        <title>DARC Warning</title>
                        <body>
                        <p>
                        <h3>Warning: DARC may be offline on {node}</h3><br />
                        DARC on {node} is either offline or no longer processing this observation:<br />
                        Task ID = {taskid}<br />
                        Name = {datetimesource}<br />
                        </p>
                        <p>
                        Please check:
                        <ul>
                          <li>Is DARC still online on {node}? See status website: http://arts041.apertif/~arts/darc/status
                          <li>Is DARC still processing on {node}?
                            <ul>
                                <li>Check the processing website: http://arts041.apertif/~arts/darc/processing
                                <li>Check the log file: <code>tail -n 50 /home/arts/darc/log/processor.{node}.log</code>
                                <li>Check if there are files in <code>/data2/output/{date}/{datetimesource}/triggers</code>
                            </ul>
                        </ul>
                        </p>
                        <p>
                        If DARC is not processing the observation, do the following:
                        <ul>
                            <li>Restart DARC on {node} (only if it is offline on the status webpage or you have other reason to suspect DARC is stuck): <code>ssh arts@{node} '. darc/venv/bin/activate && darc_kill_all; darc_start_all_services'</code>
                            <li>Create an empty output file for this observation: <code>touch /home/arts/darc/results/{date}/{datetimesource}/CB{beam:02d}_summary.yaml</code>
                        </p>
                        </body>
                        </html>
                        """)  # noqa - ignore max line length

        # set email subject with trigger time
        subject = f"DARC Warning: {node}"
        # get FQDN in way that actually adds the domain
        # simply socket.getfqdn does not actually do that on ARTS
        fqdn = socket.getaddrinfo(socket.gethostname(), None, 0, socket.SOCK_DGRAM, 0, socket.AI_CANONNAME)[0][3]
        frm = f"DARC Warning System <{os.getlogin()}@{fqdn}>"
        to = self.email_settings['to']
        body = {'type': 'html', 'content': content}
        # send
        self.logger.info(f"Sending {node} warning email")
        util.send_email(frm, to, subject, body)

    def _process_results(self, info, coordinates):
        """
        Load statistics and plots from the nodes. Copy to website directory and return data to be sent as email

        :param dict info: Observation info summary
        :param dict coordinates: Coordinates of every CB in the observation
        :return: email (str), attachments (list)
        """
        self.logger.info(f"Processing results of {self.obs_config['datetimesource']}")
        notes = ""

        if self.obs_config['parset']['task.directionReferenceFrame'].upper() == 'HADEC':
            notes += "Reference frame is HADEC: RA/Dec coordinates are given for midpoint of observation.\n"

        # initialize email fields: trigger statistics, beam info, attachments
        beaminfo = ""
        triggers = []
        attachments = []
        missing_attachments = []
        missing_beams = []

        for beam in self.obs_config['beams']:
            # load the summary file
            with open(os.path.join(self.central_result_dir, f'CB{beam:02d}_summary.yaml')) as f:
                info_beam = yaml.load(f, Loader=yaml.SafeLoader)

            if info_beam is None:
                self.logger.warning(f"Empty result file for CB{beam:02d}")
                # add to email with question marks
                beaminfo += "<tr><td>{beam:02d}</td>" \
                            "<td>?</td>" \
                            "<td>?</td>" \
                            "<td>?</td>" \
                            "<td>?</td></tr>".format(beam=beam)
                # add warning message
                missing_beams.append(f'CB{beam:02d}')
                continue

            beaminfo += "<tr><td>{beam:02d}</td>" \
                        "<td>{ncand_raw}</td>" \
                        "<td>{ncand_post_clustering}</td>" \
                        "<td>{ncand_post_thresholds}</td>" \
                        "<td>{ncand_post_classifier}</td></tr>".format(beam=beam, **info_beam)

            if info_beam['ncand_post_classifier'] > 0:
                # load the triggers
                try:
                    triggers_beam = np.atleast_1d(np.genfromtxt(os.path.join(self.central_result_dir,
                                                                             f'CB{beam:02d}_triggers.txt'),
                                                  names=True, encoding=None))
                    triggers.append(triggers_beam)
                except FileNotFoundError:
                    self.logger.error(f"Missing trigger file for {self.obs_config['datetimesource']} CB{beam:02d}")
                # load attachment
                fname = os.path.join(self.central_result_dir, f'CB{beam:02d}.pdf')
                if not os.path.isfile(fname):
                    missing_attachments.append(f'CB{beam:02d}')
                else:
                    attachments.append({'path': fname, 'name': f'CB{beam:02d}.pdf', 'type': 'pdf'})

        if missing_beams:
            notes += f"Beams failed processing: {', '.join(missing_beams)}\n"
        if missing_attachments:
            notes += f"Missing PDF files for {', '.join(missing_attachments)}\n"

        # combine triggers from different CBs and sort by p, then by S/N
        if len(triggers) > 0:
            triggers = np.sort(np.concatenate(triggers), order=('p', 'snr'))[::-1]
        # save total number of triggers
        info['total_triggers'] = len(triggers)
        # create string of trigger info
        triggerinfo = ""
        ntrig = 0
        for trigger in triggers:
            # convert trigger to a dict usable for formatting
            trigger_dict = {}
            for key in triggers.dtype.names:
                trigger_dict[key] = trigger[key]
            # convert downsampling to width in ms
            trigger_dict['width'] = trigger['downsamp'] * TSAMP.to(u.ms).value
            triggerinfo += "<tr><td>{p:.2f}</td>" \
                           "<td>{snr:.2f}</td>" \
                           "<td>{dm:.2f}</td>" \
                           "<td>{time:.4f}</td>" \
                           "<td>{width:.4f}</td>" \
                           "<td>{cb:02.0f}</td>" \
                           "<td>{sb:02.0f}</td>".format(**trigger_dict)
            ntrig += 1
            if ntrig >= self.ntrig_email_max:
                triggerinfo += "<tr><td>truncated</td><td>truncated</td><td>truncated</td>" \
                               "<td>truncated</td><td>truncated</td><td>truncated</td>"
                break

        # format the coordinate list
        coordinfo = ""
        for beam in sorted(coordinates.keys()):
            # each beam contains list of RA, Dec, Gl, Gb
            coordinfo += "<tr><td>{:02d}</td><td>{}</td><td>{}</td>" \
                         "<td>{}</td><td>{}</td>".format(beam, *coordinates[beam])

        # format the notes
        if notes:
            notesinfo = '<th style="text-align:left" colspan="2">Notes</th>' \
                        '<td colspan="4">{}</td></tr>'.format(notes)
        else:
            notesinfo = ""

        # format deep search plot command
        try:
            # extract CB00 pointing in decimal degrees
            ra_hms, dec_dms = coordinates[0][:2]
            pointing = SkyCoord(ra_hms, dec_dms, unit=(u.hourangle, u.deg))
            date = ''.join(self.obs_config['datetimesource'].split('-')[:3])
            plot_cmd = f'python2 {self.plot_script} --ra {pointing.ra.deg:.6f} --dec {pointing.dec.deg:.6f} ' \
                       f'--date {date} --root {self.obs_config["datetimesource"]}'
        except KeyError:
            # no pointing found for CB00
            plot_cmd = 'Error: no CB00 pointing found'
            self.logger.error("Failed to generate deep search command: no CB00 pointing found")

        # add info strings to overall info
        info['beaminfo'] = beaminfo
        info['coordinfo'] = coordinfo
        info['triggerinfo'] = triggerinfo
        info['notes'] = notesinfo
        info['plot_cmd'] = plot_cmd

        # generate the full email html
        # using a second level dict here because str.format does not support keys containing a dot
        email = dedent("""
            <html>
            <head><title>FRB Alert System</title></head>
            <body>
            <p>
            <table style="width:40%">
            <tr>
                <th style="text-align:left" colspan="2">UTC start</th>
                    <td colspan="4">{d[task.startTime]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Source</th>
                    <td colspan="4">{d[task.source.name]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Observation duration</th>
                    <td colspan="4">{d[task.duration]} s</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Task ID</th>
                    <td colspan="4">{d[task.taskID]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Classifier probability threshold (freq-time)</th>
                    <td colspan="4">{d[classifier_threshold_freqtime]:.2f}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Classifier probability threshold (dm-time)</th>
                    <td colspan="4">{d[classifier_threshold_dmtime]:.2f}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">YMW16 DM (central beam)</th>
                    <td colspan="4">{d[ymw16]:.2f} pc cm<sup>-3</sup></td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Used telescopes</th>
                    <td colspan="4">{d[task.telescopes]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Central frequency</th>
                    <td colspan="4">{d[freq]} MHz</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Total number of candidates</th>
                    <td colspan="4">{d[total_triggers]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Trigger web link</th>
                    <td colspan="4">{d[web_link]}</td>
            </tr><tr>
                <th style="text-align:left" colspan="2">Deep search command</th>
                    <td colspan="4">{d[plot_cmd]}</td>
            </tr>{d[notes]}
            </table>
            </p>
            <hr align="left" width="50%" />
            <p><h2>Number of triggers per Compound Beam</h2><br />
            <table style="width:50%">
            <tr style="text-align:left">
                <th>CB</th>
                <th>AMBER</th>
                <th>After grouping</th>
                <th>After local S/N threshold</th>
                <th>After classifier</th>
            </tr>
            {d[beaminfo]}
            </table>
            </p>
            <hr align="left" width="50%" />
            <p><h2>Compound Beam positions</h2><br />
            <table style="width:50%">
            <tr style="text-align:left">
                <th>CB</th>
                <th>RA (hms)</th>
                <th>Dec (dms)</th>
                <th>Gl (deg)</th>
                <th>Gb (deg)</th>
            </tr>
            {d[coordinfo]}
            </table>
            </p>
            <hr align="left" width="50%" />
            <p><h2>FRB candidates</h2><br />
            <table style="width:50%">
            <tr style="text-align:left">
                <th>Probability</th>
                <th>S/N</th>
                <th>DM (pc/cc)</th>
                <th>Arrival time (s)</th>
                <th>Width (ms)</th>
                <th>CB</th>
                <th>SB</th>
            </tr>
            {d[triggerinfo]}
            </table>
            </p>
            </body>
            </html>
            """).format(d=info)

        return email, attachments

    def _publish_results(self, body, files):
        """
        Publish email content as local website
        """
        # create output folder
        web_folder = '{home}/public_html/darc/{webdir}/{date}/{datetimesource}'.format(webdir=self.webdir,
                                                                                       **self.obs_config)
        util.makedirs(web_folder)
        # save the email body, ensuring it is at the top of the list in a browser
        with open(os.path.join(web_folder, 'A_info.html'), 'w') as f:
            f.write(body)

        # create symlinks to PDFs. These are outside the public_html folder, but they are readable as long as they
        # are owned by the same user
        for src in files:
            dest = os.path.join(web_folder, os.path.basename(src['path']))
            try:
                os.symlink(src['path'], dest)
            except FileExistsError:
                os.remove(dest)
                os.symlink(src['path'], dest)
        self.logger.info(f"Published results of {self.obs_config['datetimesource']}")

    def _send_email(self, email, attachments):
        """
        Send email with observation results

        :param str email: Email body
        :param list attachments: Attachments
        """

        subject = f"ARTS FRB Alert System - {self.obs_config['datetimesource']}"
        # get FQDN in way that actually adds the domain
        # simply socket.getfqdn does not actually do that on ARTS
        fqdn = socket.getaddrinfo(socket.gethostname(), None, 0, socket.SOCK_DGRAM, 0, socket.AI_CANONNAME)[0][3]
        frm = f"ARTS FRB Alert System <{os.getlogin()}@{fqdn}>"
        to = self.email_settings['to']
        body = {'type': 'html', 'content': email}
        util.send_email(frm, to, subject, body, attachments)
        self.logger.info(f"Sent email for {self.obs_config['datetimesource']}")

    def _generate_info_file(self):
        """
        Generate observation info files

        :return: info (dict), coordinates of each CB (dict)
        """
        # generate observation summary file
        fname = os.path.join(self.central_result_dir, 'info.yaml')
        # start with the observation parset
        parset = self.obs_config['parset']
        info = parset.copy()
        # format telescope list
        info['task.telescopes'] = info['task.telescopes'].replace('[', '').replace(']', '')
        # Add central frequency
        info['freq'] = self.obs_config['freq']
        # Add YMW16 DM limit for CB00
        info['ymw16'] = util.get_ymw16(self.obs_config['parset'], 0, self.logger)
        # Add exact start time (startpacket)
        info['startpacket'] = self.obs_config['startpacket']
        # Add classifier probability thresholds
        with open(self.config_file, 'r') as f:
            classifier_config = yaml.load(f, Loader=yaml.SafeLoader)['processor']['classifier']
        info['classifier_threshold_freqtime'] = classifier_config['thresh_freqtime']
        info['classifier_threshold_dmtime'] = classifier_config['thresh_dmtime']
        # add path to website
        # get FQDN in way that actually adds the domain
        # simply socket.getfqdn does not actually do that on ARTS
        fqdn = socket.getaddrinfo(socket.gethostname(), None, 0, socket.SOCK_DGRAM, 0, socket.AI_CANONNAME)[0][3]
        info['web_link'] = 'http://{fqdn}/~{user}/darc/{webdir}/' \
                           '{date}/{datetimesource}'.format(fqdn=fqdn, user=os.getlogin(),
                                                            webdir=self.webdir, **self.obs_config)
        # save the file
        with open(fname, 'w') as f:
            yaml.dump(info, f, default_flow_style=False)

        # generate file with coordinates
        coordinates = {}
        for beam in self.obs_config['beams']:
            try:
                key = "task.beamSet.0.compoundBeam.{}.phaseCenter".format(beam)
                c1, c2 = ast.literal_eval(parset[key].replace('deg', ''))
                if parset['task.directionReferenceFrame'] == 'HADEC':
                    # convert HADEC to J2000 RADEC at midpoint of observation
                    midpoint = Time(parset['task.startTime']) + .5 * float(parset['task.duration']) * u.s
                    pointing = SkyCoord(*util.hadec_to_radec(c1 * u.deg, c2 * u.deg, midpoint))
                else:
                    pointing = SkyCoord(c1, c2, unit=(u.deg, u.deg))
            except Exception as e:
                self.logger.error("Failed to get pointing for CB{:02d}: {}".format(beam, e))
                coordinates[beam] = ['-1', '-1', '-1', '-1']
            else:
                # get pretty strings
                ra = pointing.ra.to_string(unit=u.hourangle, sep=':', pad=True, precision=1)
                dec = pointing.dec.to_string(unit=u.deg, sep=':', pad=True, precision=1)
                gl, gb = pointing.galactic.to_string(precision=8).split(' ')
                coordinates[beam] = [ra, dec, gl, gb]

        # save to result dir
        with open(os.path.join(self.central_result_dir, 'coordinates.txt'), 'w') as f:
            f.write("#CB RA Dec Gl Gb\n")
            for beam, coord in coordinates.items():
                f.write("{:02d} {} {} {} {}\n".format(beam, *coord))

        return info, coordinates
