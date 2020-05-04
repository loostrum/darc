#!/usr/bin/env python

import os
import numpy as np
import itertools
import unittest
import multiprocessing as mp
import threading
from time import sleep
from queue import Empty

from darc.amber_listener import AMBERListener


class TestAMBERListener(unittest.TestCase):

    def test_trigger_set(self):
        """
        Check whether a set of triggers + header is put on the queue correctly
        """

        # create input queue
        obs_queue = mp.Queue()
        # create output queue
        amber_queue = mp.Queue()
        # init AMBER Listener
        listener = AMBERListener()
        # set the queues
        listener.set_source_queue(obs_queue)
        listener.set_target_queue(amber_queue)
        # start the listener
        listener.start()
        # start observation
        amber_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amber.conf')
        obs_config = {'amber_dir': os.path.dirname(os.path.abspath(__file__)),
                      'beam': 0,
                      'amber_config': amber_conf_file}
        command = {'command': 'start_observation', 'obs_config': obs_config}
        obs_queue.put(command)

        output = []
        # read queue until empty
        while True:
            try:
                raw_trigger = amber_queue.get(timeout=2)
            except Empty:
                break
            output.append(raw_trigger['trigger'])

        # stop the observation
        obs_queue.put({'command': 'stop_observation'})
        sleep(1)

        # stop the listener
        listener.stop()

        # check if there is any output at all
        self.assertTrue(len(output) > 0)

        # check the output is correct, i.e. equal to input
        # load all trigger files
        all_triggers = []
        for step in [1, 2, 3]:
            trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        'CB00_step{}.trigger'.format(step))
            with open(trigger_file, 'r') as f:
                triggers = f.readlines()
            triggers = [line.strip() for line in triggers]
            all_triggers.extend(triggers)

        # sort input and output by last element (S/N)
        output.sort()
        all_triggers.sort()
        self.assertListEqual(output, all_triggers)

    @staticmethod
    def send_triggers(triggers, output_file):
        """
        Send triggers to a queue with proper delays
        """
        with open(output_file, 'w') as f:
            # immediately send the header
            hdr = triggers[0]
            f.write(hdr + '\n')
            f.flush()
            # sleep to simulate observation start-up
            sleep(3)
            # loop over triggers, skipping the header
            batch = 0
            for i, line in enumerate(triggers[1:]):
                this_batch = int(line.split(' ')[1])
                # write a full batch
                if batch != this_batch:
                    # flush previous batch, then sleep for a batch duration
                    f.flush()
                    sleep(1)
                    batch += 1
                f.write(line + '\n')

    # Test continuous read of triggers
    def test_continuous_triggers(self):
        """
        Check whether a stream of triggers + header is put on the queue correctly
        """

        # read triggers for all three steps and prepare sending threads
        # maximum amount of batches to send
        # is roughly equal to max runtime of this test
        max_batch = 15
        # max_batch = 5
        fake_beam = 99
        threads = []
        nstep = 3
        # NOTE: [[] * nstep does not work, all sublists point to same memory!
        all_triggers = [[] for _ in range(nstep)]
        # init the threads that will send the triggers
        for i in range(nstep):
            step = i + 1
            trigger_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        'CB00_step{}.trigger'.format(step))
            with open(trigger_file, 'r') as f:
                triggers = f.readlines()
            for line in triggers:
                line = line.strip()
                if line.startswith('#'):
                    all_triggers[i].append(line)
                elif int(line.split(' ')[1]) <= max_batch:
                    all_triggers[i].append(line)

            output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       'CB{}_step{}.trigger'.format(fake_beam, step))
            # delete any old output file
            try:
                os.remove(output_file)
            except OSError:
                pass
            # init thread for sending triggers
            thread = threading.Thread(target=self.send_triggers, args=(all_triggers[i], output_file),
                                      name='step{}'.format(step))
            thread.daemon = True
            threads.append(thread)

        # create input queue
        obs_queue = mp.Queue()
        # create output queue
        amber_queue = mp.Queue()
        # init AMBER Listener
        listener = AMBERListener()
        # set the queues
        listener.set_source_queue(obs_queue)
        listener.set_target_queue(amber_queue)
        # start the listener
        listener.start()
        # start observation
        amber_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amber.conf')
        obs_config = {'amber_dir': os.path.dirname(os.path.abspath(__file__)),
                      'beam': fake_beam,
                      'amber_config': amber_conf_file}
        command = {'command': 'start_observation', 'obs_config': obs_config, 'reload_conf': False}
        obs_queue.put(command)

        # start the senders
        for thread in threads:
            thread.start()

        # wait until they are done
        for thread in threads:
            thread.join()

        output = []
        # read queue until empty
        while True:
            try:
                raw_trigger = amber_queue.get(timeout=5)
            except Empty:
                break
            output.append(raw_trigger['trigger'])

        # stop the observation
        obs_queue.put({'command': 'stop_observation'})
        sleep(5)

        # stop the listener
        listener.stop()

        # delete temp output files
        for step in range(1, nstep + 1):
            output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       'CB{}_step{}.trigger'.format(fake_beam, step))
            try:
                os.remove(output_file)
            except OSError as e:
                print("Warning: could not delete temp file {}: {}".format(output_file, e))

        # check if there is any output at all
        self.assertTrue(len(output) > 0)

        # check the output is correct, i.e. equal to input
        # check hdr separately. All hdrs should be equal
        # output should have hdr once for each step
        for i in range(nstep):
            self.assertEqual(output[i], all_triggers[i][0])
        # sort input and output by last element (S/N)
        # ignore headers
        output = [line for line in output if not line.startswith('#')]
        output.sort(key=lambda x: float(x.split(' ')[-1]))
        all_triggers_flattened = list(itertools.chain(*all_triggers))
        all_triggers_flattened = [line for line in all_triggers_flattened if not line.startswith('#')]
        all_triggers_flattened.sort(key=lambda x: float(x.split(' ')[-1]))
        self.assertListEqual(output, all_triggers_flattened)


if __name__ == '__main__':
    unittest.main()
