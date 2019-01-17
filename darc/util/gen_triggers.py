#!/usr/bin/env python
#
# Generate triggers in AMBER format
# Example CB00_step1.trigger:
# beam_id batch_id sample_id integration_step compacted_integration_steps time DM_id DM compacted_DMs SNR
#0 0 53 50 6 0.217088 138 27.6 1106 85.0372
#0 0 18 100 2 0.147456 1109 221.8 20 12.1705

import os
import time

import numpy as np

class TriggerGenerator(object):

    def __init__(self, outfile):
        # output file
        self.outfile = outfile
        # AMBER trigger file header
        self.cols = ['beam_id', 'batch_id', 'sample_id', 'integration_step', 'compacted_integration_steps', 
                        'time', 'DM_id', 'DM' , 'compacted_DMs', 'SNR']
        # Set allowed DM, S/N, integration_step
        self.dms = np.logspace(-1, 3, 10000)
        self.snrs = np.linspace(8, 100)
        self.integration_step = np.array([1, 5, 10, 25, 50, 100, 250])
        # Set observation durvation in seconds
        self.duration = 60
        # Set min and max interval between triggers
        self.min_interval = .001
        self.max_interval = 2


    def run(self):
        # generate triggers
        start_time = time.time()
        end_time = start_time + self.duration

        with open(self.outfile, 'w') as f:
            header = "# {}\n".format(' '.join(self.cols))
            f.write(header)
            while time.time() < end_time:
                dt = np.random.uniform(self.min_interval, self.max_interval)
                time.sleep(dt)
                trigger = self._create_trigger(time.time() - start_time)
                f.write(trigger)
                f.flush()


    def _create_trigger(self, curr_time):
        # pick dm, S/N, width
        dm = np.random.choice(self.dms)
        snr = np.random.choice(self.snrs)
        integration_step = np.random.choice(self.integration_step)

        # only fill in relevant keys, set rest to 0
        return "0 0 0 {:.0f} 0 {:.6f} 0 {:.2f} 0 {:.4f}\n".format(integration_step, curr_time, dm, snr)


if __name__ == '__main__':
    # run for 5 seconds
    runtime = 5
    tmpfile = "tmptriggers"
    if os.path.isfile(tmpfile):
        os.remove(tmpfile)
    generator = TriggerGenerator(tmpfile)
    generator.duration = runtime
    print "Running TriggerGenerator, duration: {}s".format(runtime)
    generator.run()
    os.remove(tmpfile)
    print "TriggerGenerator ran without errors"

