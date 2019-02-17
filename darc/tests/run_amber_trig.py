#!/usr/bin/env python

import os

from darc.util.gen_triggers import TriggerGenerator
import threading

def stream():
    os.system("stream_files_to_port tmp.trigger")

if __name__ == '__main__':
    trigger_file = "tmp.trigger"
    generator = TriggerGenerator(trigger_file)

    if os.path.isfile(trigger_file):
        os.remove(trigger_file)

    thread_generator = threading.Thread(target=generator.run)
    thread_generator.daemon = True

    thread_stream = threading.Thread(target=stream)
    thread_stream.daemon = True

    thread_generator.start()
    thread_stream.start()

    thread_generator.join()
    thread_stream.join()

