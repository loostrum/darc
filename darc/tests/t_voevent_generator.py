#!/usr/bin/env python

from queue import Queue
import threading
import socket
from time import sleep, time

from darc.definitions import *
from darc.voevent_generator import VOEventGenerator


if __name__ == '__main__':
    # create a queue
    queue = Queue()
    # create stop event for generator
    stop_event = threading.Event()
    # init VOEvent Generator
    generator = VOEventGenerator(stop_event)
    # set the queue
    generator.set_source_queue(queue)
    # start the generator
    generator.start()
    # create a trigger
    trigger = "trigger"
    queue.put(trigger)
