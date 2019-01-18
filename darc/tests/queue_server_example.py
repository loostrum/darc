#!/usr/bin/env python
# 
# Based on multiprocessing
# https://docs.python.org/2/library/multiprocessing.html
# 16.6.2.7.2. Using a remote manager

from multiprocessing.managers import BaseManager
import Queue

PORT_AMBER=50000
AUTH_AMBER='amber'

class BasicServer(BaseManager):
    def __init__(self, *args, **kwargs):
        self.queue = Queue.Queue()
        super(Server, self).__init__(*args, **kwargs)

    def get_queue(self):
        return self.queue

class AMBERServer():
    def __init__(self):
        manager = Server(address=("", PORT_AMBER), authkey=AUTH_AMBER)
        server = manager.get_server()
        server.serve_forever()
