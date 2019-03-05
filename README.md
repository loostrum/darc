# DARC
Data Analysis of Real-time Candidates from ARTS

This repository contains the necessary software to automatically process FRB candidates produced by the ARTS real-time system.
The main binary should only be run on the ARTS cluster.

### Branches
* master: Real-time, IAB mode processing.
* real_time: development of real-time processing (IAB)
* test_trigger: Directly apply thresholds to AMBER triggers to test IQUV triggering.
* iab_offline_processing: processing after end time of observation. Only supports IAB mode.

### Overview
DARC comprises several parts that communicate through either queues or sockets. The availability of different services depends on which branch is active.

* DARCMaster: Master service. Handles communication with user, controls all other services.
* AMBERListener: Listens for AMBER triggers on a socket and puts them on a Python queue.
* AMBERTriggering: Directly applies thresholds to AMBER triggers and puts triggers on VOEvent queue.
* VOEventGenerator: Converts incoming trigger to VOEvent and sends it.
* StatusWebsite: Queries status of all services and generates status webpage.
* ObservationControl: Handles offline processing for IAB mode
