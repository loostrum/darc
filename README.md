# DARC
Data Analysis of Real-time Candidates from ARTS

This repository contains the necessary software to automatically process FRB candidates produced by the ARTS real-time system.
The main executable should only be run on the ARTS cluster.

### Branches
Note: IAB modes also support processing the central TAB.
* master: Real-time, TAB mode processing.
* dev: Development of real-time processing
* test_trigger: Directly apply thresholds to AMBER triggers to test IQUV triggering (IAB).

### Overview
DARC comprises several parts that communicate through either queues or sockets. The availability of different services depends on which branch is active.

* DARCMaster: Master service. Handles communication with user, controls all other services.
* AMBERListener: Listens for AMBER triggers on a socket and puts them on a Python queue.
* AMBERTriggering: Directly applies thresholds to AMBER triggers and puts triggers on VOEvent queue.
* VOEventGenerator: Converts incoming trigger to VOEvent and sends it.
* StatusWebsite: Queries status of all services and generates status webpage.
* OfflineProcessing: Handles offline processing for IAB mode

### Executables
`darc`: Used to interact with the all services through the DARC Master service.\
`darc_start_master`: Starts the DARC Master service if not already running.\
`darc_stop_master`: Stops the DARC Master service and by extension all other services.\
`darc_start_all_services`: Starts all services, including DARC Master if it is not running.\
`darc_stop_all_services`: Stops all services except DARC Master.\
`darc_kill_all`: Kill DARC Master service and by extension all other services.\
`darc_service`: The DARC Master service. Should not be started directly, but through `darc_start_master` or `darc_start_all_services`

