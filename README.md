# DARC
Data Analysis of Real-time Candidates from ARTS

master: [![Build Status](https://travis-ci.com/loostrum/darc.svg?branch=master)](https://travis-ci.com/loostrum/darc)

dev: [![Build Status](https://travis-ci.com/loostrum/darc.svg?branch=dev)](https://travis-ci.com/loostrum/darc)

This repository contains the necessary software to automatically process FRB candidates produced by the ARTS real-time system.
The main executable should only be run on the ARTS cluster.

### Overview
DARC comprises several parts that communicate through either queues or sockets. The availability of different services depends on which branch is active.

* DARCMaster: Master service. Handles communication with user, controls all other services.
* AMBERListener: Continuously reads AMBER triggers and puts them on a Python queue (for clustering or direct triggering).
* AMBERClustering: Clusters AMBER triggers together and puts them on DADA trigger queue.
* DADATrigger: Generates and send dada_dbevent triggers for stokes I and IQUV
* VOEventGenerator: Converts incoming trigger to VOEvent and sends it.
* StatusWebsite: Queries status of all services and generates status webpage.
* OfflineProcesing: Handles offline processing for 12 TABs / IAB.

### Executables
`darc`: Used to interact with the all services through the DARC Master service.\
`darc_start_master`: Starts the DARC Master service if not already running.\
`darc_stop_master`: Stops the DARC Master service and by extension all other services.\
`darc_start_all_services`: Starts all services, including DARC Master if it is not running.\
`darc_stop_all_services`: Stops all services except DARC Master.\
`darc_kill_all`: Kill DARC Master service and by extension all other services.\
`darc_service`: The DARC Master service. Should not be started directly, but through `darc_start_master` or `darc_start_all_services`
