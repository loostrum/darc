# DARC
[![DOI](https://zenodo.org/badge/165673299.svg)](https://zenodo.org/badge/latestdoi/165673299)[![Build Status](https://travis-ci.com/loostrum/darc.svg?branch=master)](https://travis-ci.com/loostrum/darc)

Data Analysis of Real-time Candidates from the Apertif Radio Transient System

This repository contains the necessary software to automatically process FRB candidates produced by the ARTS real-time system.
As of May 2020, DARC has discovered 17 new FRBs.

An extended description of DARC and other ARTS software can be found in my PhD thesis, available [here](http://hdl.handle.net/11245.1/abe5c8fa-1fdf-490b-ac0d-61e946f5791f).

### Installation

## Requirements:
* astropy
* h5py
* matplotlib
* numpy
* pyyaml
* scipy
* voevent-parse

To install a DARC release, for example v2.0:
`pip install git+https://github.com/loostrum/darc/archive/v2.0.tar.gz`  
To install the latest master:
`pip install git+https://github.com/loostrum/darc.git`

### Overview
DARC comprises several parts that communicate through either queues or sockets. Each node of the ARTS cluster runs one instance of DARC. 

* DARCMaster: Master service. Handles communication with user, controls all other services.
* AMBERListener: Continuously reads AMBER triggers and puts them on a Python queue.
* AMBERClustering: Clusters AMBER triggers together and puts them on queues for either IQUV or LOFAR triggering.
* DADATrigger: Generates and sends dada_dbevent triggers for stokes I and IQUV
* LOFARTrigger: Generates and sends LOFAR TBB triggers.
* VOEventGenerator: Converts incoming triggers to a VOEvent and sends them to a VOEvent broker.
* StatusWebsite: Queries status of all services and generates status webpage.
* OfflineProcesing: Handles offline processing, runs after every observation.

### Executables
`darc`: Used to interact with the all services through the DARC Master service.\
`darc_start_master`: Starts the DARC Master service if not already running.\
`darc_stop_master`: Stops the DARC Master service and by extension all other services.\
`darc_start_all_services`: Starts all services, including DARC Master if it is not running.\
`darc_stop_all_services`: Stops all services except DARC Master.\
`darc_kill_all`: Kill DARC Master service and by extension all other services.\
`darc_service`: The DARC Master service. Should not be started directly, but through `darc_start_master` or `darc_start_all_services`


### Usage
Note: This example is specific to the ARTS cluster and assumes you are logged in to the ARTS master node. The python 3.6 virtual env (`. ~/python36/bin/activate`) needs to be activated to be able to run DARC commands manually. Always start DARC on the master node before starting it on other nodes. DARC can be started automatically across the cluster by running `start_full_pipeline` and stopped with `stop_full_pipeline`.  

Start DARC (e.g. on the master and arts001): `darc_start_all_services; ssh arts001 . ~/python36/bin/activate && darc_start_all_services`  
Verify that all services are running: `darc --host arts001 --service all status`  
Check the log file of a specific service: `tail ~/darc/log/amber_clustering.arts001.log`  
Restart a specific service: `darc --host arts001 --service amber_clustering restart`  
Start an observation: `darc --host arts001 --parset /path/to/parset start_observation`  
To consult the status of DARC across the cluster, open the `~arts/darc/status` webpage on the master node.  

### Documentation
Available at https://loostrum.github.io/darc/

