#!/bin/bash
# 
# Start all DARC services

# Master service
if ps ax | grep "[d]arc_service" > /dev/null; then
    echo "DARC service online"
else
    echo "DARC service offline, starting"
    darc_service 2>&1 &
    sleep 1
fi

if ps ax | grep "[d]arc_service" > /dev/null; then
    # start services
    echo "Starting AMBER Listener"
    darc --service amber_listener --cmd start
    echo "Starting AMBER Triggering"
    darc --service amber_triggering --cmd start
else
    echo "Cannot start services - master service not running"   
    exit 1
fi