#!/bin/bash
# 
# Stop all DARC services

echo "Stopping AMBER Listener"
darc --service amber_listener --cmd stop
echo "Stopping AMBER Triggering"
darc --service amber_triggering --cmd stop
