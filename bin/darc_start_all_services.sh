#!/bin/bash
#
# start as "nohup darc_start_all_services.sh"

# AMBER trigger listener

darc --service amber_listener &
