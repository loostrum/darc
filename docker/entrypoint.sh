#!/bin/bash
set -e

# start ssh server
service ssh start
# start web server
service apache2 start
# start darc master service
darc_service
# start shell
#/bin/bash
