#!/bin/bash
set -e

# start ssh server
service ssh start
# start web server
service apache2 start
# run input command as arts
su arts -c "$@"
