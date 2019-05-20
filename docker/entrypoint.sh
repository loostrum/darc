#!/bin/bash
set -e

# start ssh server
service ssh start
# start web server
service apache2 start
# load apertif config
if [ -f /opt/apertif/apertifinit.sh ]; then
    . /opt/apertif/apertifinit.sh
else
    echo "/opt/apertif not available"
fi
# start darc master service
darc_service &
# start shell
/bin/bash
