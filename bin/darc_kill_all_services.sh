#!/bin/bash

if [[ $(uname -s) == "Darwin" ]]; then
    # Running on Mac
    pkill="pkill -f"
else
    # Assume Unix
    pkill="pkill"
fi

$pkill darc_service
$pkill stream_to_port
