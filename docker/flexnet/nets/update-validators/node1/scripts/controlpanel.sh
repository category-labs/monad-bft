#!/bin/bash

set -e

mkdir -p /monad/logs

# wait for node service to create IPC socket
while [[ ! -S "/monad/controlpanel.sock" ]]; do
    sleep 1
done

debug-node --control-panel-ipc-path /monad/controlpanel.sock validators > /monad/logs/getvalidators.log 2>&1
debug-node --control-panel-ipc-path /monad/controlpanel.sock update-validators --path /monad/scripts/validators.toml > /monad/logs/update-validators.log 2>&1

while true; do
    debug-node --control-panel-ipc-path /monad/controlpanel.sock validators >> /monad/logs/getvalidators.log 2>&1
    sleep 5;
done
