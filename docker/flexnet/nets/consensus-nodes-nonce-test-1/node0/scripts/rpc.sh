#!/bin/bash

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 1
done

monad-rpc \
    --ipc-path /monad/mempool.sock > /monad/logs/rpc.log 2>&1
        