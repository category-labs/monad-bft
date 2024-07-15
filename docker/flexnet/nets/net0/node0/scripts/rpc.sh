#!/bin/bash

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 1
done

RUST_LOG=debug monad-rpc \
    --ipc-path /monad/mempool.sock --triedb-path /monad/node/triedb > /monad/logs/rpc.log 2>&1
        