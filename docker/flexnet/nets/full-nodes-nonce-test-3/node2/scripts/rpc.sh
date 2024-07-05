#!/bin/bash

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 2
done

monad-rpc                                   \
    --ipc-path /monad/mempool.sock          \
    --blockdb-path /monad/blockdb           \
    --execution-ledger-path /monad/ledger   \
    --triedb-path /monad/triedb > /monad/logs/rpc.log 2>&1
