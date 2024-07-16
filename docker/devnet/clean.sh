#!/bin/bash

blockdb="./monad/blockdb"
ledger="./monad/ledger"
mempool_sock="./monad/mempool.sock"
controlpanel_sock="./monad/controlpanel.sock"
triedb="./monad/triedb"
wal="./monad/wal"
forkpoint="./monad/config/forkpoint.toml"

# Check if directories/files from previous run exist, delete them if they exist
if [ -d "$blockdb" ]; then
    rm -r "$blockdb"
fi

if [ -d "$ledger" ]; then
    rm -r "$ledger"
fi

if [ -d "$triedb" ]; then
    rm -r "$triedb"
fi

if [ -S "$mempool_sock" ]; then
    rm "$mempool_sock"
fi

if [ -S "$controlpanel_sock" ]; then
    rm "$controlpanel_sock"
fi

if [ -f "$wal" ]; then
    rm "$wal"
fi

if [ -f "$forkpoint" ]; then
    rm "$forkpoint"
fi
