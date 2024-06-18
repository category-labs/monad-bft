#!/bin/bash

blockdb="./monad/blockdb"
ledger="./monad/ledger"
mempool_sock="./monad/mempool.sock"
triedb="./monad/triedb"
wal="./monad/wal"

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

if [ -f "$wal" ]; then
    rm "$wal"
fi
