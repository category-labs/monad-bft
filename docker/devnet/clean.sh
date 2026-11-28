#!/bin/bash

bft_ledger="./monad/bft-ledger"
block_payload="./monad/block-payload"
ledger="./monad/ledger"
mempool_sock="./monad/mempool.sock"
controlpanel_sock="./monad/controlpanel.sock"
triedb="./monad/triedb"
wal="./monad/wal"
forkpoint="./monad/config/forkpoint.toml"

# Check if directories/files from previous run exist, delete them if they exist
if [ -d "$ledger" ]; then
    rm -r "$ledger"
fi

if [ -d "$bft_ledger" ]; then
    rm -r "$bft_ledger"
fi

if [ -d "$block_payload" ]; then
    rm -r "$block_payload"
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

rm -f "${wal}"_*

if [ -f "$forkpoint" ]; then
    rm "$forkpoint"
fi
