#!/bin/bash

bash /monad/scripts/tc.sh

mkdir /monad/logs

monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --genesis-config /monad/config/genesis.toml \
    --wal-path /monad/wal \
    --blockdb-path /monad/blockdb \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    test-mode --byzantine-execution > /monad/logs/client.log 2>&1