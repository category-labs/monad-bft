#!/bin/bash

bash /monad/scripts/tc.sh

monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --genesis-config /monad/config/genesis.toml \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    --blockdb-path /monad/blockdb \
    > /monad/logs/client.log 2>&1