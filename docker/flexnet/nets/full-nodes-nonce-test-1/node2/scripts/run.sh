#!/bin/bash

bash /monad/scripts/tc.sh

monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --forkpoint-config /monad/config/forkpoint.toml \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --control-panel-ipc-path /monad/controlpanel.sock \
    --execution-ledger-path /monad/ledger \
    --blockdb-path /monad/blockdb \
    --triedb-path /monad/triedb \
    > /monad/logs/client.log 2>&1