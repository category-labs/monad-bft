#!/bin/bash

bash /monad/scripts/tc.sh

groupadd -g $HOST_GID user
useradd -u $HOST_UID -g user -N user
# usermod -aG sudo user
# echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

su -c "mkdir /monad/logs" -m user 

su -c "monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --genesis-config /monad/config/genesis.toml \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    test-mode --byzantine-execution > /monad/logs/client.log 2>&1" -m user