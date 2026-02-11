#!/bin/bash

source /home/monad/.env
MONAD_RUN_DIR=${MONAD_RUN_DIR:-/home/monad/monad-bft}

set -ex

if command -v systemctl &> /dev/null; then
    systemctl stop monad-bft monad-execution monad-rpc monad-mpt monad-execution-genesis || true
fi

mkdir "$MONAD_RUN_DIR/empty-dir"
rsync -r --delete "$MONAD_RUN_DIR/empty-dir/" "$MONAD_RUN_DIR/ledger/"
rsync -r --delete "$MONAD_RUN_DIR/empty-dir/" "$MONAD_RUN_DIR/config/forkpoint/"
rsync -r --delete "$MONAD_RUN_DIR/empty-dir/" "$MONAD_RUN_DIR/config/validators/"
touch "$MONAD_RUN_DIR/ledger/wal"
rm -rf "$MONAD_RUN_DIR/empty-dir"
rm -rf "$MONAD_RUN_DIR/snapshots"
rm -f "$MONAD_RUN_DIR/mempool.sock"
rm -f "$MONAD_RUN_DIR/controlpanel.sock"
rm -f "$MONAD_RUN_DIR/wal_"*
rm -f "$MONAD_RUN_DIR/config/peers.toml"
rm -rf "$MONAD_RUN_DIR/blockdb"
monad-mpt --storage /dev/triedb --truncate --yes
if [ -f "/home/monad/.config/forkpoint.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/forkpoint.genesis.toml "$MONAD_RUN_DIR/config/forkpoint/forkpoint.toml"
fi
if [ -f "/home/monad/.config/validators.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/validators.genesis.toml "$MONAD_RUN_DIR/config/validators/validators.toml"
fi
