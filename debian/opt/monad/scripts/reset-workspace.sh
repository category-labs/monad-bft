#!/bin/bash

set -ex

# Source path config before doing anything destructive, so the operator's
# MONAD_*_DIR overrides take effect for the wipe + restore steps below.
if [ -f /etc/monad/env ]; then
  . /etc/monad/env
elif [ -f /home/monad/.env ]; then
  . /home/monad/.env
fi
: "${MONAD_CONFIG_DIR:=/etc/monad}"
: "${MONAD_DATA_DIR:=/var/lib/monad}"
: "${MONAD_RUNTIME_DIR:=/run/monad}"

systemctl stop monad-bft monad-execution monad-rpc monad-mpt monad-execution-genesis || true
mkdir -p "${MONAD_DATA_DIR}/empty-dir"
rsync -r --delete "${MONAD_DATA_DIR}/empty-dir/" "${MONAD_DATA_DIR}/ledger/"
rsync -r --delete "${MONAD_DATA_DIR}/empty-dir/" "${MONAD_DATA_DIR}/forkpoint/"
rsync -r --delete "${MONAD_DATA_DIR}/empty-dir/" "${MONAD_DATA_DIR}/validators/"
touch "${MONAD_DATA_DIR}/ledger/wal"
rm -rf "${MONAD_DATA_DIR}/empty-dir"
rm -rf "${MONAD_DATA_DIR}/snapshots"
rm -f  "${MONAD_RUNTIME_DIR}/mempool.sock"
rm -f  "${MONAD_RUNTIME_DIR}/controlpanel.sock"
rm -f  "${MONAD_RUNTIME_DIR}/statesync.sock"
rm -f  "${MONAD_DATA_DIR}"/wal_*
rm -f  "${MONAD_DATA_DIR}/peers.toml"
rm -rf "${MONAD_DATA_DIR}/blockdb"
monad-mpt --storage /dev/triedb --truncate --yes
restore_genesis() {
  local new_path="$1" legacy_path="$2" target="$3"
  if [ -f "$new_path" ]; then
    yes | cp -rf "$new_path" "$target"
  elif [ -f "$legacy_path" ]; then
    yes | cp -rf "$legacy_path" "$target"
  fi
}

restore_genesis "${MONAD_CONFIG_DIR}/genesis/forkpoint.toml"  /home/monad/.config/forkpoint.genesis.toml  "${MONAD_DATA_DIR}/forkpoint/forkpoint.toml"
restore_genesis "${MONAD_CONFIG_DIR}/genesis/validators.toml" /home/monad/.config/validators.genesis.toml "${MONAD_DATA_DIR}/validators/validators.toml"
