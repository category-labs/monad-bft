#!/bin/bash

set -ex
systemctl stop monad-bft monad-execution monad-rpc monad-mpt monad-execution-genesis || true
mkdir -p /var/lib/monad/empty-dir
rsync -r --delete /var/lib/monad/empty-dir/ /var/lib/monad/ledger/
rsync -r --delete /var/lib/monad/empty-dir/ /var/lib/monad/forkpoint/
rsync -r --delete /var/lib/monad/empty-dir/ /var/lib/monad/validators/
touch /var/lib/monad/ledger/wal
rm -rf /var/lib/monad/empty-dir
rm -rf /var/lib/monad/snapshots
rm -f /run/monad/mempool.sock
rm -f /run/monad/controlpanel.sock
rm -f /run/monad/statesync.sock
rm -f /var/lib/monad/wal_*
rm -f /var/lib/monad/peers.toml
rm -rf /var/lib/monad/blockdb
if [ -f /etc/monad/env ]; then
  . /etc/monad/env
elif [ -f /home/monad/.env ]; then
  . /home/monad/.env
fi
monad-mpt --storage /dev/triedb --truncate --yes
restore_genesis() {
  local new_path="$1" legacy_path="$2" target="$3"
  if [ -f "$new_path" ]; then
    yes | cp -rf "$new_path" "$target"
  elif [ -f "$legacy_path" ]; then
    yes | cp -rf "$legacy_path" "$target"
  fi
}

restore_genesis /etc/monad/genesis/forkpoint.toml  /home/monad/.config/forkpoint.genesis.toml  /var/lib/monad/forkpoint/forkpoint.toml
restore_genesis /etc/monad/genesis/validators.toml /home/monad/.config/validators.genesis.toml /var/lib/monad/validators/validators.toml
