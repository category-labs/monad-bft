#!/bin/bash

set -ex

systemctl stop monad-bft monad-execution monad-rpc monad-mpt monad-execution-genesis || true

DB_MODE="slot"
MPT_OUTPUT=$(monad-mpt --storage /dev/triedb 2>/dev/null || true)
if [ -z "$MPT_OUTPUT" ]; then
  echo "WARNING: monad-mpt returned no output; defaulting to slot mode"
elif echo "$MPT_OUTPUT" | grep -q "Secondary:"; then
  DB_MODE="dual"
elif echo "$MPT_OUTPUT" | grep -q "State machine kind: monad"; then
  DB_MODE="page"
fi
echo "Detected DB mode: $DB_MODE"
mkdir /home/monad/monad-bft/empty-dir
rsync -r --delete /home/monad/monad-bft/empty-dir/ /home/monad/monad-bft/ledger/
rsync -r --delete /home/monad/monad-bft/empty-dir/ /home/monad/monad-bft/config/forkpoint/
rsync -r --delete /home/monad/monad-bft/empty-dir/ /home/monad/monad-bft/config/validators/
touch /home/monad/monad-bft/ledger/wal
rm -rf /home/monad/monad-bft/empty-dir
rm -rf /home/monad/monad-bft/snapshots
rm -f /home/monad/monad-bft/mempool.sock
rm -f /home/monad/monad-bft/controlpanel.sock
rm -f /home/monad/monad-bft/wal_*
rm -f /home/monad/monad-bft/config/peers.toml
rm -rf /home/monad/monad-bft/blockdb
source /home/monad/.env
case "$DB_MODE" in
  dual)
    monad-mpt --storage /dev/triedb --truncate --yes
    monad-mpt --storage /dev/triedb --activate-secondary --state-machine monad
    ;;
  page)
    monad-mpt --storage /dev/triedb --truncate --state-machine monad --yes
    ;;
  *)
    monad-mpt --storage /dev/triedb --truncate --yes
    ;;
esac

if [ -f "/home/monad/.config/forkpoint.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/forkpoint.genesis.toml /home/monad/monad-bft/config/forkpoint/forkpoint.toml
fi
if [ -f "/home/monad/.config/validators.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/validators.genesis.toml /home/monad/monad-bft/config/validators/validators.toml
fi
