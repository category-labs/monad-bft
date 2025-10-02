#!/bin/bash

set -ex
systemctl stop monad-bft monad-execution monad-rpc monad-mpt monad-execution-genesis || true
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
rm -rf /home/monad/monad-bft/blockdb
source /home/monad/.env
monad-mpt --storage /dev/triedb --truncate --yes
if [ -f "/home/monad/.config/forkpoint.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/forkpoint.genesis.toml /home/monad/monad-bft/config/forkpoint/forkpoint.toml
fi
if [ -f "/home/monad/.config/validators.genesis.toml" ]; then
  yes | cp -rf /home/monad/.config/validators.genesis.toml /home/monad/monad-bft/config/validators/validators.toml
fi
