#!/bin/bash

set -ex

# Pull MONAD_*_DIR overrides before doing anything destructive. The env file
# is systemd EnvironmentFile format (values may be unquoted/spaced), so extract
# the path vars instead of sourcing — sourcing would execute a spaced password.
_envfile=""
if [ -f /etc/monad/env ]; then
  _envfile=/etc/monad/env
elif [ -f /home/monad/.env ]; then
  _envfile=/home/monad/.env
fi
if [ -n "$_envfile" ]; then
  while IFS='=' read -r _k _v; do
    case "$_k" in
      MONAD_CONFIG_DIR)  MONAD_CONFIG_DIR="$_v" ;;
      MONAD_DATA_DIR)    MONAD_DATA_DIR="$_v" ;;
      MONAD_RUNTIME_DIR) MONAD_RUNTIME_DIR="$_v" ;;
    esac
  done < <(grep -E '^[[:space:]]*MONAD_(CONFIG|DATA|RUNTIME)_DIR=' "$_envfile")
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
