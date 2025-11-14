#!/bin/bash

TARGET_DIR="/home/monad/monad-bft/ledger/"

NEW_FILES=$(find "$TARGET_DIR" -type f -name "*" -mmin -20)
if [ -n "$NEW_FILES" ]; then
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.rlp.*" -mmin +300 -delete 2>/dev/null
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.toml.*" -mmin +300 -delete 2>/dev/null
    find /home/monad/monad-bft/config/validators/ -type f -name "validators.toml.*" -mtime +30 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/headers -type f -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/bodies -type f -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ -type f -name "wal_*" -mmin +300 -delete 2>/dev/null
else
    echo "No new files detected. Skipping deletion of ledger files."
fi

if [ -d "$BLOCKCAPD_ROOT_DIR" ] && [ "${BLOCKCAPD_MAX_GB:-0}" -ne "0" ]; then
  BLOCKCAPD_MAX_BYTES=$((BLOCKCAPD_MAX_GB * 1024 * 1024 * 1024))

  # while the total size of the archive is greater than allowed, delete the
  # oldest subdirectory (group of 10,000 capture files)
  while [ "$(du -sb "$BLOCKCAPD_ROOT_DIR" | awk '{print $1}')" -gt "$BLOCKCAPD_MAX_BYTES" ]; do
    OLDEST_SUBDIR=$(find "$BLOCKCAPD_ROOT_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' \
           | sort -n | head -1 | cut -d' ' -f2-)
    [ -n "$OLDEST_SUBDIR" ] && rm -rf "$OLDEST_SUBDIR"
  done
fi

exit 0
