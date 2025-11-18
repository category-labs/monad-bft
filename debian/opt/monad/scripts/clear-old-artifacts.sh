#!/bin/bash

TARGET_DIR="/home/monad/monad-bft/ledger/"

# Retention times in minutes
RETENTION_FORKPOINT=${RETENTION_FORKPOINT:-300}      # 5 hours (forkpoint.rlp.* and forkpoint.toml.*)
RETENTION_VALIDATORS=${RETENTION_VALIDATORS:-43200}  # 30 days (validators.toml.*)
RETENTION_LEDGER=${RETENTION_LEDGER:-600}            # 10 hours (headers and bodies)
RETENTION_WAL=${RETENTION_WAL:-300}                  # 5 hours (wal_* files)

echo "Cleanup script started: RETENTION_LEDGER=${RETENTION_LEDGER}min, RETENTION_WAL=${RETENTION_WAL}min, RETENTION_FORKPOINT=${RETENTION_FORKPOINT}min, RETENTION_VALIDATORS=${RETENTION_VALIDATORS}min"

NEW_FILES=$(find "$TARGET_DIR" -type f -name "*" -mmin -20)
if [ -n "$NEW_FILES" ]; then
    echo "New files detected in ledger, proceeding with cleanup"
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.rlp.*" -mmin +${RETENTION_FORKPOINT} -delete 2>/dev/null
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.toml.*" -mmin +${RETENTION_FORKPOINT} -delete 2>/dev/null
    find /home/monad/monad-bft/config/validators/ -type f -name "validators.toml.*" -mmin +${RETENTION_VALIDATORS} -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/headers -type f -mmin +${RETENTION_LEDGER} -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/bodies -type f -mmin +${RETENTION_LEDGER} -delete 2>/dev/null
    find /home/monad/monad-bft/ -type f -name "wal_*" -mmin +${RETENTION_WAL} -delete 2>/dev/null
    echo "Cleanup completed successfully"
else
    echo "No new files detected. Skipping deletion of ledger files."
fi

# When monad-blockcapd is running, BLOCKCAPD_ROOT_DIR and BLOCKCAPD_MAX_GB
# should be set in ~monad/.env (or otherwise somehow injected into the environment
# of this script). If they are not defined, this section does not run. If they
# are both defined (and BLOCKCAPD_MAX_GB is not zero), it deletes the oldest files
# in the local block archive until the maximum size (in GiB) is less than
# BLOCKCAPD_MAX_GB
if [ -d "$BLOCKCAPD_ROOT_DIR" ] && [ "${BLOCKCAPD_MAX_GB:-0}" -ne "0" ]; then
  BLOCKCAPD_MAX_BYTES=$((BLOCKCAPD_MAX_GB * 1024 * 1024 * 1024))

  # while the total size of the archive is greater than allowed, delete the
  # oldest subdirectory (group of 10,000 capture files)
  while [ "$(du -sb "$BLOCKCAPD_ROOT_DIR" | awk '{print $1}')" -gt "$BLOCKCAPD_MAX_BYTES" ]; do
    OLDEST_SUBDIR=$(find "$BLOCKCAPD_ROOT_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' \
           | sort -n | head -1 | cut -d' ' -f2-)
    [ -n "$OLDEST_SUBDIR" ] && rm -rf "$OLDEST_SUBDIR" || break
  done
fi

exit 0
