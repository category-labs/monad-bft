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
exit 0
