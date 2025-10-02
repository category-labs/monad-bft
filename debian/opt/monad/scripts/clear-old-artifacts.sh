#!/bin/bash

TARGET_DIR="/home/monad/monad-bft/ledger/"

NEW_FILES=$(find "$TARGET_DIR" -type f -name "*" -mmin -20)
if [ -n "$NEW_FILES" ]; then
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.toml.*" -mmin +300 -delete 2>/dev/null
    find /home/monad/monad-bft/config/validators/ -type f -name "validators.toml.*" -mtime +30 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/headers -type f -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/bodies -type f -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ -type f -name "wal_*" -mmin +300 -delete 2>/dev/null
else
    echo "No new files detected. Skipping deletion of ledger files."
fi
exit 0
