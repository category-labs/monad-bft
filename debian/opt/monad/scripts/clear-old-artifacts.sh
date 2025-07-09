#!/bin/bash

TARGET_DIR="/home/monad/monad-bft/ledger/"

NEW_FILES=$(find "$TARGET_DIR" -type f -name "*" -mmin -20)
if [ -n "$NEW_FILES" ]; then
    find /home/monad/monad-bft/config/forkpoint/ -type f -name "forkpoint.toml.*" -mmin +300 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/ -type f -name "*.body" -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ledger/ -type f -name "*.header" -mmin +600 -delete 2>/dev/null
    find /home/monad/monad-bft/ -type f -name "wal_*" -mmin +300 -delete 2>/dev/null
else
    echo "No new files detected. Skipping deletion of .header files."
fi
exit 0
