#!/bin/bash

monad --db /monad/triedb/test.db --block_db /monad/ledger \
    --genesis_file /monad/config/genesis.json \
    --log_level INFO > /monad/logs/execution.log 2>&1