#!/bin/bash

mkdir /monad/logs

python3 /monad/scripts/blaster.py --rpc http://monad_rpc:8080 --data /data/txns.json > /monad/logs/rpc-blaster.log 2>&1
