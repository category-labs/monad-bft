#!/bin/bash

mkdir /monad/logs

python3 /monad/scripts/blaster.py --rpc http://rpc0:8080 --data /data/txns.json > /monad/logs/rpc-blaster.log 2>&1
