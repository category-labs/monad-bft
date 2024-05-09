#!/bin/bash

mkdir /monad/logs

python3 /monad/scripts/e2e.py --rpc http://monad_rpc:8080 --data /data/txns.json 2> /monad/logs/e2e-tester.err 1> /monad/logs/e2e-tester.log