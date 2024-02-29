#!/bin/bash

mkdir /monad/logs

# ping -c 10 rpc0 > /monad/logs/ping.log 2>&1
# curl -vv --header "Content-Type: application/json" --request POST --data '{"jsonrpc": "2.0", "method": "eth_sendRawTransaction", "params": [], "id": 0}' http://rpc0:8080 > /monad/logs/curl.log 2>&1
python3 /monad/scripts/blaster.py --rpc http://rpc0:8080 --data /data/txns.json > /monad/logs/rpc-blaster.log 2>&1
