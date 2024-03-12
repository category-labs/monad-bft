#!/bin/bash

groupadd -g $HOST_GID user
useradd -u $HOST_UID -g user -N user

su -c "mkdir /monad/logs" -m user 

su -c "python3 /monad/scripts/blaster.py --rpc http://rpc0:8080 --data /data/txns.json > /monad/logs/rpc-blaster.log 2>&1" -m user
